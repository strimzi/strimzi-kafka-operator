/*
 * Copyright 2018, Strimzi Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.controller.cluster;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.client.OpenShiftClient;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.controller.cluster.operations.cluster.KafkaClusterOperations;
import io.strimzi.controller.cluster.operations.cluster.KafkaConnectClusterOperations;
import io.strimzi.controller.cluster.operations.cluster.KafkaConnectS2IClusterOperations;
import io.strimzi.controller.cluster.operations.resource.BuildConfigOperations;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentConfigOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentOperations;
import io.strimzi.controller.cluster.operations.resource.ImageStreamOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.strimzi.controller.cluster.resources.KafkaConnectCluster;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.strimzi.controller.cluster.resources.KafkaConnectS2ICluster;
import io.vertx.core.*;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ClusterController extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(ClusterController.class.getName());

    public static final String STRIMZI_DOMAIN = "strimzi.io";
    public static final String STRIMZI_CLUSTER_CONTROLLER_DOMAIN = "cluster.controller.strimzi.io";
    public static final String STRIMZI_KIND_LABEL = STRIMZI_DOMAIN + "/kind";
    public static final String STRIMZI_TYPE_LABEL = STRIMZI_DOMAIN + "/type";
    public static final String STRIMZI_CLUSTER_LABEL = STRIMZI_DOMAIN + "/cluster";
    public static final String STRIMZI_NAME_LABEL = STRIMZI_DOMAIN + "/name";

    private static final int HEALTH_SERVER_PORT = 8080;

    private final KubernetesClient client;
    private final Map<String, String> labels;
    private final String namespace;
    private ConfigMapOperations configMapOperations;
    private StatefulSetOperations statefulSetOperations;
    private DeploymentOperations deploymentOperations;
    private DeploymentConfigOperations deploymentConfigOperations;

    private Watch configMapWatch;

    private long reconcileTimer;
    private KafkaClusterOperations kafkaClusterOperations;
    private KafkaConnectClusterOperations kafkaConnectClusterOperations;
    private KafkaConnectS2IClusterOperations kafkaConnectS2IClusterOperations;

    public ClusterController(ClusterControllerConfig config) {
        log.info("Creating ClusterController");

        this.namespace = config.getNamespace();
        this.labels = config.getLabels();
        this.client = new DefaultKubernetesClient();
    }

    /**
     * Set up the operations needed for cluster manipulation
     */
    private void setupOperations() {

        ServiceOperations serviceOperations = new ServiceOperations(vertx, client);
        statefulSetOperations = new StatefulSetOperations(vertx, client);
        configMapOperations = new ConfigMapOperations(vertx, client);
        PvcOperations pvcOperations = new PvcOperations(vertx, client);
        deploymentOperations = new DeploymentOperations(vertx, client);
        ImageStreamOperations imagesStreamOperations = null;
        BuildConfigOperations buildConfigOperations = null;
        boolean isOpenShift = Boolean.TRUE.equals(client.isAdaptable(OpenShiftClient.class));
        if (isOpenShift) {
            imagesStreamOperations = new ImageStreamOperations(vertx, client.adapt(OpenShiftClient.class));
            buildConfigOperations = new BuildConfigOperations(vertx, client.adapt(OpenShiftClient.class));
            deploymentConfigOperations = new DeploymentConfigOperations(vertx, client.adapt(OpenShiftClient.class));
        }
        this.kafkaClusterOperations = new KafkaClusterOperations(vertx, isOpenShift, configMapOperations, serviceOperations, statefulSetOperations, pvcOperations);
        this.kafkaConnectClusterOperations = new KafkaConnectClusterOperations(vertx, isOpenShift, configMapOperations, deploymentOperations, serviceOperations);

        if (isOpenShift) {
            this.kafkaConnectS2IClusterOperations = new KafkaConnectS2IClusterOperations(vertx, isOpenShift, configMapOperations, deploymentConfigOperations, serviceOperations, imagesStreamOperations, buildConfigOperations);
        }
    }

    @Override
    public void start(Future<Void> start) {
        log.info("Starting ClusterController");

        this.setupOperations();

        // Configure the executor here, but it is used only in other places
        getVertx().createSharedWorkerExecutor("kubernetes-ops-pool", 5, TimeUnit.SECONDS.toNanos(120));

        createConfigMapWatch(res -> {
            if (res.succeeded())    {
                configMapWatch = res.result();

                log.info("Setting up periodical reconciliation");
                this.reconcileTimer = vertx.setPeriodic(120000, res2 -> {
                    log.info("Triggering periodic reconciliation ...");
                    reconcile();
                });

                log.info("ClusterController up and running");

                // start the HTTP server for healthchecks
                this.startHealthServer();

                start.complete();
            }
            else {
                log.error("ClusterController startup failed");
                start.fail("ClusterController startup failed");
            }
        });
    }

    @Override
    public void stop(Future<Void> stop) throws Exception {

        vertx.cancelTimer(reconcileTimer);
        configMapWatch.close();
        client.close();

        stop.complete();
    }

    private void createConfigMapWatch(Handler<AsyncResult<Watch>> handler) {
        getVertx().executeBlocking(
                future -> {
                    Watch watch = client.configMaps().inNamespace(namespace).withLabels(labels).watch(new Watcher<ConfigMap>() {
                        @Override
                        public void eventReceived(Action action, ConfigMap cm) {
                            Map<String, String> labels = cm.getMetadata().getLabels();
                            String type;

                            if (!labels.containsKey(ClusterController.STRIMZI_TYPE_LABEL)) {
                                log.warn("Missing type in Config Map {}", cm.getMetadata().getName());
                                return;
                            }
                            else if (!labels.get(ClusterController.STRIMZI_TYPE_LABEL).equals(KafkaCluster.TYPE) &&
                                     !labels.get(ClusterController.STRIMZI_TYPE_LABEL).equals(KafkaConnectCluster.TYPE) &&
                                     !labels.get(ClusterController.STRIMZI_TYPE_LABEL).equals(KafkaConnectS2ICluster.TYPE)) {
                                log.warn("Unknown type {} received in Config Map {}", labels.get(ClusterController.STRIMZI_TYPE_LABEL), cm.getMetadata().getName());
                                return;
                            }
                            else {
                                type = labels.get(ClusterController.STRIMZI_TYPE_LABEL);
                            }

                            switch (action) {
                                case ADDED:
                                    log.info("New ConfigMap {}", cm.getMetadata().getName());
                                    if (type.equals(KafkaCluster.TYPE)) {
                                        addKafkaCluster(cm);
                                    }
                                    else if (type.equals(KafkaConnectCluster.TYPE)) {
                                        addKafkaConnectCluster(cm);
                                    }
                                    else if (type.equals(KafkaConnectS2ICluster.TYPE)) {
                                        addKafkaConnectS2ICluster(cm);
                                    }
                                    break;
                                case DELETED:
                                    log.info("Deleted ConfigMap {}", cm.getMetadata().getName());
                                    if (type.equals(KafkaCluster.TYPE)) {
                                        deleteKafkaCluster(cm);
                                    }
                                    else if (type.equals(KafkaConnectCluster.TYPE)) {
                                        deleteKafkaConnectCluster(cm);
                                    }
                                    else if (type.equals(KafkaConnectS2ICluster.TYPE)) {
                                        deleteKafkaConnectS2ICluster(cm);
                                    }
                                    break;
                                case MODIFIED:
                                    log.info("Modified ConfigMap {}", cm.getMetadata().getName());
                                    if (type.equals(KafkaCluster.TYPE)) {
                                        updateKafkaCluster(cm);
                                    }
                                    else if (type.equals(KafkaConnectCluster.TYPE)) {
                                        updateKafkaConnectCluster(cm);
                                    }
                                    else if (type.equals(KafkaConnectS2ICluster.TYPE)) {
                                        updateKafkaConnectS2ICluster(cm);
                                    }
                                    break;
                                case ERROR:
                                    log.error("Failed ConfigMap {}", cm.getMetadata().getName());
                                    reconcile();
                                    break;
                                default:
                                    log.error("Unknown action: {}", cm.getMetadata().getName());
                                    reconcile();
                            }
                        }

                        @Override
                        public void onClose(KubernetesClientException e) {
                            if (e != null) {
                                log.error("Watcher closed with exception", e);
                            }
                            else {
                                log.error("Watcher closed");
                            }

                            recreateConfigMapWatch();
                        }
                    });
                    future.complete(watch);
                }, res -> {
                    if (res.succeeded())    {
                        log.info("ConfigMap watcher up and running for labels {}", labels);
                        handler.handle(Future.succeededFuture((Watch)res.result()));
                    }
                    else {
                        log.info("ConfigMap watcher failed to start");
                        handler.handle(Future.failedFuture("ConfigMap watcher failed to start"));
                    }
                }
        );
    }

    private void recreateConfigMapWatch() {
        configMapWatch.close();

        createConfigMapWatch(res -> {
            if (res.succeeded())    {
                log.info("ConfigMap watch recreated");
                configMapWatch = res.result();
            }
            else {
                log.error("Failed to recreate ConfigMap watch");
            }
        });
    }

    /*
      Periodical reconciliation (in case we lost some event)
     */
    private void reconcile() {
        reconcileKafka();
        reconcileKafkaConnect();

        if (getKafkaConnectS2IClusterOperations() != null) {
            reconcileKafkaConnectS2I();
        }
    }

    private void reconcileKafka() {
        log.info("Reconciling Kafka clusters ...");

        Map<String, String> kafkaLabels = new HashMap(labels);
        kafkaLabels.put(ClusterController.STRIMZI_TYPE_LABEL, KafkaCluster.TYPE);

        List<ConfigMap> cms = configMapOperations.list(namespace, kafkaLabels);
        List<StatefulSet> sss = statefulSetOperations.list(namespace, kafkaLabels);

        List<String> cmsNames = cms.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toList());
        List<String> sssNames = sss.stream().map(cm -> cm.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL)).collect(Collectors.toList());

        List<ConfigMap> addList = cms.stream().filter(cm -> !sssNames.contains(cm.getMetadata().getName())).collect(Collectors.toList());
        List<ConfigMap> updateList = cms.stream().filter(cm -> sssNames.contains(cm.getMetadata().getName())).collect(Collectors.toList());
        List<StatefulSet> deletionList = sss.stream().filter(ss -> !cmsNames.contains(ss.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL))).collect(Collectors.toList());

        addKafkaClusters(addList);
        deleteKafkaClusters(deletionList);
        updateKafkaClusters(updateList);
    }

    private void addKafkaClusters(List<ConfigMap> add)   {
        for (ConfigMap cm : add) {
            log.info("Reconciliation: Kafka cluster {} should be added", cm.getMetadata().getName());
            addKafkaCluster(cm);
        }
    }

    private void updateKafkaClusters(List<ConfigMap> update)   {
        for (ConfigMap cm : update) {
            log.info("Reconciliation: Kafka cluster {} should be checked for updates", cm.getMetadata().getName());
            updateKafkaCluster(cm);
        }
    }

    private void deleteKafkaClusters(List<StatefulSet> delete)   {
        for (StatefulSet ss : delete) {
            log.info("Reconciliation: Kafka cluster {} should be deleted", ss.getMetadata().getName());
            deleteKafkaCluster(ss);
        }
    }

    private void reconcileKafkaConnect() {
        log.info("Reconciling Kafka Connect clusters ...");

        Map<String, String> kafkaLabels = new HashMap(labels);
        kafkaLabels.put(ClusterController.STRIMZI_TYPE_LABEL, KafkaConnectCluster.TYPE);

        List<ConfigMap> cms = configMapOperations.list(namespace, kafkaLabels);
        List<Deployment> deps = deploymentOperations.list(namespace, kafkaLabels);

        List<String> cmsNames = cms.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toList());
        List<String> depsNames = deps.stream().map(cm -> cm.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL)).collect(Collectors.toList());

        List<ConfigMap> addList = cms.stream().filter(cm -> !depsNames.contains(cm.getMetadata().getName())).collect(Collectors.toList());
        List<ConfigMap> updateList = cms.stream().filter(cm -> depsNames.contains(cm.getMetadata().getName())).collect(Collectors.toList());
        List<Deployment> deletionList = deps.stream().filter(dep -> !cmsNames.contains(dep.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL))).collect(Collectors.toList());

        addKafkaConnectClusters(addList);
        deleteKafkaConnectClusters(deletionList);
        updateKafkaConnectClusters(updateList);
    }

    private void addKafkaConnectClusters(List<ConfigMap> add)   {
        for (ConfigMap cm : add) {
            log.info("Reconciliation: Kafka Connect cluster {} should be added", cm.getMetadata().getName());
            addKafkaConnectCluster(cm);
        }
    }

    private void updateKafkaConnectClusters(List<ConfigMap> update)   {
        for (ConfigMap cm : update) {
            log.info("Reconciliation: Kafka Connect cluster {} should be checked for updates", cm.getMetadata().getName());
            updateKafkaConnectCluster(cm);
        }
    }

    private void deleteKafkaConnectClusters(List<Deployment> delete)   {
        for (Deployment dep : delete) {
            log.info("Reconciliation: Kafka Connect cluster {} should be deleted", dep.getMetadata().getName());
            deleteKafkaConnectCluster(dep);
        }
    }

    private void reconcileKafkaConnectS2I() {
        log.info("Reconciling Kafka Connect S2I clusters ...");

        Map<String, String> kafkaLabels = new HashMap(labels);
        kafkaLabels.put(ClusterController.STRIMZI_TYPE_LABEL, KafkaConnectS2ICluster.TYPE);

        List<ConfigMap> cms = configMapOperations.list(namespace, kafkaLabels);
        List<DeploymentConfig> deps = deploymentConfigOperations.list(namespace, kafkaLabels);

        List<String> cmsNames = cms.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toList());
        List<String> depsNames = deps.stream().map(cm -> cm.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL)).collect(Collectors.toList());

        List<ConfigMap> addList = cms.stream().filter(cm -> !depsNames.contains(cm.getMetadata().getName())).collect(Collectors.toList());
        List<ConfigMap> updateList = cms.stream().filter(cm -> depsNames.contains(cm.getMetadata().getName())).collect(Collectors.toList());
        List<DeploymentConfig> deletionList = deps.stream().filter(dep -> !cmsNames.contains(dep.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL))).collect(Collectors.toList());

        addKafkaConnectS2IClusters(addList);
        deleteKafkaConnectS2IClusters(deletionList);
        updateKafkaConnectS2IClusters(updateList);
    }

    private void addKafkaConnectS2IClusters(List<ConfigMap> add)   {
        for (ConfigMap cm : add) {
            log.info("Reconciliation: Kafka Connect S2I cluster {} should be added", cm.getMetadata().getName());
            addKafkaConnectS2ICluster(cm);
        }
    }

    private void updateKafkaConnectS2IClusters(List<ConfigMap> update)   {
        for (ConfigMap cm : update) {
            log.info("Reconciliation: Kafka Connect S2I cluster {} should be checked for updates", cm.getMetadata().getName());
            updateKafkaConnectS2ICluster(cm);
        }
    }

    private void deleteKafkaConnectS2IClusters(List<DeploymentConfig> delete)   {
        for (DeploymentConfig dep : delete) {
            log.info("Reconciliation: Kafka Connect S2I cluster {} should be deleted", dep.getMetadata().getName());
            deleteKafkaConnectS2ICluster(dep);
        }
    }

    /*
      Kafka / Zookeeper cluster control
     */
    private void addKafkaCluster(ConfigMap add)   {
        String name = add.getMetadata().getName();
        log.info("Adding cluster {}", name);

        getKafkaClusterOperations().create(namespace, name, res -> {
            if (res.succeeded()) {
                log.info("Kafka cluster added {}", name);
            }
            else {
                log.error("Failed to add Kafka cluster {}.", name);
            }
        });
    }

    private void updateKafkaCluster(ConfigMap cm)   {
        String name = cm.getMetadata().getName();
        log.info("Checking for updates in cluster {}", cm.getMetadata().getName());

        getKafkaClusterOperations().update(namespace, name, res2 -> {
            if (res2.succeeded()) {
                log.info("Kafka cluster updated {}", name);
            }
            else {
                log.error("Failed to update Kafka cluster {}.", name);
            }
        });
    }

    private void deleteKafkaCluster(StatefulSet ss)   {
        String name = ss.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL);
        log.info("Deleting cluster {}", name);
        deleteKafkaCluster(namespace, name);
    }

    private void deleteKafkaCluster(ConfigMap cm)   {
        String name = cm.getMetadata().getName();
        log.info("Deleting cluster {}", name);
        deleteKafkaCluster(namespace, name);
    }

    private void deleteKafkaCluster(String namespace, String name)   {
        getKafkaClusterOperations().delete(namespace, name, res -> {
            if (res.succeeded()) {
                log.info("Kafka cluster deleted {}", name);
            }
            else {
                log.error("Failed to delete Kafka cluster {}. Skipping Zookeeper cluster deletion.", name);
            }
        });
    }

    /*
      Kafka Connect cluster control
     */
    private void addKafkaConnectCluster(ConfigMap add)   {
        String name = add.getMetadata().getName();
        log.info("Adding Kafka Connect cluster {}", name);

        getKafkaConnectClusterOperations().create(namespace, name, res -> {
            if (res.succeeded()) {
                log.info("Kafka Connect cluster added {}", name);
            }
            else {
                log.error("Failed to add Kafka Connect cluster {}.", name);
            }
        });
    }

    private void updateKafkaConnectCluster(ConfigMap cm)   {
        String name = cm.getMetadata().getName();
        log.info("Checking for updates in Kafka Connect cluster {}", cm.getMetadata().getName());

        getKafkaConnectClusterOperations().update(namespace, name, res -> {
            if (res.succeeded()) {
                log.info("Kafka Connect cluster updated {}", name);
            }
            else {
                log.error("Failed to update Kafka Connect cluster {}.", name);
            }
        });
    }

    private void deleteKafkaConnectCluster(Deployment dep)   {
        String name = dep.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL);
        log.info("Deleting cluster {}", name);
        deleteKafkaConnectCluster(namespace, name);
    }

    private void deleteKafkaConnectCluster(ConfigMap cm)   {
        String name = cm.getMetadata().getName();
        log.info("Deleting cluster {}", name);
        deleteKafkaConnectCluster(namespace, name);
    }

    private void deleteKafkaConnectCluster(String namespace, String name)   {
        getKafkaConnectClusterOperations().delete(namespace, name, res -> {
            if (res.succeeded()) {
                log.info("Kafka Connect cluster deleted {}", name);
            }
            else {
                log.error("Failed to delete Kafka Connect cluster {}.", name);
            }
        });
    }

    /*
      Kafka Connect S2I cluster control
     */
    private void addKafkaConnectS2ICluster(ConfigMap add)   {
        String name = add.getMetadata().getName();

        if (getKafkaConnectS2IClusterOperations() != null) {
            log.info("Adding Kafka Connect S2I cluster {} in namespace {}", name, namespace);

            getKafkaConnectS2IClusterOperations().create(namespace, name, res -> {
                if (res.succeeded()) {
                    log.info("Kafka Connect S2I cluster added {} in namespace {}", name, namespace);
                } else {
                    log.error("Failed to add Kafka Connect S2I cluster {} in namespace {}.", name, namespace);
                }
            });
        } else {
            log.error("Cannot create Kafka Connect S2I cluster {} in namespace {}! Kafka Connect S2I clusters are supported only on OpenShift.", name, namespace);
        }
    }

    private void updateKafkaConnectS2ICluster(ConfigMap cm)   {
        String name = cm.getMetadata().getName();

        if (getKafkaConnectS2IClusterOperations() != null) {
            log.info("Checking for updates in Kafka Connect S2I cluster {}", cm.getMetadata().getName());

            getKafkaConnectS2IClusterOperations().update(namespace, name, res -> {
                if (res.succeeded()) {
                    log.info("Kafka Connect S2I cluster {} in namespace {} updated", name, namespace);
                }
                else {
                    log.error("Failed to update Kafka Connect S2I cluster {} in namespace {}.", name, namespace);
                }
            });
        } else {
            log.error("Cannot update Kafka Connect S2I cluster {} in namespace {}! Kafka Connect S2I clusters are supported only on OpenShift.", name, namespace);
        }
    }

    private void deleteKafkaConnectS2ICluster(DeploymentConfig dep)   {
        String name = dep.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL);
        log.info("Deleting Kafka Connect S2I cluster {} in namespace {}", name, namespace);
        deleteKafkaConnectS2ICluster(namespace, name);
    }

    private void deleteKafkaConnectS2ICluster(ConfigMap cm)   {
        String name = cm.getMetadata().getName();
        log.info("Deleting cluster {} in namespace {}", name, namespace);
        deleteKafkaConnectS2ICluster(namespace, name);
    }

    private void deleteKafkaConnectS2ICluster(String namespace, String name)   {
        if (getKafkaConnectS2IClusterOperations() != null) {
            getKafkaConnectS2IClusterOperations().delete(namespace, name, res -> {
                if (res.succeeded()) {
                    log.info("Kafka Connect S2I cluster deleted {} in namespace {}", name, namespace);
                }
                else {
                    log.error("Failed to delete Kafka Connect S2I cluster {} in namespace {}.", name, namespace);
                }
            });
        } else {
            log.error("Cannot delete Kafka Connect S2I cluster {} in namespace {}! Kafka Connect S2I clusters are supported only on OpenShift.", name, namespace);
        }
    }

    /**
     * Start an HTTP health server
     */
    private void startHealthServer() {

        this.vertx.createHttpServer()
                .requestHandler(request -> {

                    if (request.path().equals("/healthy")) {
                        request.response().setStatusCode(HttpResponseStatus.OK.code()).end();
                    } else if (request.path().equals("/ready")) {
                        request.response().setStatusCode(HttpResponseStatus.OK.code()).end();
                    }
                })
                .listen(HEALTH_SERVER_PORT);
    }

    private KafkaClusterOperations getKafkaClusterOperations() {
        return kafkaClusterOperations;
    }

    private KafkaConnectClusterOperations getKafkaConnectClusterOperations() {
        return kafkaConnectClusterOperations;
    }

    private KafkaConnectS2IClusterOperations getKafkaConnectS2IClusterOperations() {
        return kafkaConnectS2IClusterOperations;
    }
}
