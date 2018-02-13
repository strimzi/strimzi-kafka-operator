/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.client.OpenShiftClient;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.controller.cluster.operations.cluster.AbstractClusterOperations;
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
import java.util.Set;
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
    private final long reconciliationInterval;
    private final ConfigMapOperations configMapOperations;
    private final StatefulSetOperations statefulSetOperations;
    private final DeploymentOperations deploymentOperations;
    private final DeploymentConfigOperations deploymentConfigOperations;

    private Watch configMapWatch;

    private long reconcileTimer;
    private final KafkaClusterOperations kafkaClusterOperations;
    private final KafkaConnectClusterOperations kafkaConnectClusterOperations;
    private final KafkaConnectS2IClusterOperations kafkaConnectS2IClusterOperations;

    public ClusterController(Vertx vertx, ClusterControllerConfig config) {
        log.info("Creating ClusterController");

        this.namespace = config.getNamespace();
        this.labels = config.getLabels();
        this.reconciliationInterval = config.getReconciliationInterval();
        this.client = new DefaultKubernetesClient();

        ServiceOperations serviceOperations = new ServiceOperations(vertx, client);
        statefulSetOperations = new StatefulSetOperations(vertx, client);
        configMapOperations = new ConfigMapOperations(vertx, client);
        PvcOperations pvcOperations = new PvcOperations(vertx, client);
        deploymentOperations = new DeploymentOperations(vertx, client);
        ImageStreamOperations imagesStreamOperations = null;
        BuildConfigOperations buildConfigOperations = null;
        boolean isOpenShift = Boolean.TRUE.equals(client.isAdaptable(OpenShiftClient.class));

        this.kafkaClusterOperations = new KafkaClusterOperations(vertx, isOpenShift, configMapOperations, serviceOperations, statefulSetOperations, pvcOperations);
        this.kafkaConnectClusterOperations = new KafkaConnectClusterOperations(vertx, isOpenShift, configMapOperations, deploymentOperations, serviceOperations);

        if (isOpenShift) {
            imagesStreamOperations = new ImageStreamOperations(vertx, client.adapt(OpenShiftClient.class));
            buildConfigOperations = new BuildConfigOperations(vertx, client.adapt(OpenShiftClient.class));
            this.deploymentConfigOperations = new DeploymentConfigOperations(vertx, client.adapt(OpenShiftClient.class));
        } else {
            this.deploymentConfigOperations = null;
        }
        this.kafkaConnectS2IClusterOperations = new KafkaConnectS2IClusterOperations(vertx, isOpenShift,
                configMapOperations, deploymentConfigOperations,
                serviceOperations, imagesStreamOperations, buildConfigOperations);
    }

    @Override
    public void start(Future<Void> start) {
        log.info("Starting ClusterController");

        // Configure the executor here, but it is used only in other places
        getVertx().createSharedWorkerExecutor("kubernetes-ops-pool", 5, TimeUnit.SECONDS.toNanos(120));

        createConfigMapWatch(res -> {
            if (res.succeeded())    {
                configMapWatch = res.result();

                log.info("Setting up periodical reconciliation");
                this.reconcileTimer = vertx.setPeriodic(this.reconciliationInterval, res2 -> {
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

    private String name(HasMetadata resource) {
        return resource.getMetadata().getName();
    }

    private String nameFromLabels(HasMetadata resource) {
        return resource.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL);
    }

    private void createConfigMapWatch(Handler<AsyncResult<Watch>> handler) {
        getVertx().executeBlocking(
                future -> {
                    Watch watch = client.configMaps().inNamespace(namespace).withLabels(labels).watch(new Watcher<ConfigMap>() {
                        @Override
                        public void eventReceived(Action action, ConfigMap cm) {
                            Map<String, String> labels = cm.getMetadata().getLabels();
                            String type = labels.get(ClusterController.STRIMZI_TYPE_LABEL);

                            final AbstractClusterOperations<?> cluster;
                            if (type == null) {
                                log.warn("Missing type in Config Map {}", cm.getMetadata().getName());
                                return;
                            } else if (type.equals(KafkaCluster.TYPE)) {
                                cluster = kafkaClusterOperations;
                            } else if (type.equals(KafkaConnectCluster.TYPE)) {
                                cluster = kafkaConnectClusterOperations;
                            } else if (type.equals(KafkaConnectS2ICluster.TYPE)) {
                                cluster = kafkaConnectS2IClusterOperations;
                            } else {
                                log.warn("Unknown type {} received in Config Map {}", labels.get(ClusterController.STRIMZI_TYPE_LABEL), cm.getMetadata().getName());
                                return;
                            }

                            switch (action) {
                                case ADDED:
                                    log.info("New ConfigMap {}", cm.getMetadata().getName());
                                    cluster.create(namespace, name(cm));
                                    break;
                                case DELETED:
                                    log.info("Deleted ConfigMap {}", cm.getMetadata().getName());
                                    cluster.delete(namespace, name(cm));
                                    break;
                                case MODIFIED:
                                    log.info("Modified ConfigMap {}", cm.getMetadata().getName());
                                    cluster.update(namespace, name(cm));
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

        if (kafkaConnectS2IClusterOperations != null) {
            reconcileKafkaConnectS2I();
        }
    }

    private void reconcileKafka() {
        log.info("Reconciling Kafka clusters ...");

        Map<String, String> kafkaLabels = new HashMap(labels);
        kafkaLabels.put(ClusterController.STRIMZI_TYPE_LABEL, KafkaCluster.TYPE);

        List<ConfigMap> cms = configMapOperations.list(namespace, kafkaLabels);
        List<StatefulSet> sss = statefulSetOperations.list(namespace, kafkaLabels);

        Set<String> cmsNames = cms.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toSet());
        Set<String> sssNames = sss.stream().map(cm -> cm.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL)).collect(Collectors.toSet());

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
            kafkaClusterOperations.create(namespace, name(cm));
        }
    }

    private void updateKafkaClusters(List<ConfigMap> update)   {
        for (ConfigMap cm : update) {
            log.info("Reconciliation: Kafka cluster {} should be checked for updates", cm.getMetadata().getName());
            kafkaClusterOperations.update(namespace, name(cm));
        }
    }

    private void deleteKafkaClusters(List<StatefulSet> delete)   {
        for (StatefulSet ss : delete) {
            log.info("Reconciliation: Kafka cluster {} should be deleted", ss.getMetadata().getName());
            kafkaClusterOperations.delete(namespace, nameFromLabels(ss));
        }
    }

    private void reconcileKafkaConnect() {
        log.info("Reconciling Kafka Connect clusters ...");

        Map<String, String> kafkaLabels = new HashMap(labels);
        kafkaLabels.put(ClusterController.STRIMZI_TYPE_LABEL, KafkaConnectCluster.TYPE);

        List<ConfigMap> cms = configMapOperations.list(namespace, kafkaLabels);
        List<Deployment> deps = deploymentOperations.list(namespace, kafkaLabels);

        Set<String> cmsNames = cms.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toSet());
        Set<String> depsNames = deps.stream().map(cm -> cm.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL)).collect(Collectors.toSet());

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
            kafkaConnectClusterOperations.create(namespace, name(cm));
        }
    }

    private void updateKafkaConnectClusters(List<ConfigMap> update)   {
        for (ConfigMap cm : update) {
            log.info("Reconciliation: Kafka Connect cluster {} should be checked for updates", cm.getMetadata().getName());
            kafkaConnectClusterOperations.update(namespace, name(cm));
        }
    }

    private void deleteKafkaConnectClusters(List<Deployment> delete)   {
        for (Deployment dep : delete) {
            log.info("Reconciliation: Kafka Connect cluster {} should be deleted", dep.getMetadata().getName());
            kafkaConnectClusterOperations.delete(namespace, nameFromLabels(dep));
        }
    }

    private void reconcileKafkaConnectS2I() {
        log.info("Reconciling Kafka Connect S2I clusters ...");

        Map<String, String> kafkaLabels = new HashMap(labels);
        kafkaLabels.put(ClusterController.STRIMZI_TYPE_LABEL, KafkaConnectS2ICluster.TYPE);

        List<ConfigMap> cms = configMapOperations.list(namespace, kafkaLabels);
        List<DeploymentConfig> deps = deploymentConfigOperations.list(namespace, kafkaLabels);

        Set<String> cmsNames = cms.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toSet());
        Set<String> depsNames = deps.stream().map(cm -> cm.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL)).collect(Collectors.toSet());

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
            kafkaConnectS2IClusterOperations.create(namespace, name(cm));
        }
    }

    private void updateKafkaConnectS2IClusters(List<ConfigMap> update)   {
        for (ConfigMap cm : update) {
            log.info("Reconciliation: Kafka Connect S2I cluster {} should be checked for updates", cm.getMetadata().getName());
            kafkaConnectS2IClusterOperations.update(namespace, name(cm));
        }
    }

    private void deleteKafkaConnectS2IClusters(List<DeploymentConfig> delete)   {
        for (DeploymentConfig dep : delete) {
            log.info("Reconciliation: Kafka Connect S2I cluster {} should be deleted", dep.getMetadata().getName());
            kafkaConnectS2IClusterOperations.delete(namespace, nameFromLabels(dep));
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

}
