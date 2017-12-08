package io.enmasse.barnabas.controller.cluster;

import io.enmasse.barnabas.controller.cluster.resources.KafkaConnectResource;
import io.enmasse.barnabas.controller.cluster.resources.KafkaResource;
import io.enmasse.barnabas.controller.cluster.resources.ZookeeperResource;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.vertx.core.*;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterController extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(ClusterController.class.getName());

    private final K8SUtils k8s;
    private final Map<String, String> labels;
    private final String namespace;

    private Watch configMapWatch;

    //private Map<ResourceId, Resource> resources = new HashMap<ResourceId, Resource<S, ServiceFluentImpl<DoneableService>>>();

    private WorkerExecutor executor;

    public ClusterController(ClusterControllerConfig config) throws Exception {
        log.info("Creating ClusterController");

        this.namespace = config.getNamespace();
        this.labels = config.getLabels();
        this.k8s = new K8SUtils(new DefaultKubernetesClient());
    }

    @Override
    public void start(Future<Void> start) {
        log.info("Starting ClusterController");

        this.executor = getVertx().createSharedWorkerExecutor("kubernetes-ops-pool", 5, 120000000000l); // time is in ns!

        createConfigMapWatch(res -> {
            if (res.succeeded())    {
                configMapWatch = res.result();

                log.info("Setting up periodical reconciliation");
                vertx.setPeriodic(120000, res2 -> {
                    log.info("Triggering periodic reconciliation ...");
                    reconcile();
                });

                log.info("ClusterController up and running");
                start.complete();
            }
            else {
                log.error("ClusterController startup failed");
                start.fail("ClusterController startup failed");
            }
        });
    }

    private void createConfigMapWatch(Handler<AsyncResult<Watch>> handler) {
        getVertx().executeBlocking(
                future -> {
                    Watch watch = k8s.getKubernetesClient().configMaps().inNamespace(namespace).withLabels(labels).watch(new Watcher<ConfigMap>() {
                        @Override
                        public void eventReceived(Action action, ConfigMap cm) {
                            Map<String, String> labels = cm.getMetadata().getLabels();
                            String kind;

                            if (!labels.containsKey("kind")) {
                                log.warn("Missing kind in Config Map {}", cm.getMetadata().getName());
                                return;
                            }
                            else if (!labels.get("kind").equals("kafka") && !labels.get("kind").equals("kafka-connect")) {
                                log.warn("Unknown kind {} received in Config Map {}", labels.get("kind"), cm.getMetadata().getName());
                                return;
                            }
                            else {
                                kind = labels.get("kind");
                            }

                            switch (action) {
                                case ADDED:
                                    log.info("New ConfigMap {}", cm.getMetadata().getName());
                                    if (kind.equals("kafka")) {
                                        addKafkaCluster(cm);
                                    }
                                    else if (kind.equals("kafka-connect")) {
                                        addKafkaConnectCluster(cm);
                                    }
                                    break;
                                case DELETED:
                                    log.info("Deleted ConfigMap {}", cm.getMetadata().getName());
                                    if (kind.equals("kafka")) {
                                        deleteKafkaCluster(cm);
                                    }
                                    else if (kind.equals("kafka-connect")) {
                                        deleteKafkaConnectCluster(cm);
                                    }
                                    break;
                                case MODIFIED:
                                    log.info("Modified ConfigMap {}", cm.getMetadata().getName());
                                    if (kind.equals("kafka")) {
                                        updateKafkaCluster(cm);
                                    }
                                    else if (kind.equals("kafka-connect")) {
                                        updateKafkaConnectCluster(cm);
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
                                log.error("Watcher closed with exception", e);
                            }

                            recreateConfigMapWatch();
                        }
                    });
                    future.complete(watch);
                }, res -> {
                    if (res.succeeded())    {
                        log.info("ConfigMap watcher up and running for lables {}", labels);
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
    }

    private void reconcileKafka() {
        log.info("Reconciling Kafka clusters ...");

        Map<String, String> kafkaLabels = new HashMap(labels);
        kafkaLabels.put("kind", "kafka");

        List<ConfigMap> cms = k8s.getConfigmaps(namespace, kafkaLabels);
        List<StatefulSet> sss = k8s.getStatefulSets(namespace, kafkaLabels);

        List<String> cmsNames = cms.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toList());
        List<String> sssNames = sss.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toList());

        List<ConfigMap> addList = cms.stream().filter(cm -> !sssNames.contains(cm.getMetadata().getName())).collect(Collectors.toList());
        List<ConfigMap> updateList = cms.stream().filter(cm -> sssNames.contains(cm.getMetadata().getName())).collect(Collectors.toList());
        List<StatefulSet> deletionList = sss.stream().filter(ss -> !cmsNames.contains(ss.getMetadata().getName())).collect(Collectors.toList());

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
        kafkaLabels.put("kind", "kafka-connect");

        List<ConfigMap> cms = k8s.getConfigmaps(namespace, kafkaLabels);
        List<Deployment> deps = k8s.getDeployments(namespace, kafkaLabels);

        List<String> cmsNames = cms.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toList());
        List<String> sssNames = deps.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toList());

        List<ConfigMap> addList = cms.stream().filter(cm -> !sssNames.contains(cm.getMetadata().getName())).collect(Collectors.toList());
        List<ConfigMap> updateList = cms.stream().filter(cm -> sssNames.contains(cm.getMetadata().getName())).collect(Collectors.toList());
        List<Deployment> deletionList = deps.stream().filter(dep -> !cmsNames.contains(dep.getMetadata().getName())).collect(Collectors.toList());

        addKafkaConnectClusters(addList);
        deleteConnectConnectClusters(deletionList);
        updateKafkaConnectClusters(updateList);
    }

    private void addKafkaConnectClusters(List<ConfigMap> add)   {
        for (ConfigMap cm : add) {
            log.info("Reconciliation: Kafka Connect cluster {} should be added", cm.getMetadata().getName());
            addKafkaCluster(cm);
        }
    }

    private void updateKafkaConnectClusters(List<ConfigMap> update)   {
        for (ConfigMap cm : update) {
            log.info("Reconciliation: Kafka Connect cluster {} should be checked for updates", cm.getMetadata().getName());
            updateKafkaConnectCluster(cm);
        }
    }

    private void deleteConnectConnectClusters(List<Deployment> delete)   {
        for (Deployment dep : delete) {
            log.info("Reconciliation: Kafka Connect cluster {} should be deleted", dep.getMetadata().getName());
            deleteKafkaConnectCluster(dep);
        }
    }

    /*
      Kafka / Zookeeper cluster control
     */
    private void addKafkaCluster(ConfigMap add)   {
        String name = add.getMetadata().getName();
        log.info("Adding cluster {}", name);

        ZookeeperResource.fromConfigMap(add, vertx, k8s).create(res -> {
            if (res.succeeded()) {
                log.info("Zookeeper cluster added {}", name);
                KafkaResource.fromConfigMap(add, vertx, k8s).create(res2 -> {
                    if (res2.succeeded()) {
                        log.info("Kafka cluster added {}", name);
                    }
                    else {
                        log.error("Failed to add Kafka cluster {}.", name);
                    }
                });
            }
            else {
                log.error("Failed to add Zookeeper cluster {}. SKipping Kafka cluster creation.", name);
            }
        });
    }

    private void updateKafkaCluster(ConfigMap cm)   {
        String name = cm.getMetadata().getName();
        log.info("Checking for updates in cluster {}", cm.getMetadata().getName());

        ZookeeperResource.fromConfigMap(cm, vertx, k8s).update(res -> {
            if (res.succeeded()) {
                log.info("Zookeeper cluster updated {}", name);
            }
            else {
                log.error("Failed to update Zookeeper cluster {}.", name);
            }

            KafkaResource.fromConfigMap(cm, vertx, k8s).update(res2 -> {
                if (res2.succeeded()) {
                    log.info("Kafka cluster updated {}", name);
                }
                else {
                    log.error("Failed to update Kafka cluster {}.", name);
                }
            });
        });
    }

    private void deleteKafkaCluster(StatefulSet ss)   {
        String name = ss.getMetadata().getName();
        log.info("Deleting cluster {}", name);

        KafkaResource.fromStatefulSet(ss, vertx, k8s).delete(res -> {
            if (res.succeeded()) {
                log.info("Kafka cluster deleted {}", name);
                ZookeeperResource.fromStatefulSet(ss, vertx, k8s).delete(res2 -> {
                    if (res2.succeeded()) {
                        log.info("Zookeeper cluster deleted {}", name);
                    }
                    else {
                        log.error("Failed to delete Zookeeper cluster {}.", name);
                    }
                });
            }
            else {
                log.error("Failed to delete Kafka cluster {}. SKipping Zookeeper cluster deletion.", name);
            }
        });
    }

    private void deleteKafkaCluster(ConfigMap cm)   {
        String name = cm.getMetadata().getName();
        log.info("Deleting cluster {}", name);

        KafkaResource.fromConfigMap(cm, vertx, k8s).delete(res -> {
            if (res.succeeded()) {
                log.info("Kafka cluster deleted {}", name);
                ZookeeperResource.fromConfigMap(cm, vertx, k8s).delete(res2 -> {
                    if (res2.succeeded()) {
                        log.info("Zookeeper cluster deleted {}", name);
                    }
                    else {
                        log.error("Failed to delete Zookeeper cluster {}.", name);
                    }
                });
            }
            else {
                log.error("Failed to delete Kafka cluster {}. SKipping Zookeeper cluster deletion.", name);
            }
        });
    }

    /*
      Kafka Connect cluster control
     */
    private void addKafkaConnectCluster(ConfigMap add)   {
        String name = add.getMetadata().getName();
        log.info("Adding Kafka Connect cluster {}", name);

        KafkaConnectResource.fromConfigMap(add, vertx, k8s).create(res -> {
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

        KafkaConnectResource.fromConfigMap(cm, vertx, k8s).update(res2 -> {
            if (res2.succeeded()) {
                log.info("Kafka Connect cluster updated {}", name);
            }
            else {
                log.error("Failed to update Kafka Connect cluster {}.", name);
            }
        });
    }

    private void deleteKafkaConnectCluster(Deployment dep)   {
        String name = dep.getMetadata().getName();
        log.info("Deleting cluster {}", name);

        KafkaConnectResource.fromDeployment(dep, vertx, k8s).delete(res -> {
            if (res.succeeded()) {
                log.info("Kafka Connect cluster deleted {}", name);
            }
            else {
                log.error("Failed to delete Kafka Connect cluster {}.", name);
            }
        });
    }

    private void deleteKafkaConnectCluster(ConfigMap cm)   {
        String name = cm.getMetadata().getName();
        log.info("Deleting cluster {}", name);

        KafkaConnectResource.fromConfigMap(cm, vertx, k8s).delete(res -> {
            if (res.succeeded()) {
                log.info("Kafka Connect cluster deleted {}", name);
            }
            else {
                log.error("Failed to delete Kafka Connect cluster {}.", name);
            }
        });
    }
}
