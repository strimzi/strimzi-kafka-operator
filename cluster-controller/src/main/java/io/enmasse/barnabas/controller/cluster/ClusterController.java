package io.enmasse.barnabas.controller.cluster;

import io.enmasse.barnabas.controller.cluster.resources.KafkaConnectResource;
import io.enmasse.barnabas.controller.cluster.resources.KafkaResource;
import io.enmasse.barnabas.controller.cluster.resources.ZookeeperResource;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterController extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(ClusterController.class.getName());

    private final K8SUtils k8s;
    private final Map<String, String> labels;
    private final String namespace;

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

        this.executor = getVertx().createSharedWorkerExecutor("kubernetes-ops-pool", 5, 60000000000l); // time is in ns!

        getVertx().executeBlocking(
                future -> {
                    k8s.getKubernetesClient().configMaps().inNamespace(namespace).withLabels(labels).watch(new Watcher<ConfigMap>() {
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
                            //TODO: Restart or reconnect?
                            log.info("Watcher closed", e);
                        }
                    });
                    future.complete();
                }, res -> {
                    if (res.succeeded())    {
                        log.info("ClusterController up and running");

                        log.info("Setting up periodical reconciliation");
                        /*vertx.setPeriodic(60000, res2 -> {
                            log.info("Triggering periodic reconciliation ...");
                            reconcile();
                        });

                        start.complete();
                        reconcile();*/
                    }
                    else {
                        log.info("ClusterController startup failed");
                        start.fail("ClusterController startup failed");
                    }
                }
        );
    }

    private void reconcile()    {
        log.info("Reconciling ...");

        List<ConfigMap> cms = k8s.getConfigmaps(namespace, labels);
        List<StatefulSet> sss = k8s.getStatefulSets(namespace, labels);

        List<String> cmsNames = cms.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toList());
        List<String> sssNames = sss.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toList());

        List<ConfigMap> addList = cms.stream().filter(cm -> !sssNames.contains(cm.getMetadata().getName())).collect(Collectors.toList());
        List<ConfigMap> updateList = cms.stream().filter(cm -> sssNames.contains(cm.getMetadata().getName())).collect(Collectors.toList());
        List<StatefulSet> deletionList = sss.stream().filter(ss -> !cmsNames.contains(ss.getMetadata().getName())).collect(Collectors.toList());

        addClusters(addList);
        deleteClusters(deletionList);
        updateClusters(updateList);
    }

    private void addClusters(List<ConfigMap> add)   {
        for (ConfigMap cm : add) {
            log.info("Cluster {} should be added", cm.getMetadata().getName());
            addKafkaCluster(cm);
        }
    }

    private void updateClusters(List<ConfigMap> update)   {
        for (ConfigMap cm : update) {
            log.info("Cluster {} should be checked for updates", cm.getMetadata().getName());
            updateKafkaCluster(cm);
        }
    }

    private void deleteClusters(List<StatefulSet> delete)   {
        for (StatefulSet ss : delete) {
            log.info("Cluster {} should be deleted", ss.getMetadata().getName());
            deleteKafkaCluster(ss);
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

        KafkaResource.fromConfigMap(cm, vertx, k8s).update(res2 -> {
            if (res2.succeeded()) {
                log.info("Kafka cluster updated {}", name);
            }
            else {
                log.error("Failed to update Kafka cluster {}.", name);
            }
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
