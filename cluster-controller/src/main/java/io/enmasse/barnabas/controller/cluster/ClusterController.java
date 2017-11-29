package io.enmasse.barnabas.controller.cluster;

import io.enmasse.barnabas.controller.cluster.resources.KafkaResource;
import io.enmasse.barnabas.controller.cluster.resources.ZookeeperResource;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
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

    private final KubernetesClient kubernetesClient;
    private final Map<String, String> labels;
    private final String namespace;

    private WorkerExecutor executor;

    public ClusterController(ClusterControllerConfig config) throws Exception {
        log.info("Creating ClusterController");

        this.namespace = config.getNamespace();
        this.labels = config.getLabels();
        this.kubernetesClient = new DefaultKubernetesClient();
    }

    @Override
    public void start(Future<Void> start) {
        log.info("Starting ClusterController");

        this.executor = getVertx().createSharedWorkerExecutor("kubernetes-ops-pool", 5, 60000000000l); // time is in ns!

        getVertx().executeBlocking(
                future -> {
                    kubernetesClient.configMaps().inNamespace(namespace).withLabels(labels).watch(new Watcher<ConfigMap>() {
                        @Override
                        public void eventReceived(Action action, ConfigMap cm) {
                            switch (action) {
                                case ADDED:
                                    log.info("New ConfigMap {}", cm.getMetadata().getName());
                                    reconcile();
                                    break;
                                case DELETED:
                                    log.info("Deleted ConfigMap {}", cm.getMetadata().getName());
                                    reconcile();
                                    break;
                                case MODIFIED:
                                    log.info("Modified ConfigMap {}", cm.getMetadata().getName());
                                    reconcile();
                                    break;
                                case ERROR:
                                    log.info("Failed ConfigMap {}", cm.getMetadata().getName());
                                    reconcile();
                                    break;
                                default:
                                    log.info("Unknown action: {}", cm.getMetadata().getName());
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
                        start.complete();
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

        List<ConfigMap> cms = kubernetesClient.configMaps().inNamespace(namespace).withLabels(labels).list().getItems();
        List<StatefulSet> sss = kubernetesClient.apps().statefulSets().inNamespace(namespace).withLabels(labels).list().getItems();

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
            addCluster(cm);
        }
    }

    private void addCluster(ConfigMap add)   {
        executor.executeBlocking(
            future -> {
                log.info("Adding cluster {}", add.getMetadata().getName());
                ZookeeperResource.fromConfigMap(add, kubernetesClient).create();
                KafkaResource.fromConfigMap(add, kubernetesClient).create();
                future.complete();
            }, false, res -> {
                if (res.succeeded()) {
                    log.info("Cluster added {}", add.getMetadata().getName());
                }
                else {
                    log.error("Failed to add cluster {}", add.getMetadata().getName());
                }
            });
    }

    private void updateClusters(List<ConfigMap> update)   {
        for (ConfigMap cm : update) {
            log.info("Cluster {} should be checked for updates -> NOT IMPLEMENTED YET", cm.getMetadata().getName());
            // No configuration => nothing to update
        }
    }

    private void deleteClusters(List<StatefulSet> delete)   {
        for (StatefulSet ss : delete) {
            log.info("Cluster {} should be deleted", ss.getMetadata().getName());
            deleteCluster(ss);
        }
    }

    private void deleteCluster(StatefulSet ss)   {
        executor.executeBlocking(
            future -> {
                log.info("Deleting cluster {}", ss.getMetadata().getName());
                KafkaResource.fromStatefulSet(ss, kubernetesClient).delete();
                ZookeeperResource.fromStatefulSet(ss, kubernetesClient).delete();
                future.complete();
            }, false, res -> {
                if (res.succeeded()) {
                    log.info("Cluster deleted {}", ss.getMetadata().getName());
                }
                else {
                    log.error("Failed to delete cluster {}", ss.getMetadata().getName());
                }
            });
    }
}
