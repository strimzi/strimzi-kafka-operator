package io.enmasse.barnabas.controller.cluster;

import io.enmasse.barnabas.controller.cluster.model.KafkaResource;
import io.enmasse.barnabas.controller.cluster.model.ZookeeperResource;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class ClusterController extends AbstractVerticle {
    private final KubernetesClient kubernetesClient;
    private final HashMap<String, String> labels;
    private final String namespace;

    public ClusterController() throws Exception {
        System.out.println("Creating ClusterController");

        this.labels = new HashMap<>();
        labels.put("app", "barnabas");
        labels.put("type", "deployment");
        labels.put("kind", "kafka");

        this.namespace = "myproject";

        this.kubernetesClient = new DefaultKubernetesClient();
    }

    @Override
    public void start(Future<Void> start) {
        System.out.println("Starting ClusterController");

        kubernetesClient.configMaps().inNamespace(namespace).withLabels(labels).watch(new Watcher<ConfigMap>() {
            @Override
            public void eventReceived(Action action, ConfigMap cm) {
                switch (action) {
                    case ADDED:
                        System.out.println("New cm: " + cm.getMetadata().getName());
                        reconcile();
                        break;
                    case DELETED:
                        System.out.println("Deleted cm: " + cm.getMetadata().getName());
                        reconcile();
                        break;
                    case MODIFIED:
                        System.out.println("Modified cm: " + cm.getMetadata().getName());
                        reconcile();
                        break;
                    case ERROR:
                        System.out.println("Failed cm: " + cm.getMetadata().getName());
                        reconcile();
                        break;
                    default:
                        System.out.println("Unknown action: " + cm.getMetadata().getName());
                        reconcile();
                }
            }

            @Override
            public void onClose(KubernetesClientException e) {
                System.out.println("Watcher closed: " + e);
            }
        });

        start.complete();
        System.out.println("Start complete");
    }

    private void reconcile()    {
        System.out.println("Reconciling ...");

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
            System.out.println("Adding cluster " + cm.getMetadata().getName());
            addCluster(cm);
        }
    }

    private void addCluster(ConfigMap add)   {
        ZookeeperResource.fromConfigMap(add, kubernetesClient).create();
        KafkaResource.fromConfigMap(add, kubernetesClient).create();
    }

    private void updateClusters(List<ConfigMap> update)   {
        // No configuration => nothing to update
    }

    private void deleteClusters(List<StatefulSet> delete)   {
        for (StatefulSet ss : delete) {
            System.out.println("Deleting cluster " + ss.getMetadata().getName());
            deleteCluster(ss);
        }
    }

    private void deleteCluster(StatefulSet ss)   {
        ZookeeperResource.fromStatefulSet(ss, kubernetesClient).delete();
        KafkaResource.fromStatefulSet(ss, kubernetesClient).delete();
    }
}
