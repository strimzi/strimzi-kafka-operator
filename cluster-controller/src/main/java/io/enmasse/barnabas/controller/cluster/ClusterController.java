package io.enmasse.barnabas.controller.cluster;

import io.enmasse.barnabas.controller.cluster.model.Zookeeper;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        Zookeeper.fromConfigMap(add, kubernetesClient).create();
        createKafka(add);
    }

    private void createKafka(ConfigMap add)   {
        String name = add.getMetadata().getName();

        Service svc = new ServiceBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withType("ClusterIP")
                .withSelector(new HashMap<String, String>(){{put("name", name);}})
                .addNewPort()
                .withPort(9092)
                .withNewTargetPort(9092)
                .withProtocol("TCP")
                .withName("kafka")
                .endPort()
                .endSpec()
                .build();
        kubernetesClient.services().inNamespace(namespace).create(svc);

        Service headlessSvc = new ServiceBuilder()
                .withNewMetadata()
                .withName(name + "-headless")
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withType("ClusterIP")
                .withClusterIP("None")
                .withSelector(new HashMap<String, String>(){{put("name", name);}})
                .addNewPort()
                .withPort(9092)
                .withNewTargetPort(9092)
                .withProtocol("TCP")
                .withName("kafka")
                .endPort()
                .endSpec()
                .build();
        kubernetesClient.services().inNamespace(namespace).create(headlessSvc);

        Container container = new ContainerBuilder()
                .withName(name)
                .withImage("enmasseproject/kafka-statefulsets:latest")
                .withVolumeMounts(new VolumeMountBuilder().withName("kafka-storage").withMountPath("/var/lib/kafka/").build())
                .withPorts(new ContainerPortBuilder().withName("kafka").withProtocol("TCP").withContainerPort(9092).build())
                .withNewLivenessProbe()
                .withNewExec()
                .withCommand("/opt/kafka/kafka_healthcheck.sh")
                .endExec()
                .withInitialDelaySeconds(15)
                .withTimeoutSeconds(5)
                .endLivenessProbe()
                .withNewReadinessProbe()
                .withNewExec()
                .withCommand("/opt/kafka/kafka_healthcheck.sh")
                .endExec()
                .withInitialDelaySeconds(15)
                .withTimeoutSeconds(5)
                .endReadinessProbe()
                .build();
        Volume volume = new VolumeBuilder()
                .withName(container.getVolumeMounts().get(0).getName())
                .withNewEmptyDir()
                .endEmptyDir()
                .build();
        StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withServiceName(headlessSvc.getMetadata().getName())
                .withReplicas(1)
                .withNewTemplate()
                .withNewMetadata()
                .withName(name)
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withContainers(container)
                .withVolumes(volume)
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
        kubernetesClient.apps().statefulSets().inNamespace(namespace).create(statefulSet);
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
        deleteZoo(ss);
        deleteKafka(ss);
    }

    private void deleteZoo(StatefulSet ss) {
        Zookeeper.fromStatefulSet(ss, kubernetesClient).delete();
    }

    private void deleteKafka(StatefulSet ss) {
        String name = ss.getMetadata().getName();
        kubernetesClient.apps().statefulSets().inNamespace(namespace).withName(name).delete();
        kubernetesClient.services().inNamespace(namespace).withName(name).delete();
        kubernetesClient.services().inNamespace(namespace).withName(name + "-headless").delete();
    }
}
