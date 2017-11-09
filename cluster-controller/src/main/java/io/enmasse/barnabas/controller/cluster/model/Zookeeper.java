package io.enmasse.barnabas.controller.cluster.model;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.HashMap;
import java.util.Map;

public class Zookeeper {
    private final KubernetesClient client;
    private final String name;
    private final String namespace;
    private final String headlessName;

    private final int clientPort = 2181;
    private final int clusteringPort = 2888;
    private final int leaderElectionPort = 3888;
    private final String mounthPath = "/var/lib/zookeeper";
    private final String volumeName = "zookeeper-storage";

    private Map<String, String> labels = new HashMap<>();
    private int replicas = 1;
    private String image = "enmasseproject/zookeeper:latest";
    private String livenessProbeScript = "/opt/zookeeper/zookeeper_healthcheck.sh";
    private int livenessProbeTimeout = 5;
    private int livenessProbeInitialDelay = 15;
    private String readinessProbeScript = "/opt/zookeeper/zookeeper_healthcheck.sh";
    private int readinessProbeTimeout = 5;
    private int readinessProbeInitialDelay = 15;

    private Zookeeper(String name, String namespace, KubernetesClient client) {
        this.name = name;
        this.headlessName = name + "-headless";
        this.namespace = namespace;
        this.client = client;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> labels) {
        if (labels.containsKey("kind") && labels.get("kind").equals("kafka")) {
            labels.put("kind", "zookeeper");
        }

        this.labels = labels;
    }

    public static Zookeeper fromConfigMap(ConfigMap cm, KubernetesClient client) {
        String name = cm.getMetadata().getName() + "-zookeeper";
        Zookeeper zk = new Zookeeper(name, cm.getMetadata().getNamespace(), client);
        zk.setLabels(cm.getMetadata().getLabels());
        return zk;
    }

    public static Zookeeper fromStatefulSet(StatefulSet ss, KubernetesClient client) {
        String name = ss.getMetadata().getName() + "-zookeeper";
        Zookeeper zk =  new Zookeeper(name, ss.getMetadata().getNamespace(), client);
        zk.setLabels(ss.getMetadata().getLabels());
        return zk;
    }

    private boolean statefulSetExists() {
        return client.apps().statefulSets().inNamespace(namespace).withName(name).get() == null ? false : true;
    }

    private boolean serviceExists() {
        return client.services().inNamespace(namespace).withName(name).get() == null ? false : true;
    }

    private boolean headlessServiceExists() {
        return client.services().inNamespace(namespace).withName(headlessName).get() == null ? false : true;
    }

    public void create() {
        createService();
        createHeadlessService();
        createStatefulSet();
    }

    private void createService() {
        Service svc = new ServiceBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withType("ClusterIP")
                .withSelector(new HashMap<String, String>(){{put("name", name);}})
                .addNewPort()
                .withPort(clientPort)
                .withNewTargetPort(clientPort)
                .withProtocol("TCP")
                .withName("clientport")
                .endPort()
                .endSpec()
                .build();
        client.services().inNamespace(namespace).createOrReplace(svc);
    }

    private void createHeadlessService() {
        Service svc = new ServiceBuilder()
                .withNewMetadata()
                .withName(headlessName)
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withType("ClusterIP")
                .withClusterIP("None")
                .withSelector(new HashMap<String, String>(){{put("name", name);}})
                .addNewPort()
                .withPort(clientPort)
                .withNewTargetPort(clientPort)
                .withProtocol("TCP")
                .withName("clientport")
                .endPort()
                .addNewPort()
                .withPort(clusteringPort)
                .withNewTargetPort(clusteringPort)
                .withProtocol("TCP")
                .withName("clustering")
                .endPort()
                .addNewPort()
                .withPort(leaderElectionPort)
                .withNewTargetPort(leaderElectionPort)
                .withProtocol("TCP")
                .withName("leaderelection")
                .endPort()
                .endSpec()
                .build();
        client.services().inNamespace(namespace).createOrReplace(svc);
    }

    private void createStatefulSet() {
        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(image)
                .withEnv(new EnvVarBuilder().withName("ZOOKEEPER_NODE_COUNT").withValue(Integer.toString(replicas)).build())
                .withVolumeMounts(new VolumeMountBuilder().withName(volumeName).withMountPath(mounthPath).build())
                .withPorts(new ContainerPortBuilder().withName("clientport").withProtocol("TCP").withContainerPort(clientPort).build(),
                        new ContainerPortBuilder().withName("clustering").withProtocol("TCP").withContainerPort(clusteringPort).build(),
                        new ContainerPortBuilder().withName("leaderelection").withProtocol("TCP").withContainerPort(leaderElectionPort).build())
                .withNewLivenessProbe()
                .withNewExec()
                .withCommand(livenessProbeScript)
                .endExec()
                .withInitialDelaySeconds(livenessProbeInitialDelay)
                .withTimeoutSeconds(livenessProbeTimeout)
                .endLivenessProbe()
                .withNewReadinessProbe()
                .withNewExec()
                .withCommand(readinessProbeScript)
                .endExec()
                .withInitialDelaySeconds(readinessProbeInitialDelay)
                .withTimeoutSeconds(readinessProbeTimeout)
                .endReadinessProbe()
                .build();
        Volume volume = new VolumeBuilder()
                .withName(volumeName)
                .withNewEmptyDir()
                .endEmptyDir()
                .build();
        StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withServiceName(headlessName)
                .withReplicas(replicas)
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
        client.apps().statefulSets().inNamespace(namespace).createOrReplace(statefulSet);
    }

    public void delete() {
        deleteService();
        deleteStatefulSet();
        deleteHeadlessService();
    }

    private void deleteService() {
        if (serviceExists()) {
            client.services().inNamespace(namespace).withName(name).delete();
        }
    }

    private void deleteHeadlessService() {
        if (headlessServiceExists()) {
            client.services().inNamespace(namespace).withName(headlessName).delete();
        }
    }

    private void deleteStatefulSet() {
        if (statefulSetExists()) {
            client.apps().statefulSets().inNamespace(namespace).withName(name).delete();
        }
    }
}
