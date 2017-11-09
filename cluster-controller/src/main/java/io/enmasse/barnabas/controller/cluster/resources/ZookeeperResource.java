package io.enmasse.barnabas.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ZookeeperResource extends AbstractResource {
    private static final Logger log = LoggerFactory.getLogger(ZookeeperResource.class.getName());

    private final KubernetesClient client;
    private final String name;
    private final String namespace;
    private final String headlessName;

    private final int clientPort = 2181;
    private final int clusteringPort = 2888;
    private final int leaderElectionPort = 3888;
    private final String mounthPath = "/var/lib/zookeeper";
    private final String volumeName = "zookeeper-storage";

    private int replicas = 1;
    private String image = "enmasseproject/zookeeper:latest";
    private String livenessProbeScript = "/opt/zookeeper/zookeeper_healthcheck.sh";
    private int livenessProbeTimeout = 5;
    private int livenessProbeInitialDelay = 15;
    private String readinessProbeScript = "/opt/zookeeper/zookeeper_healthcheck.sh";
    private int readinessProbeTimeout = 5;
    private int readinessProbeInitialDelay = 15;

    private ZookeeperResource(String name, String namespace, KubernetesClient client) {
        this.name = name;
        this.headlessName = name + "-headless";
        this.namespace = namespace;
        this.client = client;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> newLabels) {
        this.labels = new HashMap<String, String>(newLabels);

        if (labels.containsKey("kind") && labels.get("kind").equals("kafka")) {
            labels.put("kind", "zookeeper");
        }

        this.labels = labels;
    }

    public static ZookeeperResource fromConfigMap(ConfigMap cm, KubernetesClient client) {
        String name = cm.getMetadata().getName() + "-zookeeper";
        ZookeeperResource zk = new ZookeeperResource(name, cm.getMetadata().getNamespace(), client);
        zk.setLabels(cm.getMetadata().getLabels());
        return zk;
    }

    public static ZookeeperResource fromStatefulSet(StatefulSet ss, KubernetesClient client) {
        String name = ss.getMetadata().getName() + "-zookeeper";
        ZookeeperResource zk =  new ZookeeperResource(name, ss.getMetadata().getNamespace(), client);
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
        log.info("Creating Zookeeper {}", name);

        createService();
        createHeadlessService();
        createStatefulSet();
    }

    private void createService() {
        log.debug("Creating Zookeeper service {}", name);

        Service svc = new ServiceBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(labelsWithName(name))
                .endMetadata()
                .withNewSpec()
                .withType("ClusterIP")
                .withSelector(new HashMap<String, String>(){{put("name", name);}})
                .withPorts(createServicePort("clientport", clientPort, clientPort))
                .endSpec()
                .build();
        client.services().inNamespace(namespace).createOrReplace(svc);
    }

    private void createHeadlessService() {
        log.debug("Creating Zookeeper headless service {}", headlessName);

        Service svc = new ServiceBuilder()
                .withNewMetadata()
                .withName(headlessName)
                .withLabels(labelsWithName(headlessName))
                .endMetadata()
                .withNewSpec()
                .withType("ClusterIP")
                .withClusterIP("None")
                .withSelector(new HashMap<String, String>(){{put("name", name);}})
                .withPorts(createServicePort("clientport", clientPort, clientPort))
                .withPorts(createServicePort("clustering", clusteringPort, clusteringPort))
                .withPorts(createServicePort("leaderelection", leaderElectionPort, leaderElectionPort))
                .endSpec()
                .build();
        client.services().inNamespace(namespace).createOrReplace(svc);
    }

    private void createStatefulSet() {
        log.debug("Creating Zookeeper stateful set {}", name);

        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(image)
                .withEnv(new EnvVarBuilder().withName("ZOOKEEPER_NODE_COUNT").withValue(Integer.toString(replicas)).build())
                .withVolumeMounts(createVolumeMount(volumeName, mounthPath))
                .withPorts(createContainerPort("clientport", clientPort),
                        createContainerPort("clustering", clusteringPort),
                        createContainerPort("leaderelection", leaderElectionPort))
                .withLivenessProbe(createExecProbe(livenessProbeScript, livenessProbeInitialDelay, livenessProbeTimeout))
                .withReadinessProbe(createExecProbe(readinessProbeScript, readinessProbeInitialDelay, readinessProbeTimeout))
                .build();

        StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(labelsWithName(name))
                .endMetadata()
                .withNewSpec()
                .withServiceName(headlessName)
                .withReplicas(replicas)
                .withNewTemplate()
                .withNewMetadata()
                .withName(name)
                .withLabels(labelsWithName(name))
                .endMetadata()
                .withNewSpec()
                .withContainers(container)
                .withVolumes(createEmptyDirVolume(volumeName))
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
        client.apps().statefulSets().inNamespace(namespace).createOrReplace(statefulSet);
    }

    public void delete() {
        log.info("Deleting Zookeeper {}", name);

        deleteService();
        deleteStatefulSet();
        deleteHeadlessService();
    }

    private void deleteService() {
        if (serviceExists()) {
            log.debug("Deleting Zookeeper service {}", name);
            client.services().inNamespace(namespace).withName(name).delete();
        }
    }

    private void deleteHeadlessService() {
        if (headlessServiceExists()) {
            log.debug("Deleting Zookeeper headless service {}", headlessName);
            client.services().inNamespace(namespace).withName(headlessName).delete();
        }
    }

    private void deleteStatefulSet() {
        if (statefulSetExists()) {
            log.debug("Deleting Zookeeper stateful set {}", name);
            client.apps().statefulSets().inNamespace(namespace).withName(name).delete();
        }
    }
}
