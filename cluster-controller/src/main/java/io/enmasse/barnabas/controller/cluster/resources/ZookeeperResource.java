package io.enmasse.barnabas.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetUpdateStrategyBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZookeeperResource extends AbstractResource {

    private final String headlessName;

    private final int clientPort = 2181;
    private final String clientPortName = "clients";
    private final int clusteringPort = 2888;
    private final String clusteringPortName = "clustering";
    private final int leaderElectionPort = 3888;
    private final String leaderElectionPortName = "leader-election";
    private final String mounthPath = "/var/lib/zookeeper";
    private final String volumeName = "zookeeper-storage";

    // Number of replicas
    private int replicas = DEFAULT_REPLICAS;

    // Docker image configuration
    private String image = DEFAULT_IMAGE;

    private String healthCheckScript = "/opt/zookeeper/zookeeper_healthcheck.sh";
    private int healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
    private int healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;

    // Zookeeper configuration
    // N/A

    // Configuration defaults
    private static String DEFAULT_IMAGE = "enmasseproject/zookeeper:latest";
    private static int DEFAULT_REPLICAS = 3;
    private static int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static int DEFAULT_HEALTHCHECK_TIMEOUT = 5;

    // Zookeeper configuration defaults
    // N/A

    // Configuration keys
    private static String KEY_IMAGE = "zookeeper-image";
    private static String KEY_REPLICAS = "zookeeper-nodes";
    private static String KEY_HEALTHCHECK_DELAY = "zookeeper-healthcheck-delay";
    private static String KEY_HEALTHCHECK_TIMEOUT = "zookeeper-healthcheck-timeout";

    // Zookeeper configuration keys
    private static String KEY_ZOOKEEPER_NODE_COUNT = "ZOOKEEPER_NODE_COUNT";

    private ZookeeperResource(String name, String namespace) {
        super(namespace, name);
        this.headlessName = name + "-headless";
    }

    public static ZookeeperResource fromConfigMap(ConfigMap cm) {
        String name = cm.getMetadata().getName() + "-zookeeper";
        ZookeeperResource zk = new ZookeeperResource(name, cm.getMetadata().getNamespace());

        zk.setLabels(cm.getMetadata().getLabels());

        zk.setReplicas(Integer.parseInt(cm.getData().getOrDefault(KEY_REPLICAS, String.valueOf(DEFAULT_REPLICAS))));
        zk.setImage(cm.getData().getOrDefault(KEY_IMAGE, DEFAULT_IMAGE));
        zk.setHealthCheckInitialDelay(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_DELAY, String.valueOf(DEFAULT_HEALTHCHECK_DELAY))));
        zk.setHealthCheckTimeout(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_TIMEOUT, String.valueOf(DEFAULT_HEALTHCHECK_TIMEOUT))));

        return zk;
    }

    // This is currently needed only to delete the headless service. All other deletions can be done just using namespace
    // and name. Do we need this as it is? Or would it be enough to create just and empty shell from name and namespace
    // which would generate the headless name?
    public static ZookeeperResource fromStatefulSet(StatefulSet ss) {
        String name = ss.getMetadata().getName();
        ZookeeperResource zk =  new ZookeeperResource(name, ss.getMetadata().getNamespace());

        zk.setLabels(ss.getMetadata().getLabels());
        zk.setReplicas(ss.getSpec().getReplicas());
        zk.setImage(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        zk.setHealthCheckInitialDelay(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        zk.setHealthCheckInitialDelay(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());

        return zk;
    }

    public ResourceDiffResult diff(StatefulSet ss)  {
        ResourceDiffResult diff = new ResourceDiffResult();

        if (replicas > ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            diff.setScaleUp(true);
            diff.setRollingUpdate(true);
        }
        else if (replicas < ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            diff.setScaleDown(true);
            diff.setRollingUpdate(true);
        }

        if (!getLabelsWithName().equals(ss.getMetadata().getLabels()))    {
            log.info("Diff: Expected labels {}, actual labels {}", getLabelsWithName(), ss.getMetadata().getLabels());
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        if (!image.equals(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage())) {
            log.info("Diff: Expected image {}, actual image {}", image, ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        if (healthCheckInitialDelay != ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds()
                || healthCheckTimeout != ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds()) {
            log.info("Diff: Zookeeper healthcheck timing changed");
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        return diff;
    }

    public Service generateService() {

        return createService("ClusterIP",
                createServicePort(clientPortName, clientPort, clientPort, "TCP"));
    }

    public Service patchService(Service svc) {
        svc.getMetadata().setLabels(getLabelsWithName());
        svc.getSpec().setSelector(getLabelsWithName());

        return svc;
    }

    public Service generateHeadlessService() {

        return createHeadlessService(headlessName, getServicePortList());
    }

    public Service patchHeadlessService(Service svc) {
        svc.getMetadata().setLabels(getLabelsWithName(headlessName));
        svc.getSpec().setSelector(getLabelsWithName());

        return svc;
    }

    public StatefulSet generateStatefulSet() {
        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(image)
                .withEnv(getEnvList())
                .withVolumeMounts(createVolumeMount(volumeName, mounthPath))
                .withPorts(getContainerPortList())
                .withLivenessProbe(createExecProbe(healthCheckScript, healthCheckInitialDelay, healthCheckTimeout))
                .withReadinessProbe(createExecProbe(healthCheckScript, healthCheckInitialDelay, healthCheckTimeout))
                .build();

        StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(getLabelsWithName())
                .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                .withPodManagementPolicy("Parallel")
                .withUpdateStrategy(new StatefulSetUpdateStrategyBuilder().withType("OnDelete").build())
                .withServiceName(headlessName)
                .withReplicas(replicas)
                .withSelector(new LabelSelectorBuilder().withMatchLabels(getLabelsWithName()).build())
                .withNewTemplate()
                .withNewMetadata()
                .withName(name)
                .withLabels(getLabelsWithName())
                .endMetadata()
                .withNewSpec()
                .withContainers(container)
                .withVolumes(createEmptyDirVolume(volumeName))
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();

        return statefulSet;
    }

    public StatefulSet patchStatefulSet(StatefulSet statefulSet) {
        statefulSet.getMetadata().setLabels(getLabelsWithName());
        statefulSet.getSpec().setSelector(new LabelSelectorBuilder().withMatchLabels(getLabelsWithName()).build());
        statefulSet.getSpec().getTemplate().getMetadata().setLabels(getLabelsWithName());
        statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).setImage(image);
        statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).setLivenessProbe(createExecProbe(healthCheckScript, healthCheckInitialDelay, healthCheckTimeout));
        statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).setReadinessProbe(createExecProbe(healthCheckScript, healthCheckInitialDelay, healthCheckTimeout));
        statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(getEnvList());

        return statefulSet;
    }

    private List<EnvVar> getEnvList() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(new EnvVarBuilder().withName(KEY_ZOOKEEPER_NODE_COUNT).withValue(Integer.toString(replicas)).build());

        return varList;
    }

    private List<ServicePort> getServicePortList() {
        List<ServicePort> portList = new ArrayList<>();
        portList.add(createServicePort(clientPortName, clientPort, clientPort, "TCP"));
        portList.add(createServicePort(clusteringPortName, clusteringPort, clusteringPort, "TCP"));
        portList.add(createServicePort(leaderElectionPortName, leaderElectionPort, leaderElectionPort, "TCP"));

        return portList;
    }

    private List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>();
        portList.add(createContainerPort(clientPortName, clientPort, "TCP"));
        portList.add(createContainerPort(clusteringPortName, clusteringPort, "TCP"));
        portList.add(createContainerPort(leaderElectionPortName, leaderElectionPort,"TCP"));

        return portList;
    }

    public void setLabels(Map<String, String> labels) {
        Map<String, String> newLabels = new HashMap<>(labels);

        if (newLabels.containsKey("kind") && newLabels.get("kind").equals("kafka")) {
            newLabels.put("kind", "zookeeper");
        }

        super.setLabels(newLabels);
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public void setHealthCheckScript(String healthCheckScript) {
        this.healthCheckScript = healthCheckScript;
    }

    public void setHealthCheckTimeout(int healthCheckTimeout) {
        this.healthCheckTimeout = healthCheckTimeout;
    }

    public void setHealthCheckInitialDelay(int healthCheckInitialDelay) {
        this.healthCheckInitialDelay = healthCheckInitialDelay;
    }

    public int getReplicas() {
        return replicas;
    }

    public String getHeadlessName() {
        return headlessName;
    }
}
