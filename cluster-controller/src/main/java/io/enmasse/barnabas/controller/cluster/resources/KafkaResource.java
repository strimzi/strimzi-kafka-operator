package io.enmasse.barnabas.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetUpdateStrategyBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaResource extends AbstractResource {

    private final String headlessName;

    private final int clientPort = 9092;
    private final String clientPortName = "clients";
    private final String mounthPath = "/var/lib/kafka";
    private final String volumeName = "kafka-storage";

    // Number of replicas
    private int replicas = DEFAULT_REPLICAS;

    // Docker image configuration
    private String image = DEFAULT_IMAGE;

    private String healthCheckScript = "/opt/kafka/kafka_healthcheck.sh";
    private int healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
    private int healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;

    // Kafka configuration
    private String zookeeperConnect = DEFAULT_KAFKA_ZOOKEEPER_CONNECT;
    private int defaultReplicationFactor = DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR;
    private int offsetsTopicReplicationFactor = DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR;
    private int transactionStateLogReplicationFactor = DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR;

    // Configuration defaults
    private static String DEFAULT_IMAGE = "enmasseproject/kafka-statefulsets:latest";
    private static int DEFAULT_REPLICAS = 3;
    private static int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static int DEFAULT_HEALTHCHECK_TIMEOUT = 5;

    // Kafka configuration defaults
    private static String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "zookeeper:2181";
    private static int DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR = 3;
    private static int DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = 3;
    private static int DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR = 3;

    // Configuration keys
    private static String KEY_IMAGE = "kafka-image";
    private static String KEY_REPLICAS = "kafka-nodes";
    private static String KEY_HEALTHCHECK_DELAY = "kafka-healthcheck-delay";
    private static String KEY_HEALTHCHECK_TIMEOUT = "kafka-healthcheck-timeout";

    // Kafka configuration keys
    private static String KEY_KAFKA_ZOOKEEPER_CONNECT = "KAFKA_ZOOKEEPER_CONNECT";
    private static String KEY_KAFKA_DEFAULT_REPLICATION_FACTOR = "KAFKA_DEFAULT_REPLICATION_FACTOR";
    private static String KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR";
    private static String KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR";

    private KafkaResource(String name, String namespace) {
        super(namespace, name);
        this.headlessName = name + "-headless";
    }

    public static KafkaResource fromConfigMap(ConfigMap cm) {
        KafkaResource kafka = new KafkaResource(cm.getMetadata().getName(), cm.getMetadata().getNamespace());
        kafka.setLabels(cm.getMetadata().getLabels());

        kafka.setReplicas(Integer.parseInt(cm.getData().getOrDefault(KEY_REPLICAS, String.valueOf(DEFAULT_REPLICAS))));
        kafka.setImage(cm.getData().getOrDefault(KEY_IMAGE, DEFAULT_IMAGE));
        kafka.setHealthCheckInitialDelay(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_DELAY, String.valueOf(DEFAULT_HEALTHCHECK_DELAY))));
        kafka.setHealthCheckTimeout(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_TIMEOUT, String.valueOf(DEFAULT_HEALTHCHECK_TIMEOUT))));

        kafka.setZookeeperConnect(cm.getData().getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, cm.getMetadata().getName() + "-zookeeper:2181"));
        kafka.setDefaultReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR))));
        kafka.setOffsetsTopicReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR))));
        kafka.setTransactionStateLogReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR))));

        return kafka;
    }

    // This is currently needed only to delete the headless service. All other deletions can be done just using namespace
    // and name. Do we need this as it is? Or would it be enough to create just and empty shell from name and namespace
    // which would generate the headless name?
    public static KafkaResource fromStatefulSet(StatefulSet ss) {
        KafkaResource kafka =  new KafkaResource(ss.getMetadata().getName(), ss.getMetadata().getNamespace());

        kafka.setLabels(ss.getMetadata().getLabels());
        kafka.setReplicas(ss.getSpec().getReplicas());
        kafka.setImage(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        kafka.setHealthCheckInitialDelay(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        kafka.setHealthCheckInitialDelay(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());

        Map<String, String> vars = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));

        kafka.setZookeeperConnect(vars.getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, ss.getMetadata().getName() + "-zookeeper:2181"));
        kafka.setDefaultReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR))));
        kafka.setOffsetsTopicReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR))));
        kafka.setTransactionStateLogReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR))));

        return kafka;
    }

    public ResourceDiffResult diff(StatefulSet ss)  {
        ResourceDiffResult diff = new ResourceDiffResult();

        if (replicas > ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            diff.setScaleUp(true);
        }
        else if (replicas < ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            diff.setScaleDown(true);
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

        Map<String, String> vars = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));

        if (!zookeeperConnect.equals(vars.getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, DEFAULT_KAFKA_ZOOKEEPER_CONNECT))
                || defaultReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR)))
                || offsetsTopicReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR)))
                || transactionStateLogReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR)))
                ) {
            log.info("Diff: Kafka options changed");
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        if (healthCheckInitialDelay != ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds()
                || healthCheckTimeout != ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds()) {
            log.info("Diff: Kafka healthcheck timing changed");
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        return diff;
    }

    public Service generateService() {
        Service svc = new ServiceBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(getLabelsWithName())
                .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                .withType("ClusterIP")
                .withSelector(getLabelsWithName())
                .withPorts(createServicePort(clientPortName, clientPort, clientPort))
                .endSpec()
                .build();

        return svc;
    }

    public Service patchService(Service svc) {
        svc.getMetadata().setLabels(getLabelsWithName());
        svc.getSpec().setSelector(getLabelsWithName());

        return svc;
    }

    public Service generateHeadlessService() {
        Service svc = new ServiceBuilder()
                .withNewMetadata()
                .withName(headlessName)
                .withLabels(getLabelsWithName(headlessName))
                .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                .withType("ClusterIP")
                .withClusterIP("None")
                .withSelector(getLabelsWithName())
                .withPorts(createServicePort(clientPortName, clientPort, clientPort))
                .endSpec()
                .build();

        return svc;
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
                .withPorts(createContainerPort(clientPortName, clientPort))
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
                .withSelector(new LabelSelectorBuilder().withMatchLabels(getLabelsWithName()).build())
                .withServiceName(headlessName)
                .withReplicas(replicas)
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
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_ZOOKEEPER_CONNECT).withValue(zookeeperConnect).build());
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR).withValue(String.valueOf(defaultReplicationFactor)).build());
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR).withValue(String.valueOf(offsetsTopicReplicationFactor)).build());
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR).withValue(String.valueOf(transactionStateLogReplicationFactor)).build());

        return varList;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public void setHealthCheckTimeout(int healthCheckTimeout) {
        this.healthCheckTimeout = healthCheckTimeout;
    }

    public void setHealthCheckInitialDelay(int healthCheckInitialDelay) {
        this.healthCheckInitialDelay = healthCheckInitialDelay;
    }

    public void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    public void setDefaultReplicationFactor(int defaultReplicationFactor) {
        this.defaultReplicationFactor = defaultReplicationFactor;
    }

    public void setOffsetsTopicReplicationFactor(int offsetsTopicReplicationFactor) {
        this.offsetsTopicReplicationFactor = offsetsTopicReplicationFactor;
    }

    public void setTransactionStateLogReplicationFactor(int transactionStateLogReplicationFactor) {
        this.transactionStateLogReplicationFactor = transactionStateLogReplicationFactor;
    }

    public int getReplicas() {
        return replicas;
    }

    public String getHeadlessName() {
        return headlessName;
    }
}