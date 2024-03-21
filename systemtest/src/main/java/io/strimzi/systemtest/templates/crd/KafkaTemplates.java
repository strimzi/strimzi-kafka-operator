/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.test.TestUtils;

import java.util.Collections;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

public class KafkaTemplates {

    private KafkaTemplates() {}

    private static final String PATH_TO_KAFKA_EPHEMERAL_KRAFT_EXAMPLE = TestConstants.PATH_TO_PACKAGING_EXAMPLES + "/kafka/kraft/kafka-ephemeral.yaml";
    private static final String PATH_TO_KAFKA_PERSISTENT_KRAFT_EXAMPLE = TestConstants.PATH_TO_PACKAGING_EXAMPLES + "/kafka/kraft/kafka.yaml";
    private static final String PATH_TO_KAFKA_PERSISTENT_NODE_POOLS_EXAMPLE = TestConstants.PATH_TO_PACKAGING_EXAMPLES + "/kafka/kafka-with-node-pools.yaml";
    private static final String KAFKA_METRICS_CONFIG_REF_KEY = "kafka-metrics-config.yml";
    private static final String ZOOKEEPER_METRICS_CONFIG_REF_KEY = "zookeeper-metrics-config.yml";
    private static final String METRICS_KAFKA_CONFIG_MAP_SUFFIX = "-kafka-metrics";
    private static final String METRICS_CC_CONFIG_MAP_SUFFIX = "-cc-metrics";

    // -------------------------------------------------------------------------------------------
    // Kafka Ephemeral
    // -------------------------------------------------------------------------------------------

    public static KafkaBuilder kafkaEphemeral(String clusterName, int kafkaReplicas) {
        return kafkaEphemeral(clusterName, kafkaReplicas, Math.min(kafkaReplicas, 3));
    }

    public static KafkaBuilder kafkaEphemeral(String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        if (Environment.isKafkaNodePoolsEnabled()) {
            if (Environment.isKRaftModeEnabled()) {
                return kafkaEphemeralKRaft(clusterName, kafkaReplicas);
            }
            return kafkaEphemeralNodePools(clusterName, kafkaReplicas, zookeeperReplicas);
        } else {
            return kafkaEphemeralWithoutNodePools(clusterName, kafkaReplicas, zookeeperReplicas);
        }
    }

    public static KafkaBuilder kafkaEphemeralWithoutNodePools(String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(TestConstants.PATH_TO_KAFKA_EPHEMERAL_CONFIG, false);
        return defaultKafkaWithoutNodePools(kafka, clusterName, kafkaReplicas, zookeeperReplicas);
    }

    public static KafkaBuilder kafkaEphemeralNodePools(String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(PATH_TO_KAFKA_PERSISTENT_NODE_POOLS_EXAMPLE, true);

        kafka.getSpec().getZookeeper().setStorage(null);

        return defaultKafkaNodePools(kafka, clusterName, kafkaReplicas, zookeeperReplicas)
            .editSpec()
            .editZookeeper()
                // the NodePools (in ZK mode) example contains persistent storage for ZK, so we need to specify the
                // ephemeral storage
                .withNewEphemeralStorage()
                .endEphemeralStorage()
            .endZookeeper()
            .endSpec();
    }

    public static KafkaBuilder kafkaEphemeralKRaft(String clusterName, int kafkaReplicas) {
        Kafka kafka = getKafkaFromYaml(PATH_TO_KAFKA_EPHEMERAL_KRAFT_EXAMPLE, true);
        return defaultKafkaKRaft(kafka, clusterName, kafkaReplicas);
    }

    // -------------------------------------------------------------------------------------------
    // Kafka Persistent
    // -------------------------------------------------------------------------------------------

    public static KafkaBuilder kafkaPersistent(String clusterName, int kafkaReplicas) {
        return kafkaPersistent(clusterName, kafkaReplicas, Math.min(kafkaReplicas, 3));
    }

    public static KafkaBuilder kafkaPersistent(String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        if (Environment.isKafkaNodePoolsEnabled()) {
            if (Environment.isKRaftModeEnabled()) {
                return kafkaPersistentKRaft(clusterName, kafkaReplicas);
            }
            return kafkaPersistentNodePools(clusterName, kafkaReplicas, zookeeperReplicas);
        } else {
            return kafkaPersistentWithoutNodePools(clusterName, kafkaReplicas, zookeeperReplicas);
        }
    }

    public static KafkaBuilder kafkaPersistentWithoutNodePools(String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(TestConstants.PATH_TO_KAFKA_EPHEMERAL_CONFIG, false);

        return defaultKafkaWithoutNodePools(kafka, clusterName, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withDeleteClaim(true)
                    .endPersistentClaimStorage()
                .endKafka()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withDeleteClaim(true)
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec();
    }

    public static KafkaBuilder kafkaPersistentNodePools(String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(PATH_TO_KAFKA_PERSISTENT_NODE_POOLS_EXAMPLE, true);
        kafka.getSpec().getZookeeper().setStorage(null);

        return defaultKafkaNodePools(kafka, clusterName, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withDeleteClaim(true)
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec();
    }

    public static KafkaBuilder kafkaPersistentKRaft(String clusterName, int kafkaReplicas) {
        Kafka kafka = getKafkaFromYaml(PATH_TO_KAFKA_PERSISTENT_KRAFT_EXAMPLE, true);
        return defaultKafkaKRaft(kafka, clusterName, kafkaReplicas);
    }

    // -------------------------------------------------------------------------------------------
    // Kafka JBOD
    // -------------------------------------------------------------------------------------------

    public static KafkaBuilder kafkaJBOD(String clusterName, int kafkaReplicas, int zookeeperReplicas, JbodStorage jbodStorage) {
        if (Environment.isKafkaNodePoolsEnabled()) {
            return kafkaPersistentNodePools(clusterName, kafkaReplicas, zookeeperReplicas);
        } else {
            return kafkaPersistentWithoutNodePools(clusterName, kafkaReplicas, kafkaReplicas)
                .editSpec()
                    .editKafka()
                        .withStorage(jbodStorage)
                    .endKafka()
                .endSpec();
        }
    }

    // -------------------------------------------------------------------------------------------
    // Kafka with metrics
    // -------------------------------------------------------------------------------------------

    public static KafkaBuilder kafkaWithMetrics(String namespaceName, String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(TestConstants.PATH_TO_KAFKA_METRICS_CONFIG, false);
        String configMapName = clusterName + METRICS_KAFKA_CONFIG_MAP_SUFFIX;

        KafkaBuilder kafkaBuilder = defaultKafka(kafka, clusterName, kafkaReplicas, zookeeperReplicas)
            .editMetadata()
                .withNamespace(namespaceName)
            .endMetadata()
            .editSpec()
                .withNewKafkaExporter()
                .endKafkaExporter()
                .editKafka()
                    .withNewJmxPrometheusExporterMetricsConfig()
                        .withNewValueFrom()
                            .withNewConfigMapKeyRef(KAFKA_METRICS_CONFIG_REF_KEY, configMapName, false)
                        .endValueFrom()
                    .endJmxPrometheusExporterMetricsConfig()
                .endKafka()
            .endSpec();

        if (!Environment.isKRaftModeEnabled()) {
            kafkaBuilder
                .editSpec()
                    .editZookeeper()
                        .withNewJmxPrometheusExporterMetricsConfig()
                            .withNewValueFrom()
                                .withNewConfigMapKeyRef(ZOOKEEPER_METRICS_CONFIG_REF_KEY, configMapName, false)
                            .endValueFrom()
                        .endJmxPrometheusExporterMetricsConfig()
                    .endZookeeper()
                .endSpec();
        }

        return kafkaBuilder;
    }

    public static KafkaBuilder kafkaWithMetricsAndCruiseControlWithMetrics(String namespaceName, String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        String ccConfigMapName = clusterName + METRICS_CC_CONFIG_MAP_SUFFIX;

        ConfigMapKeySelector cmks = new ConfigMapKeySelectorBuilder()
            .withName(ccConfigMapName)
            .withKey("metrics-config.yml")
            .build();

        JmxPrometheusExporterMetrics jmxPrometheusExporterMetrics = new JmxPrometheusExporterMetricsBuilder()
            .withNewValueFrom()
                .withConfigMapKeyRef(cmks)
            .endValueFrom()
            .build();

        return kafkaWithMetrics(namespaceName, clusterName, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .withNewCruiseControl()
                    .withMetricsConfig(jmxPrometheusExporterMetrics)
                    // Extend active users tasks
                    .addToConfig("max.active.user.tasks", 10)
                .endCruiseControl()
            .endSpec();
    }

    // -------------------------------------------------------------------------------------------
    // ConfigMaps for Kafka with metrics
    // -------------------------------------------------------------------------------------------

    public static ConfigMap kafkaMetricsConfigMap(String namespaceName, String clusterName) {
        String configMapName = clusterName + METRICS_KAFKA_CONFIG_MAP_SUFFIX;

        ConfigMap kafkaMetricsCm = TestUtils.configMapFromYaml(TestConstants.PATH_TO_KAFKA_METRICS_CONFIG, "kafka-metrics");

        return new ConfigMapBuilder(kafkaMetricsCm)
            .editMetadata()
                .withName(configMapName)
                .withNamespace(namespaceName)
            .endMetadata()
            .build();
    }

    public static ConfigMap cruiseControlMetricsConfigMap(String namespaceName, String clusterName) {
        String configMapName = clusterName + METRICS_CC_CONFIG_MAP_SUFFIX;

        return new ConfigMapBuilder()
            .withNewMetadata()
                .withName(configMapName)
                .withLabels(Collections.singletonMap("app", "strimzi"))
                .withNamespace(namespaceName)
            .endMetadata()
            .withData(
                Collections.singletonMap("metrics-config.yml",
                "lowercaseOutputName: true\n" +
                    "rules:\n" +
                    "- pattern: kafka.cruisecontrol<name=(.+)><>(\\w+)\n" +
                    "  name: kafka_cruisecontrol_$1_$2\n" +
                    "  type: GAUGE"))
            .build();
    }

    // -------------------------------------------------------------------------------------------
    // Kafka with Cruise Control
    // -------------------------------------------------------------------------------------------

    public static KafkaBuilder kafkaWithCruiseControl(String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(TestConstants.PATH_TO_KAFKA_CRUISE_CONTROL_CONFIG, false);

        return defaultKafka(kafka, clusterName, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .editCruiseControl()
                    // Extend active users tasks
                    .addToConfig("max.active.user.tasks", 10)
                .endCruiseControl()
            .endSpec();
    }

    // -------------------------------------------------------------------------------------------
    // Kafka default templates
    // -------------------------------------------------------------------------------------------

    private static KafkaBuilder defaultKafka(Kafka kafka, String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        if (Environment.isKafkaNodePoolsEnabled()) {
            if (Environment.isKRaftModeEnabled()) {
                return defaultKafkaKRaft(kafka, clusterName, kafkaReplicas);
            }
            return defaultKafkaNodePools(kafka, clusterName, kafkaReplicas, zookeeperReplicas);
        } else {
            return defaultKafkaWithoutNodePools(kafka, clusterName, kafkaReplicas, zookeeperReplicas);
        }
    }

    private static KafkaBuilder defaultKafkaWithoutNodePools(Kafka kafka, String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        KafkaBuilder kb = new KafkaBuilder(kafka)
            .withNewMetadata()
                .withName(clusterName)
                .withNamespace(kubeClient().getNamespace())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withReplicas(kafkaReplicas)
                .endKafka()
            .endSpec();

        setDefaultSpecOfKafka(kb, kafkaReplicas);
        setDefaultConfigurationOfZookeeperKafka(kb, zookeeperReplicas);
        setDefaultLogging(kb, true);
        setMemoryRequestsAndLimitsIfNeeded(kb, true);

        return kb;
    }

    private static KafkaBuilder defaultKafkaNodePools(Kafka kafka, String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        KafkaBuilder kb = new KafkaBuilder(kafka)
            .withNewMetadata()
                .withName(clusterName)
                .withNamespace(kubeClient().getNamespace())
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
            .endMetadata();

        setDefaultSpecOfKafka(kb, kafkaReplicas);
        setDefaultConfigurationOfZookeeperKafka(kb, zookeeperReplicas);
        setDefaultLogging(kb, true);
        setMemoryRequestsAndLimitsIfNeeded(kb, true);
        kb = removeFieldsNotRelatedToParticularMode(kb, true);

        return kb;
    }

    private static KafkaBuilder defaultKafkaKRaft(Kafka kafka, String clusterName, int kafkaReplicas) {
        KafkaBuilder kb = new KafkaBuilder(kafka)
            .withNewMetadata()
                .withName(clusterName)
                .withNamespace(kubeClient().getNamespace())
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withMetadataVersion(TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).metadataVersion())
                .endKafka()
            .endSpec();

        setDefaultSpecOfKafka(kb, kafkaReplicas);
        setDefaultLogging(kb, false);
        setMemoryRequestsAndLimitsIfNeeded(kb, false);
        kb = removeFieldsNotRelatedToParticularMode(kb, false);

        return kb;
    }

    // -------------------------------------------------------------------------------------------
    // Application of defaults to the builders
    // -------------------------------------------------------------------------------------------

    private static void setDefaultConfigurationOfZookeeperKafka(KafkaBuilder kafkaBuilder, int zookeeperReplicas) {
        kafkaBuilder
            .editSpec()
                .editKafka()
                    .addToConfig("log.message.format.version", TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).messageVersion())
                    .addToConfig("inter.broker.protocol.version", TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).protocolVersion())
                .endKafka()
                .editZookeeper()
                    .withReplicas(zookeeperReplicas)
                .endZookeeper()
            .endSpec();
    }

    private static void setDefaultLogging(KafkaBuilder kafkaBuilder, boolean withZookeeper) {
        kafkaBuilder
            .editSpec()
                .editKafka()
                    .withNewInlineLogging()
                        .addToLoggers("kafka.root.logger.level", "DEBUG")
                    .endInlineLogging()
                .endKafka()
                .editEntityOperator()
                    .editUserOperator()
                        .withNewInlineLogging()
                            .addToLoggers("rootLogger.level", "DEBUG")
                        .endInlineLogging()
                    .endUserOperator()
                    .editTopicOperator()
                        .withNewInlineLogging()
                            .addToLoggers("rootLogger.level", "DEBUG")
                        .endInlineLogging()
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec();

        if (withZookeeper) {
            kafkaBuilder
                .editSpec()
                    .editZookeeper()
                        .withNewInlineLogging()
                            .addToLoggers("zookeeper.root.logger", "DEBUG")
                        .endInlineLogging()
                    .endZookeeper()
                .endSpec();
        }
    }

    private static void setMemoryRequestsAndLimitsIfNeeded(KafkaBuilder kafkaBuilder, boolean withZookeeper) {
        if (!Environment.isSharedMemory()) {
            // in case that we are using NodePools, the resource limits and requirements are specified in KafkaNodePool resources
            if (!Environment.isKafkaNodePoolsEnabled()) {
                kafkaBuilder.editSpec()
                        .editKafka()
                            // we use such values, because on environments where it is limited to 7Gi, we are unable to deploy
                            // Cluster Operator, two Kafka clusters and MirrorMaker/2. Such situation may result in an OOM problem.
                            // For Kafka using 784Mi is too much and on the other hand 256Mi is causing OOM problem at the start.
                            .withResources(new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("512Mi"))
                                .addToRequests("memory", new Quantity("512Mi"))
                                .build())
                        .endKafka()
                    .endSpec();
            }

            kafkaBuilder.editSpec()
                .editEntityOperator()
                    .editUserOperator()
                        // For User Operator using 512Mi is too much and on the other hand 128Mi is causing OOM problem at the start.
                        .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("256Mi"))
                            .addToRequests("memory", new Quantity("256Mi"))
                            .build())
                    .endUserOperator()
                    .editTopicOperator()
                        // For Topic Operator using 512Mi is too much and on the other hand 128Mi is causing OOM problem at the start.
                        .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("256Mi"))
                            .addToRequests("memory", new Quantity("256Mi"))
                            .build())
                    .endTopicOperator()
                .endEntityOperator()
                .endSpec();

            if (withZookeeper) {
                kafkaBuilder
                    .editSpec()
                        .editZookeeper()
                            // For ZooKeeper using 512Mi is too much and on the other hand 128Mi is causing OOM problem at the start.
                            .withResources(new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("256Mi"))
                                .addToRequests("memory", new Quantity("256Mi"))
                                .build())
                        .endZookeeper()
                    .endSpec();
            }
        }
    }

    private static void setDefaultSpecOfKafka(KafkaBuilder kafkaBuilder, int kafkaReplicas) {
        kafkaBuilder
            .editSpec()
                .editKafka()
                    .withListeners(
                        new GenericKafkaListenerBuilder()
                            .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .build())
                    .withVersion(Environment.ST_KAFKA_VERSION)
                    .addToConfig("offsets.topic.replication.factor", Math.min(kafkaReplicas, 3))
                    .addToConfig("transaction.state.log.min.isr", Math.min(kafkaReplicas, 2))
                    .addToConfig("transaction.state.log.replication.factor", Math.min(kafkaReplicas, 3))
                    .addToConfig("default.replication.factor", Math.min(kafkaReplicas, 3))
                    .addToConfig("min.insync.replicas", Math.min(Math.max(kafkaReplicas - 1, 1), 2))
                .endKafka()
            .endSpec();
    }

    private static KafkaBuilder removeFieldsNotRelatedToParticularMode(KafkaBuilder kafkaBuilder, boolean withZookeeper) {
        Kafka kafka = kafkaBuilder.build();

        // in case that we are using file that is not customized to usage of NodePools or KRaft, we need to remove all the
        // fields here
        if (!withZookeeper) {
            kafka.getSpec().setZookeeper(null);
            kafka.getSpec().getKafka().getConfig().remove("log.message.format.version");
            kafka.getSpec().getKafka().getConfig().remove("inter.broker.protocol.version");
        }

        kafka.getSpec().getKafka().setStorage(null);
        kafka.getSpec().getKafka().setReplicas(null);

        return new KafkaBuilder(kafka);
    }

    private static Kafka getKafkaFromYaml(String yamlPath, boolean containsNodePools) {
        return containsNodePools ?
            TestUtils.configFromMultiYamlFile(yamlPath, Kafka.RESOURCE_KIND, Kafka.class) : TestUtils.configFromYaml(yamlPath, Kafka.class);
    }
}
