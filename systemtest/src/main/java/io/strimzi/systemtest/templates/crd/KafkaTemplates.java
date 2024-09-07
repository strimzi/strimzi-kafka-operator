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
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;

import java.util.Collections;

public class KafkaTemplates {

    private KafkaTemplates() {}

    private static final String KAFKA_METRICS_CONFIG_REF_KEY = "kafka-metrics-config.yml";
    private static final String ZOOKEEPER_METRICS_CONFIG_REF_KEY = "zookeeper-metrics-config.yml";
    private static final String METRICS_KAFKA_CONFIG_MAP_SUFFIX = "-kafka-metrics";
    private static final String METRICS_CC_CONFIG_MAP_SUFFIX = "-cc-metrics";

    // -------------------------------------------------------------------------------------------
    // Kafka Ephemeral
    // -------------------------------------------------------------------------------------------

    public static KafkaBuilder kafkaEphemeral(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        return kafkaEphemeral(namespaceName, kafkaClusterName, kafkaReplicas, Math.min(kafkaReplicas, 3));
    }

    public static KafkaBuilder kafkaEphemeral(String namespaceName, String kafkaClusterName, int kafkaReplicas, int zookeeperReplicas) {
        if (Environment.isKafkaNodePoolsEnabled()) {
            if (Environment.isKRaftModeEnabled()) {
                return kafkaEphemeralKRaft(namespaceName, kafkaClusterName, kafkaReplicas);
            }
            return kafkaEphemeralNodePools(namespaceName, kafkaClusterName, kafkaReplicas, zookeeperReplicas);
        } else {
            return kafkaEphemeralWithoutNodePools(namespaceName, kafkaClusterName, kafkaReplicas, zookeeperReplicas);
        }
    }

    public static KafkaBuilder kafkaEphemeralWithoutNodePools(String namespaceName, String kafkaClusterName, int kafkaReplicas, int zookeeperReplicas) {
        return defaultKafkaWithoutNodePools(namespaceName, kafkaClusterName, kafkaReplicas, zookeeperReplicas);
    }

    public static KafkaBuilder kafkaEphemeralNodePools(String namespaceName, String kafkaClusterName, int kafkaReplicas, int zookeeperReplicas) {
        return defaultKafkaNodePools(namespaceName, kafkaClusterName, kafkaReplicas, zookeeperReplicas);
    }

    public static KafkaBuilder kafkaEphemeralKRaft(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        return defaultKafkaKRaft(namespaceName, kafkaClusterName, kafkaReplicas);
    }

    // -------------------------------------------------------------------------------------------
    // Kafka Persistent
    // -------------------------------------------------------------------------------------------

    public static KafkaBuilder kafkaPersistent(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        return kafkaPersistent(namespaceName, kafkaClusterName, kafkaReplicas, Math.min(kafkaReplicas, 3));
    }

    public static KafkaBuilder kafkaPersistent(String namespaceName, String kafkaClusterName, int kafkaReplicas, int zookeeperReplicas) {
        if (Environment.isKafkaNodePoolsEnabled()) {
            if (Environment.isKRaftModeEnabled()) {
                return kafkaPersistentKRaft(namespaceName, kafkaClusterName, kafkaReplicas);
            }
            return kafkaPersistentNodePools(namespaceName, kafkaClusterName, kafkaReplicas, zookeeperReplicas);
        } else {
            return kafkaPersistentWithoutNodePools(namespaceName, kafkaClusterName, kafkaReplicas, zookeeperReplicas);
        }
    }

    public static KafkaBuilder kafkaPersistentWithoutNodePools(String namespaceName, String kafkaClusterName, int kafkaReplicas, int zookeeperReplicas) {
        return defaultKafkaWithoutNodePools(namespaceName, kafkaClusterName, kafkaReplicas, zookeeperReplicas)
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

    public static KafkaBuilder kafkaPersistentNodePools(String namespaceName, String kafkaClusterName, int kafkaReplicas, int zookeeperReplicas) {
        return defaultKafkaNodePools(namespaceName, kafkaClusterName, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                        .withDeleteClaim(true)
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec();
    }

    public static KafkaBuilder kafkaPersistentKRaft(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        return defaultKafkaKRaft(namespaceName, kafkaClusterName, kafkaReplicas);
    }

    // -------------------------------------------------------------------------------------------
    // Kafka with metrics
    // -------------------------------------------------------------------------------------------

    public static KafkaBuilder kafkaWithMetrics(String namespaceName, String kafkaClusterName, int kafkaReplicas, int zookeeperReplicas) {
        String configMapName = kafkaClusterName + METRICS_KAFKA_CONFIG_MAP_SUFFIX;

        KafkaBuilder kafkaBuilder = defaultKafka(namespaceName, kafkaClusterName, kafkaReplicas, zookeeperReplicas)
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

    public static KafkaBuilder kafkaWithMetricsAndCruiseControlWithMetrics(String namespaceName, String kafkaClusterName, int kafkaReplicas, int zookeeperReplicas) {
        String ccConfigMapName = kafkaClusterName + METRICS_CC_CONFIG_MAP_SUFFIX;

        ConfigMapKeySelector cmks = new ConfigMapKeySelectorBuilder()
            .withName(ccConfigMapName)
            .withKey("metrics-config.yml")
            .build();

        JmxPrometheusExporterMetrics jmxPrometheusExporterMetrics = new JmxPrometheusExporterMetricsBuilder()
            .withNewValueFrom()
                .withConfigMapKeyRef(cmks)
            .endValueFrom()
            .build();

        return kafkaWithMetrics(namespaceName, kafkaClusterName, kafkaReplicas, zookeeperReplicas)
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

    public static ConfigMap kafkaMetricsConfigMap(String namespaceName, String kafkaClusterName) {
        String configMapName = kafkaClusterName + METRICS_KAFKA_CONFIG_MAP_SUFFIX;

        return new ConfigMapBuilder(FileUtils.extractConfigMapFromYAMLWithResources(TestConstants.PATH_TO_KAFKA_METRICS_CONFIG, "kafka-metrics"))
            .editMetadata()
                .withName(configMapName)
                .withNamespace(namespaceName)
            .endMetadata()
            .build();
    }

    public static ConfigMap cruiseControlMetricsConfigMap(String namespaceName, String kafkaClusterName) {
        String configMapName = kafkaClusterName + METRICS_CC_CONFIG_MAP_SUFFIX;

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

    public static KafkaBuilder kafkaWithCruiseControl(String namespaceName, String kafkaClusterName, int kafkaReplicas, int zookeeperReplicas) {
        return defaultKafka(namespaceName, kafkaClusterName, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .editKafka()
                    .addToConfig("cruise.control.metrics.reporter.metrics.reporting.interval.ms", 5_000)
                    .addToConfig("cruise.control.metrics.reporter.metadata.max.age.ms", 4_000)
                .endKafka()
                .editOrNewCruiseControl()
                    // the following configurations are set for better reliability and stability of CC related tests
                    .addToConfig("max.active.user.tasks", 10)
                    .addToConfig("metric.sampling.interval.ms", 5_000)
                    .addToConfig("cruise.control.metrics.reporter.metrics.reporting.interval.ms", 5_000)
                    .addToConfig("metadata.max.age.ms", 4_000)
                .endCruiseControl()
            .endSpec();
    }

    public static KafkaBuilder kafkaWithCruiseControlTunedForFastModelGeneration(String namespaceName, String kafkaClusterName, int kafkaReplicas, int zookeeperReplicas) {
        return defaultKafka(namespaceName, kafkaClusterName, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .editKafka()
                    .addToConfig("cruise.control.metrics.reporter.metrics.reporting.interval.ms", 5_000)
                    .addToConfig("cruise.control.metrics.reporter.metadata.max.age.ms", 4_000)
                    .addToConfig("cruise.control.metrics.topic.replication.factor", 1)
                    .addToConfig("cruise.control.metrics.topic.min.insync.replicas", 1)
                .endKafka()
                .editOrNewCruiseControl()
                    .addToConfig("max.active.user.tasks", 10)
                    .addToConfig("metric.sampling.interval.ms", 5_000)
                    .addToConfig("cruise.control.metrics.reporter.metrics.reporting.interval.ms", 5_000)
                    .addToConfig("metadata.max.age.ms", 4_000)
                    .addToConfig("sample.store.topic.replication.factor", 1)
                    .addToConfig("partition.sample.store.topic.partition.count", 1)
                    .addToConfig("broker.sample.store.topic.partition.count", 1)
                    .addToConfig("skip.sample.store.topic.rack.awareness.check", true)
                    .addToConfig("partition.metrics.window.ms", 10_000)
                    .addToConfig("broker.metrics.window.ms", 10_000)
                    .addToConfig("monitor.state.update.interval.ms", 10_000)
                .endCruiseControl()
            .endSpec();
    }

    // -------------------------------------------------------------------------------------------
    // Kafka default templates
    // -------------------------------------------------------------------------------------------

    private static KafkaBuilder defaultKafka(String namespaceName, String kafkaClusterName, int kafkaReplicas, int zookeeperReplicas) {
        if (Environment.isKafkaNodePoolsEnabled()) {
            if (Environment.isKRaftModeEnabled()) {
                return defaultKafkaKRaft(namespaceName, kafkaClusterName, kafkaReplicas);
            }
            return defaultKafkaNodePools(namespaceName, kafkaClusterName, kafkaReplicas, zookeeperReplicas);
        } else {
            return defaultKafkaWithoutNodePools(namespaceName, kafkaClusterName, kafkaReplicas, zookeeperReplicas);
        }
    }

    private static KafkaBuilder defaultKafkaWithoutNodePools(String namespaceName, String kafkaClusterName, int kafkaReplicas, int zookeeperReplicas) {
        KafkaBuilder kb = new KafkaBuilder()
            .withNewMetadata()
                .withName(kafkaClusterName)
                .withNamespace(namespaceName)
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
        setKafkaEphemeralStorage(kb);
        setZookeeperEphemeralStorage(kb);

        return kb;
    }

    private static KafkaBuilder defaultKafkaNodePools(String namespaceName, String kafkaClusterName, int kafkaReplicas, int zookeeperReplicas) {
        KafkaBuilder kb = new KafkaBuilder()
            .withNewMetadata()
                .withName(kafkaClusterName)
                .withNamespace(namespaceName)
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
            .endMetadata();

        setDefaultSpecOfKafka(kb, kafkaReplicas);
        setDefaultConfigurationOfZookeeperKafka(kb, zookeeperReplicas);
        setDefaultLogging(kb, true);
        setMemoryRequestsAndLimitsIfNeeded(kb, true);
        setZookeeperEphemeralStorage(kb);

        return kb;
    }

    private static KafkaBuilder defaultKafkaKRaft(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        KafkaBuilder kb = new KafkaBuilder()
            .withNewMetadata()
                .withName(kafkaClusterName)
                .withNamespace(namespaceName)
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

        return kb;
    }

    // -------------------------------------------------------------------------------------------
    // Application of defaults to the builders
    // -------------------------------------------------------------------------------------------

    private static void setKafkaEphemeralStorage(KafkaBuilder kafkaBuilder) {
        kafkaBuilder
            .editSpec()
                .editOrNewKafka()
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                .endKafka()
            .endSpec();
    }

    private static void setZookeeperEphemeralStorage(KafkaBuilder kafkaBuilder) {
        kafkaBuilder
            .editSpec()
                .editOrNewZookeeper()
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                .endZookeeper()
            .endSpec();
    }

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
}
