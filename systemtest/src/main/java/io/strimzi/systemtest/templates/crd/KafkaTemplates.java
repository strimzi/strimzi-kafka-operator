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
import io.strimzi.api.kafka.model.common.template.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;

import java.util.Collections;

public class KafkaTemplates {

    private KafkaTemplates() {}

    private static final String KAFKA_METRICS_CONFIG_REF_KEY = "kafka-metrics-config.yml";
    private static final String METRICS_KAFKA_CONFIG_MAP_SUFFIX = "-kafka-metrics";
    private static final String METRICS_CC_CONFIG_MAP_SUFFIX = "-cc-metrics";

    // -------------------------------------------------------------------------------------------
    // Kafka with metrics
    // -------------------------------------------------------------------------------------------

    public static KafkaBuilder kafkaWithMetrics(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        String configMapName = kafkaClusterName + METRICS_KAFKA_CONFIG_MAP_SUFFIX;

        return kafka(namespaceName, kafkaClusterName, kafkaReplicas)
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
    }

    public static KafkaBuilder kafkaWithMetricsAndCruiseControlWithMetrics(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
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

        return kafkaWithMetrics(namespaceName, kafkaClusterName, kafkaReplicas)
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

    public static KafkaBuilder kafkaWithCruiseControl(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        return kafka(namespaceName, kafkaClusterName, kafkaReplicas)
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

    public static KafkaBuilder kafkaWithCruiseControlTunedForFastModelGeneration(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        return kafka(namespaceName, kafkaClusterName, kafkaReplicas)
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

    public static KafkaBuilder kafka(String namespaceName, String kafkaClusterName, int kafkaReplicas) {
        KafkaBuilder kb = new KafkaBuilder()
            .withNewMetadata()
                .withName(kafkaClusterName)
                .withNamespace(namespaceName)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withMetadataVersion(TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).metadataVersion())
                .endKafka()
            .endSpec();

        setDefaultSpecOfKafka(kb, kafkaReplicas);
        setDefaultLogging(kb);
        setMemoryRequestsAndLimitsIfNeeded(kb);

        return kb;
    }

    // -------------------------------------------------------------------------------------------
    // Application of defaults to the builders
    // -------------------------------------------------------------------------------------------

    private static void setDefaultLogging(KafkaBuilder kafkaBuilder) {
        kafkaBuilder
            .editSpec()
                .editKafka()
                    .withNewInlineLogging()
                        .addToLoggers("rootLogger.level", "DEBUG")
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
                    .editOrNewTemplate()
                        .editOrNewTopicOperatorContainer()
                            // Finalizers ensure orderly and controlled deletion of KafkaTopic resources.
                            // In this case we would delete them automatically via ResourceManager
                            // And in case that we forget to delete some KafkaTopic in the test, we will anyway remove
                            // whole Namespace with it -> otherwise the Namespace deletion would be blocked.
                            .addToEnv(new ContainerEnvVarBuilder()
                                .withName("STRIMZI_USE_FINALIZERS")
                                .withValue("false")
                                .build()
                            )
                        .endTopicOperatorContainer()
                    .endTemplate()
                .endEntityOperator()
            .endSpec();
    }

    private static void setMemoryRequestsAndLimitsIfNeeded(KafkaBuilder kafkaBuilder) {
        if (!Environment.isSharedMemory()) {
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
