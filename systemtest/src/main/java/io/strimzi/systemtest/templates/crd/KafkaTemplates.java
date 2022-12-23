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
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;

import java.util.Collections;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

public class KafkaTemplates {

    private KafkaTemplates() {}

    public static KafkaBuilder kafkaEphemeral(String name, int kafkaReplicas) {
        return kafkaEphemeral(name, kafkaReplicas, Math.min(kafkaReplicas, 3));
    }

    public static KafkaBuilder kafkaEphemeral(String name, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_EPHEMERAL_CONFIG);
        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas);
    }

    public static KafkaBuilder kafkaPersistent(String name, int kafkaReplicas) {
        return kafkaPersistent(name, kafkaReplicas, Math.min(kafkaReplicas, 3));
    }

    public static KafkaBuilder kafkaPersistent(String name, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_PERSISTENT_CONFIG);
        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas)
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

    public static KafkaBuilder kafkaJBOD(String name, int kafkaReplicas, JbodStorage jbodStorage) {
        return kafkaJBOD(name, kafkaReplicas, 3, jbodStorage);
    }

    public static KafkaBuilder kafkaJBOD(String name, int kafkaReplicas, int zookeeperReplicas, JbodStorage jbodStorage) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_PERSISTENT_CONFIG);
        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .editKafka()
                    .withStorage(jbodStorage)
                .endKafka()
                .editZookeeper().
                    withReplicas(zookeeperReplicas)
                .endZookeeper()
            .endSpec();
    }

    public static KafkaBuilder kafkaWithMetrics(String name, String namespace, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_METRICS_CONFIG);

        ConfigMap metricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_METRICS_CONFIG, "kafka-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(namespace).resource(metricsCm).createOrReplace();
        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas)
            .editOrNewMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .editSpec()
                .withNewKafkaExporter()
                .endKafkaExporter()
            .endSpec();
    }

    public static KafkaBuilder kafkaWithCruiseControl(String name, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_CRUISE_CONTROL_CONFIG);
        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas);
    }

    public static KafkaBuilder kafkaWithMetricsAndCruiseControlWithMetrics(String namespaceName, String name, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_METRICS_CONFIG);
        ConfigMap kafkaMetricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_METRICS_CONFIG, "kafka-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(kafkaMetricsCm).createOrReplace();
        ConfigMap zkMetricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_METRICS_CONFIG, "kafka-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(zkMetricsCm).createOrReplace();

        ConfigMap ccCm = new ConfigMapBuilder()
                .withApiVersion("v1")
                .withNewMetadata()
                    .withName("cruise-control-metrics-test")
                    .withLabels(Collections.singletonMap("app", "strimzi"))
                .endMetadata()
                .withData(Collections.singletonMap("metrics-config.yml",
                        "lowercaseOutputName: true\n" +
                        "rules:\n" +
                        "- pattern: kafka.cruisecontrol<name=(.+)><>(\\w+)\n" +
                        "  name: kafka_cruisecontrol_$1_$2\n" +
                        "  type: GAUGE"))
                .build();
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(ccCm).createOrReplace();

        ConfigMapKeySelector cmks = new ConfigMapKeySelectorBuilder()
                .withName("cruise-control-metrics-test")
                .withKey("metrics-config.yml")
                .build();
        JmxPrometheusExporterMetrics jmxPrometheusExporterMetrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(cmks)
                .endValueFrom()
                .build();

        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .withNewKafkaExporter()
                .endKafkaExporter()
                .withNewCruiseControl()
                    .withMetricsConfig(jmxPrometheusExporterMetrics)
                .endCruiseControl()
            .endSpec();
    }

    private static KafkaBuilder defaultKafka(Kafka kafka, String name, int kafkaReplicas, int zookeeperReplicas) {
        KafkaBuilder kb = new KafkaBuilder(kafka)
            .withNewMetadata()
                .withName(name)
                .withNamespace(kubeClient().getNamespace())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withVersion(Environment.ST_KAFKA_VERSION)
                    .withReplicas(kafkaReplicas)
                    .addToConfig("log.message.format.version", TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).protocolVersion())
                    .addToConfig("inter.broker.protocol.version", TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).protocolVersion())
                    .addToConfig("offsets.topic.replication.factor", Math.min(kafkaReplicas, 3))
                    .addToConfig("transaction.state.log.min.isr", Math.min(kafkaReplicas, 2))
                    .addToConfig("transaction.state.log.replication.factor", Math.min(kafkaReplicas, 3))
                    .addToConfig("default.replication.factor", Math.min(kafkaReplicas, 3))
                    .addToConfig("min.insync.replicas", Math.min(Math.max(kafkaReplicas - 1, 1), 2))
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .build())
                    .withNewInlineLogging()
                        .addToLoggers("kafka.root.logger.level", "DEBUG")
                    .endInlineLogging()
                .endKafka()
                .editZookeeper()
                    .withReplicas(zookeeperReplicas)
                    .withNewInlineLogging()
                        .addToLoggers("zookeeper.root.logger", "DEBUG")
                    .endInlineLogging()
                .endZookeeper()
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

        if (!Environment.isSharedMemory()) {
            kb.editSpec()
                .editKafka()
                    // we use such values, because on environments where it is limited to 7Gi, we are unable to deploy
                    // Cluster Operator, two Kafka clusters and MirrorMaker/2. Such situation may result in an OOM problem.
                    // For Kafka using 784Mi is too much and on the other hand 256Mi is causing OOM problem at the start.
                    .withResources(new ResourceRequirementsBuilder()
                        .addToLimits("memory", new Quantity("512Mi"))
                        .addToRequests("memory", new Quantity("512Mi"))
                        .build())
                .endKafka()
                .editZookeeper()
                    // For ZooKeeper using 512Mi is too much and on the other hand 128Mi is causing OOM problem at the start.
                    .withResources(new ResourceRequirementsBuilder()
                        .addToLimits("memory", new Quantity("256Mi"))
                        .addToRequests("memory", new Quantity("256Mi"))
                        .build())
                .endZookeeper()
                .editEntityOperator()
                    .editUserOperator()
                        // For UserOperator using 512Mi is too much and on the other hand 128Mi is causing OOM problem at the start.
                        .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("256Mi"))
                            .addToRequests("memory", new Quantity("256Mi"))
                            .build())
                    .endUserOperator()
                    .editTopicOperator()
                        // For TopicOperator using 512Mi is too much and on the other hand 128Mi is causing OOM problem at the start.
                        .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("256Mi"))
                            .addToRequests("memory", new Quantity("256Mi"))
                            .build())
                    .endTopicOperator()
                .endEntityOperator()
                .endSpec();
        }

        return kb;
    }

    private static Kafka getKafkaFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Kafka.class);
    }
}
