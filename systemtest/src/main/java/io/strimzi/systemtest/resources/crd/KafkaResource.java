/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

public class KafkaResource {
    private static final String PATH_TO_KAFKA_CRUISE_CONTROL_CONFIG = TestUtils.USER_PATH + "/../examples/cruise-control/kafka-cruise-control.yaml";
    private static final String PATH_TO_KAFKA_EPHEMERAL_CONFIG = TestUtils.USER_PATH + "/../examples/kafka/kafka-ephemeral.yaml";
    private static final String PATH_TO_KAFKA_PERSISTENT_CONFIG = TestUtils.USER_PATH + "/../examples/kafka/kafka-persistent.yaml";

    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaClient() {
        return Crds.kafkaV1Beta2Operation(ResourceManager.kubeClient().getClient());
    }

    public static KafkaBuilder kafkaEphemeral(String clusterName, int kafkaReplicas) {
        return kafkaEphemeral(clusterName, kafkaReplicas, Math.min(kafkaReplicas, 3));
    }

    public static KafkaBuilder kafkaEphemeral(String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        return defaultKafka(getKafkaFromYaml(PATH_TO_KAFKA_EPHEMERAL_CONFIG), clusterName, kafkaReplicas, zookeeperReplicas);
    }

    public static KafkaBuilder kafkaPersistent(String name, int kafkaReplicas) {
        return kafkaPersistent(name, kafkaReplicas, Math.min(kafkaReplicas, 3));
    }

    public static KafkaBuilder kafkaPersistent(String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        return defaultKafka(getKafkaFromYaml(PATH_TO_KAFKA_PERSISTENT_CONFIG), clusterName, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withNewSize("100")
                        .withDeleteClaim(true)
                    .endPersistentClaimStorage()
                .endKafka()
                .editZookeeper()
                    .withNewPersistentClaimStorage()
                        .withNewSize("100")
                        .withDeleteClaim(true)
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec();
    }

    public static KafkaBuilder kafkaJBOD(String name, int kafkaReplicas, JbodStorage jbodStorage) {
        return kafkaJBOD(name, kafkaReplicas, Math.min(kafkaReplicas, 3), jbodStorage);
    }

    public static KafkaBuilder kafkaJBOD(String name, int kafkaReplicas, int zookeeperReplicas, JbodStorage jbodStorage) {
        return kafkaPersistent(name, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .editKafka()
                    .withStorage(jbodStorage)
                .endKafka()
            .endSpec();
    }

    public static KafkaBuilder kafkaWithMetrics(String name, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_METRICS_CONFIG);

        deployMetricsConfigMaps();

        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .withNewKafkaExporter()
                .endKafkaExporter()
            .endSpec();
    }

    public static KafkaBuilder kafkaWithCruiseControl(String name, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(PATH_TO_KAFKA_CRUISE_CONTROL_CONFIG);
        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas);
    }

    public static KafkaBuilder kafkaAndCruiseControlWithMetrics(String name, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(Constants.PATH_TO_KAFKA_CRUISE_CONTROL_METRICS_CONFIG);

        deployMetricsConfigMaps();

        ConfigMap ccMetricsCm = TestUtils.configMapFromYaml(PATH_TO_KAFKA_CRUISE_CONTROL_CONFIG, "cruise-control-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(kubeClient().getNamespace()).createOrReplace(ccMetricsCm);

        return defaultKafka(kafka, name, kafkaReplicas, zookeeperReplicas);
    }

    public static KafkaBuilder kafkaWithMetricsAndCruiseControlWithMetrics(String name, int kafkaReplicas, int zookeeperReplicas) {
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

        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(kubeClient().getNamespace()).createOrReplace(ccCm);

        ConfigMapKeySelector cmks = new ConfigMapKeySelectorBuilder()
                .withName("cruise-control-metrics-test")
                .withKey("metrics-config.yml")
                .build();

        JmxPrometheusExporterMetrics jmxPrometheusExporterMetrics = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(cmks)
                .endValueFrom()
                .build();

        return kafkaWithMetrics(name, kafkaReplicas, zookeeperReplicas)
            .editSpec()
                .withNewCruiseControl()
                    .withMetricsConfig(jmxPrometheusExporterMetrics)
                .endCruiseControl()
            .endSpec();
    }

    private static void deployMetricsConfigMaps() {
        ConfigMap metricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_METRICS_CONFIG, "kafka-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(kubeClient().getNamespace()).createOrReplace(metricsCm);
    }

    private static KafkaBuilder defaultKafka(Kafka kafka, String name, int kafkaReplicas, int zookeeperReplicas) {
        return new KafkaBuilder(kafka)
            .withNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
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
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                        .endGenericKafkaListener()
                    .endListeners()
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
    }

    public static KafkaBuilder kafkaFromYaml(File yamlFile, String clusterName, int kafkaReplicas, int zookeeperReplicas) {
        Kafka kafka = getKafkaFromYaml(yamlFile);
        return new KafkaBuilder(kafka)
            .withNewMetadata()
                .withName(clusterName)
                .withNamespace(kubeClient().getNamespace())
            .endMetadata()
            .editOrNewSpec()
                .editKafka()
                    .withReplicas(kafkaReplicas)
                .endKafka()
                .editZookeeper()
                    .withReplicas(zookeeperReplicas)
                .endZookeeper()
            .endSpec();
    }

    public static Kafka createAndWaitForReadiness(Kafka kafka) {
        TestUtils.waitFor("Kafka creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, CR_CREATION_TIMEOUT,
            () -> {
                try {
                    kafkaClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kafka);
                    return true;
                } catch (KubernetesClientException e) {
                    if (e.getMessage().contains("object is being deleted")) {
                        return false;
                    } else {
                        throw e;
                    }
                }
            });
        return waitFor(deleteLater(kafka));
    }

    /**
     * This method is used for deploy specific Kafka cluster without wait for all resources.
     * It can be use for example for deploy Kafka cluster with unsupported Kafka version.
     * @param kafka kafka cluster specification
     * @return kafka cluster specification
     */
    public static Kafka kafkaWithoutWait(Kafka kafka) {
        kafkaClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kafka);
        return kafka;
    }

    public static Kafka kafkaWithCruiseControlWithoutWait(String name, int kafkaReplicas, int zookeeperReplicas) {
        return kafkaClient().inNamespace(ResourceManager.kubeClient().getNamespace())
            .createOrReplace(kafkaWithCruiseControl(name, kafkaReplicas, zookeeperReplicas).build());
    }

    /**
     * This method is used for delete specific Kafka cluster without wait for all resources deletion.
     * It can be use for example for delete Kafka cluster CR with unsupported Kafka version.
     * @param resourceName kafka cluster name
     */
    public static void deleteKafkaWithoutWait(String resourceName) {
        kafkaClient().inNamespace(ResourceManager.kubeClient().getNamespace()).withName(resourceName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    private static Kafka getKafkaFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Kafka.class);
    }

    private static Kafka getKafkaFromYaml(File yamlFile) {
        return TestUtils.configFromYaml(yamlFile, Kafka.class);
    }

    /**
     * Wait until the ZK, Kafka and EO are all ready
     */
    private static Kafka waitFor(Kafka kafka) {
        long timeout = ResourceOperation.getTimeoutForResourceReadiness(kafka.getKind());

        // Kafka Exporter is not setup every time
        if (kafka.getSpec().getKafkaExporter() != null) {
            timeout += ResourceOperation.getTimeoutForResourceReadiness(Constants.KAFKA_EXPORTER_DEPLOYMENT);
        }
        // Cruise Control is not setup every time
        if (kafka.getSpec().getCruiseControl() != null) {
            timeout += ResourceOperation.getTimeoutForResourceReadiness(Constants.KAFKA_CRUISE_CONTROL_DEPLOYMENT);
        }
        return ResourceManager.waitForResourceStatus(kafkaClient(), kafka, Ready, timeout);
    }

    private static Kafka deleteLater(Kafka kafka) {
        return ResourceManager.deleteLater(kafkaClient(), kafka);
    }

    public static void replaceKafkaResource(String resourceName, Consumer<Kafka> editor) {
        ResourceManager.replaceCrdResource(Kafka.class, KafkaList.class, resourceName, editor);
    }

    public static String getKafkaTlsListenerCaCertName(String namespace, String clusterName, String listenerName) {
        List<GenericKafkaListener> listeners = kafkaClient().inNamespace(namespace).withName(clusterName).get().getSpec().getKafka().getListeners().newOrConverted();

        GenericKafkaListener tlsListener = listenerName == null || listenerName.isEmpty() ?
            listeners.stream().filter(listener -> Constants.TLS_LISTENER_DEFAULT_NAME.equals(listener.getName())).findFirst().orElseThrow(RuntimeException::new) :
            listeners.stream().filter(listener -> listenerName.equals(listener.getName())).findFirst().orElseThrow(RuntimeException::new);
        return tlsListener.getConfiguration() == null ?
                KafkaResources.clusterCaCertificateSecretName(clusterName) : tlsListener.getConfiguration().getBrokerCertChainAndKey().getSecretName();
    }

    public static String getKafkaExternalListenerCaCertName(String namespace, String clusterName, String listenerName) {
        List<GenericKafkaListener> listeners = kafkaClient().inNamespace(namespace).withName(clusterName).get().getSpec().getKafka().getListeners().newOrConverted();

        GenericKafkaListener external = listenerName == null || listenerName.isEmpty() ?
            listeners.stream().filter(listener -> Constants.EXTERNAL_LISTENER_DEFAULT_NAME.equals(listener.getName())).findFirst().orElseThrow(RuntimeException::new) :
            listeners.stream().filter(listener -> listenerName.equals(listener.getName())).findFirst().orElseThrow(RuntimeException::new);
        return external.getConfiguration() == null ?
            KafkaResources.clusterCaCertificateSecretName(clusterName) : external.getConfiguration().getBrokerCertChainAndKey().getSecretName();
    }

    public static KafkaStatus getKafkaStatus(String clusterName, String namespace) {
        return kafkaClient().inNamespace(namespace).withName(clusterName).get().getStatus();
    }
}
