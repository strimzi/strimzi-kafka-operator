/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

public class KafkaConnectResource {
    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectResource.class);

    public static final String PATH_TO_KAFKA_CONNECT_CONFIG = "../examples/kafka-connect/kafka-connect.yaml";
    public static final String PATH_TO_KAFKA_CONNECT_METRICS_CONFIG = "../examples/metrics/kafka-connect-metrics.yaml";

    public static MixedOperation<KafkaConnect, KafkaConnectList, DoneableKafkaConnect, Resource<KafkaConnect, DoneableKafkaConnect>> kafkaConnectClient() {
        return Crds.kafkaConnectOperation(ResourceManager.kubeClient().getClient());
    }

    public static DoneableKafkaConnect kafkaConnect(String name, int kafkaConnectReplicas) {
        return kafkaConnect(name, name, kafkaConnectReplicas);
    }

    public static DoneableKafkaConnect kafkaConnect(String name, String clusterName, int kafkaConnectReplicas) {
        KafkaConnect kafkaConnect = getKafkaConnectFromYaml(PATH_TO_KAFKA_CONNECT_CONFIG);
        return deployKafkaConnect(defaultKafkaConnect(kafkaConnect, name, clusterName, kafkaConnectReplicas).build(), clusterName);
    }

    public static DoneableKafkaConnect kafkaConnectWithMetrics(String name, int kafkaConnectReplicas) {
        return kafkaConnectWithMetrics(name, kafkaConnectReplicas, true);
    }

    public static DoneableKafkaConnect kafkaConnectWithMetrics(String name, int kafkaConnectReplicas, boolean allowNetworkPolicyAccess) {
        return kafkaConnectWithMetrics(name, name, kafkaConnectReplicas, allowNetworkPolicyAccess);
    }

    public static DoneableKafkaConnect kafkaConnectWithMetrics(String name, String clusterName, int kafkaConnectReplicas, boolean allowNetworkPolicyAccess) {
        KafkaConnect kafkaConnect = getKafkaConnectFromYaml(PATH_TO_KAFKA_CONNECT_METRICS_CONFIG);
        return deployKafkaConnect(defaultKafkaConnect(kafkaConnect, name, clusterName, kafkaConnectReplicas).build(), clusterName);
    }

    public static KafkaConnectBuilder defaultKafkaConnect(String name, String kafkaClusterName, int kafkaConnectReplicas) {
        KafkaConnect kafkaConnect = getKafkaConnectFromYaml(PATH_TO_KAFKA_CONNECT_CONFIG);
        return defaultKafkaConnect(kafkaConnect, name, kafkaClusterName, kafkaConnectReplicas);
    }

    private static KafkaConnectBuilder defaultKafkaConnect(KafkaConnect kafkaConnect, String name, String kafkaClusterName, int kafkaConnectReplicas) {
        return new KafkaConnectBuilder(kafkaConnect)
            .withNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withClusterName(kafkaClusterName)
                .addToLabels("type", "kafka-connect")
            .endMetadata()
            .editOrNewSpec()
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterName))
                .withReplicas(kafkaConnectReplicas)
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder().withNewSecretName(kafkaClusterName + "-cluster-ca-cert").withCertificate("ca.crt").build())
                .endTls()
                .addToConfig("group.id", KafkaConnectResources.deploymentName(kafkaClusterName))
                .addToConfig("offset.storage.topic", KafkaConnectResources.configStorageTopicOffsets(kafkaClusterName))
                .addToConfig("config.storage.topic", KafkaConnectResources.metricsAndLogConfigMapName(kafkaClusterName))
                .addToConfig("status.storage.topic", KafkaConnectResources.configStorageTopicStatus(kafkaClusterName))
                .withNewInlineLogging()
                    .addToLoggers("connect.root.logger.level", "DEBUG")
                .endInlineLogging()
            .endSpec();
    }

    private static DoneableKafkaConnect deployKafkaConnect(KafkaConnect kafkaConnect, String clusterName) {
        if (Environment.DEFAULT_TO_DENY_NETWORK_POLICIES.equals(Boolean.TRUE.toString())) {
            KubernetesResource.allowNetworkPolicySettingsForResource(kafkaConnect, KafkaConnectResources.deploymentName(kafkaConnect.getMetadata().getName()), clusterName);
        }
        return new DoneableKafkaConnect(kafkaConnect, kC -> {
            TestUtils.waitFor("KafkaConnect creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, Constants.TIMEOUT_FOR_CR_CREATION,
                () -> {
                    try {
                        kafkaConnectClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kC);
                        return true;
                    } catch (KubernetesClientException e) {
                        if (e.getMessage().contains("object is being deleted")) {
                            return false;
                        } else {
                            throw e;
                        }
                    }
                }
            );
            return waitFor(deleteLater(kC));
        });
    }

    public static KafkaConnect kafkaConnectWithoutWait(KafkaConnect kafkaConnect) {
        kafkaConnectClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kafkaConnect);
        return kafkaConnect;
    }

    public static void deleteKafkaConnectWithoutWait(KafkaConnect kafkaConnect) {
        kafkaConnectClient().inNamespace(ResourceManager.kubeClient().getNamespace()).delete(kafkaConnect);
    }

    private static KafkaConnect getKafkaConnectFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaConnect.class);
    }

    private static KafkaConnect waitFor(KafkaConnect kafkaConnect) {
        String kafkaConnectCrName = kafkaConnect.getMetadata().getName();

        LOGGER.info("Waiting for Kafka Connect {}", kafkaConnectCrName);
        DeploymentUtils.waitForDeploymentReady(KafkaConnectResources.deploymentName(kafkaConnectCrName), kafkaConnect.getSpec().getReplicas());
        LOGGER.info("Kafka Connect {} is ready", kafkaConnectCrName);

        return kafkaConnect;
    }

    private static KafkaConnect deleteLater(KafkaConnect kafkaConnect) {
        return ResourceManager.deleteLater(kafkaConnectClient(), kafkaConnect);
    }

    public static void replaceKafkaConnectResource(String resourceName, Consumer<KafkaConnect> editor) {
        ResourceManager.replaceCrdResource(KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class, resourceName, editor);
    }

}
