/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.test.TestUtils;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.k8s.KubeClusterResource;

import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

public class KafkaConnectResource {
    public static final String PATH_TO_KAFKA_CONNECT_CONFIG = TestUtils.USER_PATH + "/../examples/connect/kafka-connect.yaml";

    public static MixedOperation<KafkaConnect, KafkaConnectList, Resource<KafkaConnect>> kafkaConnectClient() {
        return Crds.kafkaConnectV1Beta2Operation(ResourceManager.kubeClient().getClient());
    }

    public static KafkaConnectBuilder kafkaConnect(String name, int kafkaConnectReplicas) {
        return kafkaConnect(name, name, kafkaConnectReplicas);
    }

    public static KafkaConnectBuilder kafkaConnect(String name, String clusterName, int kafkaConnectReplicas) {
        KafkaConnect kafkaConnect = getKafkaConnectFromYaml(PATH_TO_KAFKA_CONNECT_CONFIG);
        return defaultKafkaConnect(kafkaConnect, name, clusterName, kafkaConnectReplicas);
    }

    public static KafkaConnectBuilder kafkaConnectWithMetrics(String name, int kafkaConnectReplicas) {
        return kafkaConnectWithMetrics(name, name, kafkaConnectReplicas);
    }

    public static KafkaConnectBuilder kafkaConnectWithMetrics(String name, String clusterName, int kafkaConnectReplicas) {
        KafkaConnect kafkaConnect = getKafkaConnectFromYaml(Constants.PATH_TO_KAFKA_CONNECT_METRICS_CONFIG);

        ConfigMap metricsCm = TestUtils.configMapFromYaml(Constants.PATH_TO_KAFKA_CONNECT_METRICS_CONFIG, "connect-metrics");
        KubeClusterResource.kubeClient().getClient().configMaps().inNamespace(kubeClient().getNamespace()).createOrReplace(metricsCm);

        return defaultKafkaConnect(kafkaConnect, name, clusterName, kafkaConnectReplicas);
    }

    private static KafkaConnectBuilder defaultKafkaConnect(KafkaConnect kafkaConnect, String name, String kafkaClusterName, int kafkaConnectReplicas) {
        return new KafkaConnectBuilder(kafkaConnect)
            .withNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withClusterName(kafkaClusterName)
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
                    .addToLoggers("connect.root.logger.level", Environment.STRIMZI_COMPONENTS_LOG_LEVEL)
                .endInlineLogging()
            .endSpec();
    }

    public static KafkaConnect createAndWaitForReadiness(KafkaConnect kafkaConnect) {
        return createAndWaitForReadiness(kafkaConnect, true);
    }

    public static KafkaConnect createAndWaitForReadiness(KafkaConnect kafkaConnect, boolean allowNP) {
        if (allowNP) {
            KubernetesResource.deployNetworkPolicyForResource(kafkaConnect, KafkaConnectResources.deploymentName(kafkaConnect.getMetadata().getName()));
        }

        TestUtils.waitFor("KafkaConnect creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, CR_CREATION_TIMEOUT,
            () -> {
                try {
                    kafkaConnectClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kafkaConnect);
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
        return waitFor(deleteLater(kafkaConnect));
    }

    public static KafkaConnect kafkaConnectWithoutWait(KafkaConnect kafkaConnect) {
        kafkaConnectClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kafkaConnect);
        return kafkaConnect;
    }

    public static void deleteKafkaConnectWithoutWait(String resourceName) {
        kafkaConnectClient().inNamespace(ResourceManager.kubeClient().getNamespace()).withName(resourceName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    private static KafkaConnect getKafkaConnectFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaConnect.class);
    }

    private static KafkaConnect waitFor(KafkaConnect kafkaConnect) {
        return ResourceManager.waitForResourceStatus(kafkaConnectClient(), kafkaConnect, Ready);
    }

    private static KafkaConnect deleteLater(KafkaConnect kafkaConnect) {
        return ResourceManager.deleteLater(kafkaConnectClient(), kafkaConnect);
    }

    public static void replaceKafkaConnectResource(String resourceName, Consumer<KafkaConnect> editor) {
        ResourceManager.replaceCrdResource(KafkaConnect.class, KafkaConnectList.class, resourceName, editor);
    }

}
