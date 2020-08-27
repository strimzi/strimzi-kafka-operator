/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectS2IList;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.test.TestUtils;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;

public class KafkaConnectS2IResource {
    public static final String PATH_TO_KAFKA_CONNECT_S2I_CONFIG = TestUtils.USER_PATH + "/../examples/connect/kafka-connect-s2i.yaml";

    public static MixedOperation<KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I, Resource<KafkaConnectS2I, DoneableKafkaConnectS2I>> kafkaConnectS2IClient() {
        return Crds.kafkaConnectS2iOperation(ResourceManager.kubeClient().getClient());
    }

    public static DoneableKafkaConnectS2I kafkaConnectS2I(String name, String clusterName, int kafkaConnectS2IReplicas) {
        KafkaConnectS2I kafkaConnectS2I = getKafkaConnectS2IFromYaml(PATH_TO_KAFKA_CONNECT_S2I_CONFIG);
        return deployKafkaConnectS2I(defaultKafkaConnectS2I(kafkaConnectS2I, name, clusterName, kafkaConnectS2IReplicas).build());
    }

    public static KafkaConnectS2IBuilder defaultKafkaConnectS2I(String name, String kafkaClusterName, int kafkaConnectReplicas) {
        KafkaConnectS2I kafkaConnectS2I = getKafkaConnectS2IFromYaml(PATH_TO_KAFKA_CONNECT_S2I_CONFIG);
        return defaultKafkaConnectS2I(kafkaConnectS2I, name, kafkaClusterName, kafkaConnectReplicas);
    }

    public static KafkaConnectS2IBuilder defaultKafkaConnectS2I(KafkaConnectS2I kafkaConnectS2I, String name, String kafkaClusterName, int kafkaConnectReplicas) {
        return new KafkaConnectS2IBuilder(kafkaConnectS2I)
            .withNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withClusterName(kafkaClusterName)
            .endMetadata()
            .editSpec()
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterName))
                .withReplicas(kafkaConnectReplicas)
                // Try it without TLS
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder().withNewSecretName(kafkaClusterName + "-cluster-ca-cert").withCertificate("ca.crt").build())
                .endTls()
                .withInsecureSourceRepository(true)
                .withNewInlineLogging()
                    .addToLoggers("log4j.rootLogger", "DEBUG")
                .endInlineLogging()
            .endSpec();
    }

    private static DoneableKafkaConnectS2I deployKafkaConnectS2I(KafkaConnectS2I kafkaConnectS2I) {
        if (Environment.DEFAULT_TO_DENY_NETWORK_POLICIES.equals(Boolean.TRUE.toString())) {
            KubernetesResource.allowNetworkPolicySettingsForResource(kafkaConnectS2I, KafkaConnectS2IResources.deploymentName(kafkaConnectS2I.getMetadata().getName()));
        }
        return new DoneableKafkaConnectS2I(kafkaConnectS2I, kC -> {
            TestUtils.waitFor("KafkaConnect creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, CR_CREATION_TIMEOUT,
                () -> {
                    try {
                        kafkaConnectS2IClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kC);
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

    public static KafkaConnectS2I kafkaConnectS2IWithoutWait(KafkaConnectS2I kafkaConnectS2I) {
        kafkaConnectS2IClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kafkaConnectS2I);
        return kafkaConnectS2I;
    }

    public static void deleteKafkaConnectS2IWithoutWait(String resourceName) {
        kafkaConnectS2IClient().inNamespace(ResourceManager.kubeClient().getNamespace()).withName(resourceName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    private static KafkaConnectS2I getKafkaConnectS2IFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaConnectS2I.class);
    }

    private static KafkaConnectS2I waitFor(KafkaConnectS2I kafkaConnectS2I) {
        return ResourceManager.waitForResourceStatus(kafkaConnectS2IClient(), kafkaConnectS2I, Ready);
    }

    private static KafkaConnectS2I deleteLater(KafkaConnectS2I kafkaConnectS2I) {
        return ResourceManager.deleteLater(kafkaConnectS2IClient(), kafkaConnectS2I);
    }

    public static void replaceConnectS2IResource(String resourceName, Consumer<KafkaConnectS2I> editor) {
        ResourceManager.replaceCrdResource(KafkaConnectS2I.class, KafkaConnectS2IList.class, DoneableKafkaConnectS2I.class, resourceName, editor);
    }
}
