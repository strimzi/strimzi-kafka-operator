/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaBridgeList;
import io.strimzi.api.kafka.model.DoneableKafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.test.TestUtils;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

public class KafkaBridgeResource {

    public static final String PATH_TO_KAFKA_BRIDGE_CONFIG = "../examples/kafka-bridge/kafka-bridge.yaml";

    public static MixedOperation<KafkaBridge, KafkaBridgeList, DoneableKafkaBridge, Resource<KafkaBridge, DoneableKafkaBridge>> kafkaBridgeClient() {
        return Crds.kafkaBridgeOperation(ResourceManager.kubeClient().getClient());
    }

    public static DoneableKafkaBridge kafkaBridge(String name, String bootstrap, int kafkaBridgeReplicas) {
        return kafkaBridge(name, name, bootstrap, kafkaBridgeReplicas);
    }

    public static DoneableKafkaBridge kafkaBridge(String name, String clusterName, String bootstrap, int kafkaBridgeReplicas) {
        KafkaBridge kafkaBridge = getKafkaBridgeFromYaml(PATH_TO_KAFKA_BRIDGE_CONFIG);
        return deployKafkaBridge(defaultKafkaBridge(kafkaBridge, name, clusterName, bootstrap, kafkaBridgeReplicas).build());
    }

    public static DoneableKafkaBridge kafkaBridgeWithCors(String name, String bootstrap, int kafkaBridgeReplicas,
                                                          String allowedCorsOrigin, String allowedCorsMethods) {
        return kafkaBridgeWithCors(name, name, bootstrap, kafkaBridgeReplicas, allowedCorsOrigin, allowedCorsMethods);
    }

    public static DoneableKafkaBridge kafkaBridgeWithCors(String name, String clusterName, String bootstrap,
                                                          int kafkaBridgeReplicas, String allowedCorsOrigin,
                                                          String allowedCorsMethods) {
        KafkaBridge kafkaBridge = getKafkaBridgeFromYaml(PATH_TO_KAFKA_BRIDGE_CONFIG);

        KafkaBridgeBuilder kafkaBridgeBuilder = defaultKafkaBridge(kafkaBridge, name, clusterName, bootstrap, kafkaBridgeReplicas);

        kafkaBridgeBuilder
            .editSpec()
                .editHttp()
                    .withNewCors()
                        .withAllowedOrigins(allowedCorsOrigin)
                        .withAllowedMethods(allowedCorsMethods != null ? allowedCorsMethods : "GET,POST,PUT,DELETE,OPTIONS,PATCH")
                    .endCors()
                .endHttp()
            .endSpec();

        return deployKafkaBridge(kafkaBridgeBuilder.build());
    }

    private static KafkaBridgeBuilder defaultKafkaBridge(KafkaBridge kafkaBridge, String name, String kafkaClusterName, String bootstrap, int kafkaBridgeReplicas) {
        return new KafkaBridgeBuilder(kafkaBridge)
            .withNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withClusterName(kafkaClusterName)
            .endMetadata()
            .editSpec()
                .withBootstrapServers(bootstrap)
                .withReplicas(kafkaBridgeReplicas)
                .withNewInlineLogging()
                    .addToLoggers("bridge.root.logger", "DEBUG")
                .endInlineLogging()
            .endSpec();
    }

    private static DoneableKafkaBridge deployKafkaBridge(KafkaBridge kafkaBridge) {
        return new DoneableKafkaBridge(kafkaBridge, kB -> {
            TestUtils.waitFor("KafkaBridge creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, Constants.TIMEOUT_FOR_CR_CREATION,
                () -> {
                    try {
                        kafkaBridgeClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kB);
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
            return waitFor(deleteLater(kB));
        });
    }

    public static KafkaBridge kafkaBridgeWithoutWait(KafkaBridge kafkaBridge) {
        kafkaBridgeClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kafkaBridge);
        return kafkaBridge;
    }

    public static void deleteKafkaBridgeWithoutWait(KafkaBridge kafkaBridge) {
        kafkaBridgeClient().inNamespace(ResourceManager.kubeClient().getNamespace()).delete(kafkaBridge);
    }

    private static KafkaBridge getKafkaBridgeFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaBridge.class);
    }

    private static KafkaBridge waitFor(KafkaBridge kafkaBridge) {
        return ResourceManager.waitForResourceStatus(kafkaBridgeClient(), kafkaBridge, "Ready");
    }

    private static KafkaBridge deleteLater(KafkaBridge kafkaBridge) {
        return ResourceManager.deleteLater(kafkaBridgeClient(), kafkaBridge);
    }

    public static void replaceBridgeResource(String resourceName, Consumer<KafkaBridge> editor) {
        ResourceManager.replaceCrdResource(KafkaBridge.class, KafkaBridgeList.class, DoneableKafkaBridge.class, resourceName, editor);
    }
}
