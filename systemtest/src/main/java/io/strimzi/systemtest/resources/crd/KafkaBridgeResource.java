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
import io.strimzi.api.kafka.KafkaBridgeList;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.test.TestUtils;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;

public class KafkaBridgeResource {

    public static final String PATH_TO_KAFKA_BRIDGE_CONFIG = TestUtils.USER_PATH + "/../packaging/examples/bridge/kafka-bridge.yaml";
    public static final String PATH_TO_KAFKA_BRIDGE_METRICS_CONFIG = TestUtils.USER_PATH + "/../packaging/examples/metrics/kafka-bridge-metrics.yaml";

    public static MixedOperation<KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>> kafkaBridgeClient() {
        return Crds.kafkaBridgeV1Beta2Operation(ResourceManager.kubeClient().getClient());
    }

    public static KafkaBridgeBuilder kafkaBridge(String name, String bootstrap, int kafkaBridgeReplicas) {
        return kafkaBridge(name, name, bootstrap, kafkaBridgeReplicas);
    }

    public static KafkaBridgeBuilder kafkaBridge(String name, String clusterName, String bootstrap, int kafkaBridgeReplicas) {
        KafkaBridge kafkaBridge = getKafkaBridgeFromYaml(PATH_TO_KAFKA_BRIDGE_CONFIG);
        return defaultKafkaBridge(kafkaBridge, name, clusterName, bootstrap, kafkaBridgeReplicas);
    }

    public static KafkaBridgeBuilder kafkaBridgeWithCors(String name, String bootstrap, int kafkaBridgeReplicas, String allowedCorsOrigin, String allowedCorsMethods) {
        return kafkaBridgeWithCors(name, name, bootstrap, kafkaBridgeReplicas, allowedCorsOrigin, allowedCorsMethods);
    }

    public static KafkaBridgeBuilder kafkaBridgeWithCors(String name, String clusterName, String bootstrap, int kafkaBridgeReplicas, String allowedCorsOrigin, String allowedCorsMethods) {
        return kafkaBridge(name, clusterName, bootstrap, kafkaBridgeReplicas)
            .editSpec()
                .editHttp()
                    .withNewCors()
                        .withAllowedOrigins(allowedCorsOrigin)
                        .withAllowedMethods(allowedCorsMethods != null ? allowedCorsMethods : "GET,POST,PUT,DELETE,OPTIONS,PATCH")
                    .endCors()
                .endHttp()
            .endSpec();
    }

    public static KafkaBridgeBuilder kafkaBridgeWithMetrics(String name, String clusterName, String bootstrap) {
        return kafkaBridgeWithMetrics(name, clusterName, bootstrap, 1);
    }

    public static KafkaBridgeBuilder kafkaBridgeWithMetrics(String name, String clusterName, String bootstrap, int kafkaBridgeReplicas) {
        KafkaBridge kafkaBridge = getKafkaBridgeFromYaml(PATH_TO_KAFKA_BRIDGE_METRICS_CONFIG);

        return defaultKafkaBridge(kafkaBridge, name, clusterName, bootstrap, kafkaBridgeReplicas);
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
                    .addToLoggers("bridge.root.logger", Environment.STRIMZI_COMPONENTS_LOG_LEVEL)
                .endInlineLogging()
            .endSpec();
    }

    public static KafkaBridge createAndWaitForReadiness(KafkaBridge kafkaBridge) {
        KubernetesResource.deployNetworkPolicyForResource(kafkaBridge, KafkaBridgeResources.deploymentName(kafkaBridge.getMetadata().getName()));

        TestUtils.waitFor("KafkaBridge creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, CR_CREATION_TIMEOUT,
            () -> {
                try {
                    kafkaBridgeClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kafkaBridge);
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
        return waitFor(deleteLater(kafkaBridge));
    }

    public static KafkaBridge kafkaBridgeWithoutWait(KafkaBridge kafkaBridge) {
        kafkaBridgeClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kafkaBridge);
        return kafkaBridge;
    }

    public static void deleteKafkaBridgeWithoutWait(String resourceName) {
        kafkaBridgeClient().inNamespace(ResourceManager.kubeClient().getNamespace()).withName(resourceName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    private static KafkaBridge getKafkaBridgeFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaBridge.class);
    }

    private static KafkaBridge waitFor(KafkaBridge kafkaBridge) {
        return ResourceManager.waitForResourceStatus(kafkaBridgeClient(), kafkaBridge, Ready);
    }

    private static KafkaBridge deleteLater(KafkaBridge kafkaBridge) {
        return ResourceManager.deleteLater(kafkaBridgeClient(), kafkaBridge);
    }

    public static void replaceBridgeResource(String resourceName, Consumer<KafkaBridge> editor) {
        ResourceManager.replaceCrdResource(KafkaBridge.class, KafkaBridgeList.class, resourceName, editor);
    }
}
