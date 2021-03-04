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
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.test.TestUtils;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;

public class KafkaConnectorResource {
    public static final String PATH_TO_KAFKA_CONNECTOR_CONFIG = TestUtils.USER_PATH + "/../packaging/examples/connect/source-connector.yaml";

    public static MixedOperation<KafkaConnector, KafkaConnectorList, Resource<KafkaConnector>> kafkaConnectorClient() {
        return Crds.kafkaConnectorV1Beta2Operation(ResourceManager.kubeClient().getClient());
    }

    public static KafkaConnectorBuilder kafkaConnector(String name) {
        return kafkaConnector(name, name, 2);
    }

    public static KafkaConnectorBuilder kafkaConnector(String name, String clusterName) {
        return kafkaConnector(name, clusterName, 2);
    }

    public static KafkaConnectorBuilder kafkaConnector(String name, String clusterName, int maxTasks) {
        KafkaConnector kafkaConnector = getKafkaConnectorFromYaml(PATH_TO_KAFKA_CONNECTOR_CONFIG);
        return defaultKafkaConnector(kafkaConnector, name, clusterName, maxTasks);
    }

    public static KafkaConnectorBuilder defaultKafkaConnector(KafkaConnector kafkaConnector, String name, String kafkaConnectClusterName, int maxTasks) {
        return new KafkaConnectorBuilder(kafkaConnector)
            .editOrNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, kafkaConnectClusterName)
            .endMetadata()
            .editOrNewSpec()
                .withTasksMax(maxTasks)
            .endSpec();
    }

    public static KafkaConnector kafkaConnectorWithoutWait(KafkaConnector kafkaConnector) {
        kafkaConnectorClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kafkaConnector);
        return kafkaConnector;
    }

    public static void deleteKafkaConnectorWithoutWait(String connectorName) {
        kafkaConnectorClient().inNamespace(ResourceManager.kubeClient().getNamespace()).withName(connectorName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    public static KafkaConnector createAndWaitForReadiness(KafkaConnector kafkaConnector) {
        TestUtils.waitFor("KafkaConnector creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, CR_CREATION_TIMEOUT,
            () -> {
                try {
                    kafkaConnectorClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kafkaConnector);
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
        return waitFor(deleteLater(kafkaConnector));
    }

    private static KafkaConnector getKafkaConnectorFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaConnector.class);
    }

    private static KafkaConnector waitFor(KafkaConnector kafkaConnector) {
        return ResourceManager.waitForResourceStatus(kafkaConnectorClient(), kafkaConnector, Ready);
    }

    private static KafkaConnector deleteLater(KafkaConnector kafkaConnector) {
        return ResourceManager.deleteLater(kafkaConnectorClient(), kafkaConnector);
    }

    public static void replaceKafkaConnectorResource(String resourceName, Consumer<KafkaConnector> editor) {
        ResourceManager.replaceCrdResource(KafkaConnector.class, KafkaConnectorList.class, resourceName, editor);
    }
}
