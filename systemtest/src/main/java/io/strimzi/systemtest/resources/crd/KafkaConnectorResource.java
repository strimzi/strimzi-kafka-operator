/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

public class KafkaConnectorResource {
    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectorResource.class);

    public static final String PATH_TO_KAFKA_CONNECTOR_CONFIG = "../examples/connector/source-connector.yaml";

    public static MixedOperation<KafkaConnector, KafkaConnectorList, DoneableKafkaConnector, Resource<KafkaConnector, DoneableKafkaConnector>> kafkaConnectorClient() {
        return Crds.kafkaConnectorOperation(ResourceManager.kubeClient().getClient());
    }

    public static DoneableKafkaConnector kafkaConnector(String name) {
        return kafkaConnector(name, name, 2);
    }

    public static DoneableKafkaConnector kafkaConnector(String name, int maxTasks) {
        return kafkaConnector(name, name, maxTasks);
    }

    public static DoneableKafkaConnector kafkaConnector(String name, String clusterName, int maxTasks) {
        KafkaConnector kafkaConnector = getKafkaConnectorFromYaml(PATH_TO_KAFKA_CONNECTOR_CONFIG);
        return deployKafkaConnector(defaultKafkaConnector(kafkaConnector, name, clusterName, maxTasks).build());
    }

    public static KafkaConnectorBuilder defaultKafkaConnector(String name, String clusterName, int maxTasks) {
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

    private static DoneableKafkaConnector deployKafkaConnector(KafkaConnector kafkaConnector) {
        return new DoneableKafkaConnector(kafkaConnector, kC -> {
            TestUtils.waitFor("KafkaConnector creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, Constants.TIMEOUT_FOR_CR_CREATION,
                () -> {
                    try {
                        kafkaConnectorClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kC);
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

    private static KafkaConnector getKafkaConnectorFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaConnector.class);
    }

    private static KafkaConnector waitFor(KafkaConnector kafkaConnector) {
        String kafkaConnectorCrName = kafkaConnector.getMetadata().getName();

        LOGGER.info("Waiting for Kafka Connector {}", kafkaConnectorCrName);
        KafkaConnectorUtils.waitForConnectorStatus(kafkaConnectorCrName, "Ready");
        LOGGER.info("Kafka Connector {} is ready", kafkaConnectorCrName);

        return kafkaConnector;
    }

    private static KafkaConnector deleteLater(KafkaConnector kafkaConnector) {
        return ResourceManager.deleteLater(kafkaConnectorClient(), kafkaConnector);
    }

    public static void replaceKafkaConnectorResource(String resourceName, Consumer<KafkaConnector> editor) {
        ResourceManager.replaceCrdResource(KafkaConnector.class, KafkaConnectorList.class, DoneableKafkaConnector.class, resourceName, editor);
    }
}
