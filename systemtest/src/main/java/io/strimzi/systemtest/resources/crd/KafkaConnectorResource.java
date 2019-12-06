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
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.strimzi.systemtest.resources.ResourceManager;

public class KafkaConnectorResource {
    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectorResource.class);

    public static final String PATH_TO_KAFKA_CONNECTOR_CONFIG = "../examples/connector/source-connector.yaml";

    public static MixedOperation<KafkaConnector, KafkaConnectorList, DoneableKafkaConnector, Resource<KafkaConnector, DoneableKafkaConnector>> kafkaConnectorClient() {
        return Crds.kafkaConnectorOperation(ResourceManager.kubeClient().getClient());
    }

    public static DoneableKafkaConnector kafkaConnector(String name, String topicName) {
        return kafkaConnector(name, name, topicName, 2);
    }

    public static DoneableKafkaConnector kafkaConnector(String name, String topicName, int maxTasks) {
        return kafkaConnector(name, name, topicName, maxTasks);
    }

    public static DoneableKafkaConnector kafkaConnector(String name, String clusterName, String topicName, int maxTasks) {
        KafkaConnector kafkaConnector = getKafkaConnectorFromYaml(PATH_TO_KAFKA_CONNECTOR_CONFIG);
        return deployKafkaConnector(defaultKafkaConnector(kafkaConnector, name, clusterName, topicName, maxTasks).build());
    }

    private static KafkaConnectorBuilder defaultKafkaConnector(KafkaConnector kafkaConnector, String name, String kafkaClusterName, String topicName, int maxTasks) {
        return new KafkaConnectorBuilder(kafkaConnector)
            .editOrNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .addToLabels("strimzi.io/cluster", kafkaClusterName)
            .endMetadata()
            .editOrNewSpec()
                .addToConfig("topic", topicName)
                .withTasksMax(maxTasks)
            .endSpec();
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
        LOGGER.info("Waiting for Kafka Connector {}", kafkaConnector.getMetadata().getName());
        StUtils.waitForConnectorReady(kafkaConnector.getMetadata().getName());
        LOGGER.info("Kafka Connector {} is ready", kafkaConnector.getMetadata().getName());
        return kafkaConnector;
    }

    private static KafkaConnector deleteLater(KafkaConnector kafkaConnector) {
        return ResourceManager.deleteLater(kafkaConnectorClient(), kafkaConnector);
    }

}
