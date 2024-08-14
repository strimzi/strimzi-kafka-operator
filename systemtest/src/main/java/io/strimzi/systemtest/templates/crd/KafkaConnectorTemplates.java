/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
import io.strimzi.operator.common.model.Labels;

public class KafkaConnectorTemplates {

    private static final int DEFAULT_MAX_TASKS = 2;

    private KafkaConnectorTemplates() {}

    public static KafkaConnectorBuilder kafkaConnector(String namespaceName, String kafkaConnectClusterName) {
        return kafkaConnector(namespaceName, kafkaConnectClusterName, DEFAULT_MAX_TASKS);
    }

    public static KafkaConnectorBuilder kafkaConnector(String namespaceName, String kafkaConnectClusterName, int maxTasks) {
        return kafkaConnector(namespaceName, kafkaConnectClusterName, kafkaConnectClusterName, maxTasks);
    }

    public static KafkaConnectorBuilder kafkaConnector(
        String namespaceName,
        String connectorName,
        String kafkaConnectClusterName
    ) {
        return kafkaConnector(namespaceName, connectorName, kafkaConnectClusterName, DEFAULT_MAX_TASKS);
    }

    public static KafkaConnectorBuilder kafkaConnector(
        String namespaceName,
        String connectorName,
        String kafkaConnectClusterName,
        int maxTasks
    ) {
        return defaultKafkaConnector(namespaceName, connectorName, kafkaConnectClusterName, maxTasks);
    }

    private static KafkaConnectorBuilder defaultKafkaConnector(
        String namespaceName,
        String connectorName,
        String kafkaConnectClusterName,
        int maxTasks
    ) {
        return new KafkaConnectorBuilder()
            .withNewMetadata()
                .withName(connectorName)
                .withNamespace(namespaceName)
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, kafkaConnectClusterName)
            .endMetadata()
            .editOrNewSpec()
                .withTasksMax(maxTasks)
                .withClassName("org.apache.kafka.connect.file.FileStreamSourceConnector")
                .addToConfig("file", "/opt/kafka/LICENSE")
                .addToConfig("topic", "my-topic")
            .endSpec();
    }
}
