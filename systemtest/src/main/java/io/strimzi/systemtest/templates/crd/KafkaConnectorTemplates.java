/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
import io.strimzi.operator.common.model.Labels;

public class KafkaConnectorTemplates {

    private KafkaConnectorTemplates() {}

    public static KafkaConnectorBuilder kafkaConnector(String namespaceName, String clusterName) {
        return kafkaConnector(namespaceName, clusterName, 2);
    }

    public static KafkaConnectorBuilder kafkaConnector(String namespaceName, String clusterName, int maxTasks) {
        return kafkaConnector(namespaceName, clusterName, clusterName, maxTasks);
    }

    public static KafkaConnectorBuilder kafkaConnector(
        String namespaceName,
        String connectorName,
        String clusterName
    ) {
        return kafkaConnector(namespaceName, connectorName, clusterName, 2);
    }

    public static KafkaConnectorBuilder kafkaConnector(
        String namespaceName,
        String connectorName,
        String clusterName,
        int maxTasks
    ) {
        return defaultKafkaConnector(namespaceName, connectorName, clusterName, maxTasks);
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
