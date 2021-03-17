/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.HELM;
import static io.strimzi.systemtest.Constants.REGRESSION;

@Tag(HELM)
@Tag(REGRESSION)
class HelmChartST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HelmChartST.class);
    static final String NAMESPACE = "helm-chart-cluster-test";

    @IsolatedTest
    void testStrimziComponentsViaHelmChart(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext,
            KafkaClientsTemplates.kafkaClients(true, kafkaClientsName).build());

        resourceManager.createResource(extensionContext,
            // Deploy Kafka and wait for readiness
            KafkaTemplates.kafkaEphemeral(clusterName, 3).build(),
            KafkaTopicTemplates.topic(clusterName, TOPIC_NAME).build(),
            // Deploy KafkaConnect and wait for readiness
            KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1).editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata().build(),
            KafkaConnectorTemplates.kafkaConnector(clusterName).build(),
            // Deploy KafkaBridge (different image than Kafka) and wait for readiness
            KafkaBridgeTemplates.kafkaBridge(clusterName, KafkaResources.plainBootstrapAddress(clusterName), 1).build());
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        LOGGER.info("Creating resources before the test class");
        cluster.createNamespace(NAMESPACE);
        helmResource.create();
    }

    @AfterAll
    protected void tearDownEnvironmentAfterAll() {
        helmResource.delete();
        cluster.deleteNamespaces();
    }
}
