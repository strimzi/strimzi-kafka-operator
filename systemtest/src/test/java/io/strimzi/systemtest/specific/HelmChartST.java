/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.operator.HelmResource;
import io.strimzi.systemtest.AbstractST;
import org.apache.logging.log4j.LogManager;
import io.strimzi.systemtest.templates.KafkaTemplates;
import io.strimzi.systemtest.templates.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
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

    @Test
    void testStrimziComponentsViaHelmChart() {
        // Deploy Kafka and wait for readiness
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(clusterName, 3).build());
        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(clusterName, TOPIC_NAME).build());
        // Deploy KafkaConnect and wait for readiness
        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS).build());
        KafkaConnectResource.createAndWaitForReadiness(KafkaConnectResource.kafkaConnect(clusterName, 1)
            .editMetadata()
                .addToAnnotations("strimzi.io/use-connector-resources", "true")
            .endMetadata().build());
        KafkaConnectorResource.createAndWaitForReadiness(KafkaConnectorResource.kafkaConnector(clusterName).build());
        // Deploy KafkaBridge (different image than Kafka) and wait for readiness
        KafkaBridgeResource.createAndWaitForReadiness(KafkaBridgeResource.kafkaBridge(clusterName, KafkaResources.plainBootstrapAddress(clusterName), 1).build());
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        LOGGER.info("Creating resources before the test class");
        cluster.createNamespace(NAMESPACE);
        resourceManager.createResource(extensionContext, HelmResource.clusterOperator());
    }

    @AfterAll
    protected void tearDownEnvironmentAfterAll() {
        cluster.deleteNamespaces();
    }
}
