/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.TestConstants.HELM;
import static io.strimzi.systemtest.TestConstants.REGRESSION;

@Tag(HELM)
@Tag(REGRESSION)
class HelmChartST extends AbstractST {

    @IsolatedTest
    void testStrimziComponentsViaHelmChart() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        // Deploy Kafka and wait for readiness
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());

        resourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build(),
            // Deploy KafkaConnect and wait for readiness
            KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE, 1)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .build(),
            // Deploy KafkaBridge (different image than Kafka) and wait for readiness
            KafkaBridgeTemplates.kafkaBridge(testStorage.getClusterName(), KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), 1).build());

        resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getClusterName()).build());
    }

    @BeforeAll
    void setup() {
        clusterOperator = clusterOperator.defaultInstallation()
            .createInstallation()
            // run always Helm installation
            .runHelmInstallation();

        cluster.setNamespace(Environment.TEST_SUITE_NAMESPACE);
    }
}
