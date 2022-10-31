/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.resources.operator.specific.HelmResource;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.HELM;
import static io.strimzi.systemtest.Constants.REGRESSION;

@Tag(HELM)
@Tag(REGRESSION)
@IsolatedSuite
class HelmChartIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HelmChartIsolatedST.class);
    private HelmResource helmResource = new HelmResource(Constants.INFRA_NAMESPACE);

    @IsolatedTest
    void testStrimziComponentsViaHelmChart(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        // Deploy Kafka and wait for readiness
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(clusterName, topicName).build(),
            // Deploy KafkaConnect and wait for readiness
            KafkaConnectTemplates.kafkaConnectWithFilePlugin(clusterName, clusterOperator.getDeploymentNamespace(), 1)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .build(),
            // Deploy KafkaBridge (different image than Kafka) and wait for readiness
            KafkaBridgeTemplates.kafkaBridge(clusterName, KafkaResources.plainBootstrapAddress(clusterName), 1).build());

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName).build());
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        // shared classic CO operator removed
        clusterOperator.unInstall();

        LOGGER.info("Creating resources before the test class");
        cluster.createNamespace(CollectorElement.createCollectorElement(extensionContext.getRequiredTestClass().getName()), clusterOperator.getDeploymentNamespace());
        // Helm CO created
        helmResource.create(extensionContext);
    }

    @Override
    protected void afterAllMayOverride(ExtensionContext extensionContext) throws Exception {
        // Helm CO deleted
        helmResource.delete();

        super.afterAllMayOverride(extensionContext);
        // back to the old CO because we performed verification by Helm Chart Cluster Operator and now we continue with default.
        clusterOperator.defaultInstallation()
            .createInstallation()
            .runInstallation();
    }
}
