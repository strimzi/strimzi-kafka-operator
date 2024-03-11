/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;

import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.REGRESSION;

@Tag(REGRESSION)
class MultipleNamespaceST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleNamespaceST.class);

    private void deployTestSpecificClusterOperator() {
        LOGGER.info("Creating Cluster Operator which will watch over multiple Namespaces");

        NamespaceManager.getInstance().createNamespaces(clusterOperator.getDeploymentNamespace(),
            CollectorElement.createCollectorElement(this.getClass().getName()), Arrays.asList(PRIMARY_KAFKA_WATCHED_NAMESPACE, MAIN_TEST_NAMESPACE));

        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(ResourceManager.getTestContext())
            .withNamespace(CO_NAMESPACE)
            .withWatchingNamespaces(String.join(",", CO_NAMESPACE, PRIMARY_KAFKA_WATCHED_NAMESPACE, MAIN_TEST_NAMESPACE))
            .withBindingsNamespaces(Arrays.asList(CO_NAMESPACE, PRIMARY_KAFKA_WATCHED_NAMESPACE, MAIN_TEST_NAMESPACE))
            .createInstallation()
            .runInstallation();
    }

    @BeforeAll
    void setupEnvironment() {
        deployTestSpecificClusterOperator();

        LOGGER.info("deploy all other resources (Kafka Cluster and Scrapper) for testing Namespaces");
        deployAdditionalGenericResourcesForAbstractNamespaceST();
    }
}