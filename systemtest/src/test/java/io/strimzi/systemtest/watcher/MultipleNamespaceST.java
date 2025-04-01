/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.strimzi.systemtest.logs.CollectorElement;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;

import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestTags.REGRESSION;

@Tag(REGRESSION)
class MultipleNamespaceST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleNamespaceST.class);

    private void deployTestSpecificClusterOperator() {
        LOGGER.info("Creating Cluster Operator which will watch over multiple Namespaces");

        NamespaceManager.getInstance().createNamespaces(setupClusterOperator.getOperatorNamespace(),
            CollectorElement.createCollectorElement(this.getClass().getName()), Arrays.asList(PRIMARY_KAFKA_WATCHED_NAMESPACE, MAIN_TEST_NAMESPACE));

        setupClusterOperator
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withNamespaceName(CO_NAMESPACE)
                .withNamespacesToWatch(String.join(",", CO_NAMESPACE, PRIMARY_KAFKA_WATCHED_NAMESPACE, MAIN_TEST_NAMESPACE))
                .build()
            )
            .install();
    }

    @BeforeAll
    void setupEnvironment() {
        deployTestSpecificClusterOperator();

        LOGGER.info("deploy all other resources (Kafka cluster and Scrapper) for testing Namespaces");
        deployAdditionalGenericResourcesForAbstractNamespaceST();
    }
}