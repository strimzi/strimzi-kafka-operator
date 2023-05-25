/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
@IsolatedSuite
class AllNamespaceIsolatedST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(AllNamespaceIsolatedST.class);

    private void deployTestSpecificClusterOperator() {
        LOGGER.info("Creating Cluster Operator which will watch over all namespaces");

        clusterOperator.unInstall();
        clusterOperator = clusterOperator.defaultInstallation()
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .withBindingsNamespaces(Arrays.asList(clusterOperator.getDeploymentNamespace(), PRIMARY_KAFKA_WATCHED_NAMESPACE, MAIN_TEST_NAMESPACE))
            .createInstallation()
            .runInstallation();
    }

    @BeforeAll
    void setupEnvironment(ExtensionContext extensionContext) {
        // Strimzi is deployed with cluster-wide access in this class STRIMZI_RBAC_SCOPE=NAMESPACE won't work
        assumeFalse(Environment.isNamespaceRbacScope());

        deployTestSpecificClusterOperator();

        LOGGER.info("deploy all other resources (Kafka Cluster and Scrapper) for testing namespaces");
        deployAdditionalGenericResourcesForAbstractNamespaceST(extensionContext);
    }
}
