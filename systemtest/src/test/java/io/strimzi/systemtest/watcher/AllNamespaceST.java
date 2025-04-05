/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
class AllNamespaceST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(AllNamespaceST.class);

    private void deployTestSpecificClusterOperator() {
        LOGGER.info("Creating Cluster Operator which will watch over all Namespaces");

        KubeResourceManager.get().createResourceWithWait(
            new NamespaceBuilder()
                .withNewMetadata()
                    .withName(PRIMARY_KAFKA_WATCHED_NAMESPACE)
                .endMetadata()
                .build(),
            new NamespaceBuilder()
                .withNewMetadata()
                    .withName(MAIN_TEST_NAMESPACE)
                .endMetadata()
                .build()
        );

        SetupClusterOperator
            .get()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withNamespacesToWatch(TestConstants.WATCH_ALL_NAMESPACES)
                .build()
            )
            .install();
    }

    @BeforeAll
    void setupEnvironment() {
        // Strimzi is deployed with cluster-wide access in this class STRIMZI_RBAC_SCOPE=NAMESPACE won't work
        assumeFalse(Environment.isNamespaceRbacScope());

        deployTestSpecificClusterOperator();

        LOGGER.info("Deploying all other resources (Kafka cluster and Scrapper) for testing Namespaces");
        deployAdditionalGenericResourcesForAbstractNamespaceST();
    }
}
