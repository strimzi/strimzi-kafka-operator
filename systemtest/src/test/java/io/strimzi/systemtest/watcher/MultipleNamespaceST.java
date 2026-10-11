/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.List;

import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestTags.REGRESSION;

@Tag(REGRESSION)
class MultipleNamespaceST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleNamespaceST.class);

    private void deployTestSpecificClusterOperator() {
        LOGGER.info("Creating Cluster Operator which will watch over multiple Namespaces");

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
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withNamespaceName(CO_NAMESPACE)
                .withNamespacesToWatch(String.join(",", CO_NAMESPACE, PRIMARY_KAFKA_WATCHED_NAMESPACE, MAIN_TEST_NAMESPACE))
                .withExtraEnvVars(List.of(
                    new EnvVar("STRIMZI_ENTITY_OPERATOR_WATCHED_NAMESPACE_ENABLED", "true", null)
                ))
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