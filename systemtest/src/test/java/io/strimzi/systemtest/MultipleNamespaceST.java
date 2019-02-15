/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.extensions.StrimziExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

@ExtendWith(StrimziExtension.class)
@ClusterOperator
class MultipleNamespaceST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleNamespaceST.class);

    /**
     * Test the case where the TO is configured to watch a different namespace that it is deployed in
     */
    @Test
    @Tag(REGRESSION)
    void testTopicOperatorWatchingOtherNamespace() {
        LOGGER.info("Deploying TO to watch a different namespace that it is deployed in");

        List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems(TOPIC_NAME)));

        deployNewTopic(SECOND_NAMESPACE, CO_NAMESPACE, TOPIC_NAME);
        deleteNewTopic(SECOND_NAMESPACE, TOPIC_NAME);
    }

    /**
     * Test the case when Kafka will be deployed in different namespace than CO
     */
    @Test
    @Tag(REGRESSION)
    void testKafkaInDifferentNsThanClusterOperator() {
        LOGGER.info("Deploying Kafka in different namespace than CO when CO watches multiple namespaces");
        checkKafkaInDiffNamespaceThanCO();
    }

    /**
     * Test the case when MirrorMaker will be deployed in different namespace across multiple namespaces
     */
    @Test
    @Tag(REGRESSION)
    void testDeployMirrorMakerAcrossMultipleNamespace() {
        LOGGER.info("Deploying Kafka MirrorMaker in different namespace than CO when CO watches multiple namespaces");
        checkMirrorMakerForKafkaInDifNamespaceThanCO();
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        applyRoleBindings(CO_NAMESPACE, CO_NAMESPACE, SECOND_NAMESPACE);

        LOGGER.info("Deploying CO to watch multiple namespaces");
        testClassResources.clusterOperator(String.join(",", CO_NAMESPACE, SECOND_NAMESPACE)).done();

        testClassResources.kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editEntityOperator()
                    .editTopicOperator()
                        .withWatchedNamespace(SECOND_NAMESPACE)
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
            .done();

        testClass = testInfo.getTestClass().get().getSimpleName();
    }
}
