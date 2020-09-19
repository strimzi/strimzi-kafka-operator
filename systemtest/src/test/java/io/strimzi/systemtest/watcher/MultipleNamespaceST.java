/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.resources.operator.BundleResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;

import java.util.Arrays;
import java.util.List;

import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

@Tag(REGRESSION)
class MultipleNamespaceST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleNamespaceST.class);

    /**
     * Test the case where the TO is configured to watch a different namespace that it is deployed in
     */
    @Test
    void testTopicOperatorWatchingOtherNamespace() {
        LOGGER.info("Deploying TO to watch a different namespace that it is deployed in");
        cluster.setNamespace(SECOND_NAMESPACE);
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems(EXAMPLE_TOPIC_NAME)));

        deployNewTopic(CO_NAMESPACE, SECOND_NAMESPACE, EXAMPLE_TOPIC_NAME);
        deleteNewTopic(CO_NAMESPACE, EXAMPLE_TOPIC_NAME);
        cluster.setNamespace(CO_NAMESPACE);
    }

    /**
     * Test the case when Kafka will be deployed in different namespace than CO
     */
    @Test
    void testKafkaInDifferentNsThanClusterOperator() {
        LOGGER.info("Deploying Kafka in different namespace than CO when CO watches multiple namespaces");
        checkKafkaInDiffNamespaceThanCO(CLUSTER_NAME, SECOND_NAMESPACE);
    }

    /**
     * Test the case when MirrorMaker will be deployed in different namespace across multiple namespaces
     */
    @Test
    @Tag(MIRROR_MAKER)
    void testDeployMirrorMakerAcrossMultipleNamespace() {
        LOGGER.info("Deploying KafkaMirrorMaker in different namespace than CO when CO watches multiple namespaces");
        checkMirrorMakerForKafkaInDifNamespaceThanCO(CLUSTER_NAME);
    }

    @BeforeAll
    void setupEnvironment() {
        deployTestSpecificResources();
    }

    private void deployTestSpecificResources() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(CO_NAMESPACE, Arrays.asList(CO_NAMESPACE, SECOND_NAMESPACE));

        applyRoleBindings(CO_NAMESPACE);
        applyRoleBindings(CO_NAMESPACE, SECOND_NAMESPACE);
        // 060-Deployment
        BundleResource.clusterOperator(String.join(",", CO_NAMESPACE, SECOND_NAMESPACE)).done();

        cluster.setNamespace(SECOND_NAMESPACE);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editEntityOperator()
                    .editTopicOperator()
                        .withWatchedNamespace(CO_NAMESPACE)
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec().done();

        cluster.setNamespace(CO_NAMESPACE);
    }
}
