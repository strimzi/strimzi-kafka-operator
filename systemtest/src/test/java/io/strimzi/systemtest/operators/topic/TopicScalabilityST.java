/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.SCALABILITY;

@Tag(SCALABILITY)
public class TopicScalabilityST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicScalabilityST.class);
    private static final int NUMBER_OF_TOPICS = 1000;
    private static final int SAMPLE_OFFSET = 50;
    static final String NAMESPACE = "topic-scale-cluster-test";

    @Test
    void testBigAmountOfTopicsCreatingViaK8s() {
        final String topicName = "topic-example";

        LOGGER.info("Creating topics via Kubernetes");
        for (int i = 0; i < NUMBER_OF_TOPICS; i++) {
            String currentTopic = topicName + i;
            LOGGER.debug("Creating {} topic", currentTopic);
            KafkaTopicResource.topicWithoutWait(KafkaTopicResource.defaultTopic(CLUSTER_NAME,
                currentTopic, 3, 1, 1).build());
        }

        for (int i = 0; i < NUMBER_OF_TOPICS; i = i + SAMPLE_OFFSET) {
            String currentTopic = topicName + i;
            LOGGER.debug("Verifying that {} topic CR has Ready status", currentTopic);

            KafkaTopicUtils.waitForKafkaTopicReady(currentTopic);
        }

        LOGGER.info("Verifying that we created {} topics", NUMBER_OF_TOPICS);

        KafkaTopicUtils.waitForKafkaTopicsCount(NUMBER_OF_TOPICS, CLUSTER_NAME);
    }

    void deployTestSpecificResources() {
        LOGGER.info("Deploying shared kafka across all test cases in {} namespace", NAMESPACE);
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        LOGGER.info("Deploying shared kafka across all test cases in {} namespace", NAMESPACE);
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();
    }

}
