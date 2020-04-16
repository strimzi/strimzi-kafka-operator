/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.topic;

import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.SCALABILITY;

@Tag(SCALABILITY)
public class TopicScalabilityST extends TopicST {

    private static final Logger LOGGER = LogManager.getLogger(TopicScalabilityST.class);
    private static final int NUMBER_OF_TOPICS = 2500;

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

        LOGGER.info("Verifying that we created {} topics", NUMBER_OF_TOPICS);

        KafkaTopicUtils.waitForKafkaTopicsCount(NUMBER_OF_TOPICS, CLUSTER_NAME);
    }
}
