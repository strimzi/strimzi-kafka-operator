/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

import io.strimzi.api.kafka.model.topic.KafkaTopic;

/**
 * Utility class.
 */
public class TopicOperatorUtil {
    private TopicOperatorUtil() {
    }

    /**
     * Get the topic name from a {@link KafkaTopic} resource.
     * 
     * @param kafkaTopic Topic resource.
     * @return Topic name.
     */
    public static String topicName(KafkaTopic kafkaTopic) {
        String tn = null;
        if (kafkaTopic.getSpec() != null) {
            tn = kafkaTopic.getSpec().getTopicName();
        }
        if (tn == null) {
            tn = kafkaTopic.getMetadata().getName();
        }
        return tn;
    }
}
