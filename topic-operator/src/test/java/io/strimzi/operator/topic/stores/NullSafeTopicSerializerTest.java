/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.stores;

import io.strimzi.operator.topic.Topic;
import io.strimzi.operator.topic.TopicSerde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class NullSafeTopicSerializerTest {

    @Test
    void serialize() {
        Serializer<Topic> serializer = new NullSafeTopicSerializer();
        assertNull(serializer.serialize("topic", null), "Serializer should return null when passed a null");

        Topic topic = new Topic.Builder("topic2", 3, (short) 3, Map.of("compression.type", "ztd")).build();
        byte[] expected = new TopicSerde().serialize("topic", topic);
        assertArrayEquals(expected, serializer.serialize("topic", topic));
    }
}