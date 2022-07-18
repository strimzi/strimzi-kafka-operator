/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.stores;

import io.strimzi.operator.topic.Topic;
import io.strimzi.operator.topic.TopicSerde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Passes through nulls safely to allow for tombstone records (that is, deletion)
 */
public class NullSafeTopicSerializer implements Serializer<Topic> {

    private final Serializer<Topic> wrappedSerializer;

    public NullSafeTopicSerializer() {
        wrappedSerializer = new TopicSerde().serializer();
    }

    @Override
    public byte[] serialize(String topic, Topic data) {
        if (data == null) {
            return new byte[0];
        }
        return wrappedSerializer.serialize(topic, data);
    }
}