/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.apicurio.registry.utils.kafka.SelfSerde;

/**
 * Topic Kafka Serde
 */
public class TopicSerde extends SelfSerde<Topic> {
    @Override
    public byte[] serialize(String topic, Topic data) {
        return TopicSerialization.toJson(data);
    }

    @Override
    public Topic deserialize(String topic, byte[] data) {
        return TopicSerialization.fromJson(data);
    }
}
