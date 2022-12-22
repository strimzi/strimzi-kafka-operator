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

    /**
     * Method to serialize the topic into byte stream
     *
     * @param topic  The Kafka topic
     * @param data   Topic data
     * @return byte stream after serializing the topic data
     */
    @Override
    public byte[] serialize(String topic, Topic data) {
        return TopicSerialization.toJson(data);
    }

    /**
     * Method to deserialize the topic data into topic command
     *
     * @param data   Topic in byte stream
     * @return Topic after deserializing the topic byte stream
     */
    @Override
    public Topic deserialize(String topic, byte[] data) {
        return TopicSerialization.fromJson(data);
    }
}
