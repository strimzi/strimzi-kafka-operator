/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import com.fasterxml.jackson.databind.JsonNode;
import io.apicurio.registry.utils.kafka.SelfSerde;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * TopicCommand Kafka Serde
 */
public class TopicCommandSerde extends SelfSerde<TopicCommand> {

    private static final String UUID = "uuid";
    private static final String TYPE = "type";
    private static final String TOPIC = "topic";
    private static final String KEY = "key";
    private static final String VERSION = "version";

    /**
     * Method to serialize the topic command data into byte stream
     *
     * @param topic  The Kafka topic
     * @param data   Topic command
     * @return byte stream after serializing the topic command
     */
    @Override
    public byte[] serialize(String topic, TopicCommand data) {
        return TopicSerialization.toBytes((mapper, root) -> {
            root.put(UUID, data.getUuid());
            TopicCommand.Type type = data.getType();
            root.put(VERSION, data.getVersion());
            root.put(TYPE, type.getId());
            if (type == TopicCommand.Type.CREATE || type == TopicCommand.Type.UPDATE) {
                JsonNode json = TopicSerialization.toJsonNode(data.getTopic());
                root.set(TOPIC, json);
            } else {
                root.put(KEY, data.getKey());
            }
        });
    }

    /**
     * Method to deserialize the topic data into topic command
     *
     * @param data   Topic command in byte stream
     * @return Topic command after deserializing the topic command byte stream
     */
    @Override
    public TopicCommand deserialize(String t, byte[] data) {
        return TopicSerialization.fromJson(data, (mapper, bytes) -> {
            try {
                JsonNode root = mapper.readTree(bytes);
                String uuid = root.get(UUID).asText();
                int id = root.get(TYPE).asInt();
                TopicCommand.Type type = TopicCommand.Type.fromId(id);
                Topic topic = null;
                TopicName name = null;
                if (type == TopicCommand.Type.CREATE || type == TopicCommand.Type.UPDATE) {
                    JsonNode json = root.get(TOPIC);
                    topic = TopicSerialization.fromJsonNode(json);
                } else {
                    name = new TopicName(root.get(KEY).asText());
                }
                int version = root.get(VERSION).asInt();
                return new TopicCommand(uuid, type, topic, name, version);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
}
