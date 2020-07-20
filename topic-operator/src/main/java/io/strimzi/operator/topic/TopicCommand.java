/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import java.util.Arrays;
import java.util.UUID;

/**
 * Command to send to Kafka Streams topology.
 */
public class TopicCommand {

    public enum Type {
        CREATE(0),
        UPDATE(1),
        DELETE(2);

        // make sure ids are unique!
        private final int id;

        Type(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public static Type fromId(int id) {
            return Arrays.stream(values())
                    .filter(t -> t.id == id)
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException("No such id: " + id));
        }
    }

    public static final int CURRENT_VERSION = 1;

    private final String uuid;
    private final Type type;
    private final Topic topic;
    private final TopicName name;
    private final int version;

    private TopicCommand(Type type, Topic topic, TopicName name) {
        this(UUID.randomUUID().toString(), type, topic, name, CURRENT_VERSION);
    }

    public TopicCommand(String uuid, Type type, Topic topic, TopicName name, int version) {
        this.uuid = uuid;
        this.type = type;
        this.topic = topic;
        this.name = name;
        this.version = version;
    }

    public static TopicCommand create(Topic topic) {
        return new TopicCommand(Type.CREATE, topic, null);
    }

    public static TopicCommand update(Topic topic) {
        return new TopicCommand(Type.UPDATE, topic, null);
    }

    public static TopicCommand delete(TopicName name) {
        return new TopicCommand(Type.DELETE, null, name);
    }

    public String getKey() {
        return name != null ? name.toString() : topic.getTopicName().toString();
    }

    public String getUuid() {
        return uuid;
    }

    public Type getType() {
        return type;
    }

    public Topic getTopic() {
        return topic;
    }

    public TopicName getName() {
        return name;
    }

    public int getVersion() {
        return version;
    }
}
