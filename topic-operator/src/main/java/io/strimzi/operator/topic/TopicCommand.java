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

    /** Enum for type of Topic commands */
    public enum Type {

        /** Create Command */
        CREATE(0),

        /** Update Command */
        UPDATE(1),

        /** Delete Command */
        DELETE(2);

        // make sure ids are unique!
        private final int id;

        Type(int id) {
            this.id = id;
        }

        /**
         * @return  the id
         */
        public int getId() {
            return id;
        }

        /**
         * Returns the type of Topic command w.r.t id
         *
         * @param id   Id
         * @return Type of Topic command
         */
        public static Type fromId(int id) {
            return Arrays.stream(values())
                    .filter(t -> t.id == id)
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException("No such id: " + id));
        }
    }

    protected static final int CURRENT_VERSION = 1;

    private final String uuid;
    private final Type type;
    private final Topic topic;
    private final TopicName name;
    private final int version;

    private TopicCommand(Type type, Topic topic, TopicName name) {
        this(UUID.randomUUID().toString(), type, topic, name, CURRENT_VERSION);
    }

    protected TopicCommand(String uuid, Type type, Topic topic, TopicName name, int version) {
        this.uuid = uuid;
        this.type = type;
        this.topic = topic;
        this.name = name;
        this.version = version;
    }

    protected static TopicCommand create(Topic topic) {
        return new TopicCommand(Type.CREATE, topic, null);
    }

    protected static TopicCommand update(Topic topic) {
        return new TopicCommand(Type.UPDATE, topic, null);
    }

    protected static TopicCommand delete(TopicName name) {
        return new TopicCommand(Type.DELETE, null, name);
    }

    protected String getKey() {
        return name != null ? name.toString() : topic.getTopicName().toString();
    }

    protected String getUuid() {
        return uuid;
    }

    protected Type getType() {
        return type;
    }

    protected Topic getTopic() {
        return topic;
    }

    protected TopicName getName() {
        return name;
    }

    protected int getVersion() {
        return version;
    }
}
