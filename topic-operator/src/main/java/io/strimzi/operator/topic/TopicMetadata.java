/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;

/**
 * Pairs a {@code org.apache.kafka.clients.admin.TopicDescription} with a
 * topic {@code org.apache.kafka.clients.admin.Config}, to capture
 * complete information about a Kafka topic.
 * This is necessary because the Kafka AdminClient doesn't have an API for
 * getting this information in one go.
 */
public class TopicMetadata {
    private final Config config;
    private final TopicDescription description;

    public TopicMetadata(TopicDescription description, Config config) {
        this.config = config;
        this.description = description;
    }

    public Config getConfig() {
        return config;
    }

    public TopicDescription getDescription() {
        return description;
    }
}
