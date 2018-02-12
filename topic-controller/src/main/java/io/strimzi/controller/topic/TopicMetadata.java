/*
 * Copyright 2018 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.controller.topic;

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
