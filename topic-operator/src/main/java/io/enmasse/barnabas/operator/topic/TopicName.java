/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.enmasse.barnabas.operator.topic;

import io.fabric8.kubernetes.api.model.ConfigMap;
import org.apache.kafka.common.internals.Topic;

/**
 * Typesafe representation of the name of a topic.
 */
class TopicName {
    private final String name;

    public TopicName(String name) {
        assert(name != null && !name.isEmpty());
        // TODO Shame we can't validate a topic name without relying on an internal class
        Topic.validate(name);
        this.name = name;
    }

    public TopicName(ConfigMap cm) {
        this(cm.getData().getOrDefault("name", cm.getMetadata().getName()));
    }

    public String toString() {
        return this.name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicName topicName = (TopicName) o;

        return name.equals(topicName.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public MapName asMapName() {
        // TODO sanitize name
        return new MapName(this.name);
    }
}
