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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Topic {

    public static class Builder {
        private TopicName name;
        private int numPartitions;
        private Map<String, String> config = new HashMap<>();

        public Builder() {

        }

        public Builder(String name, int numPartitions) {
            this(new TopicName(name), numPartitions, null);
        }

        public Builder(TopicName name, int numPartitions) {
            this(name, numPartitions, null);
        }

        public Builder(String name, int numPartitions, Map<String, String> config) {
            this(new TopicName(name), numPartitions, config);
        }

        public Builder(TopicName name, int numPartitions, Map<String, String> config) {
            this.name = name;
            this.numPartitions = numPartitions;
            if (config != null) {
                this.config.putAll(config);
            }
        }

        public Builder(Topic topic) {
            this.name = topic.name;
            this.numPartitions = topic.numPartitions;
            this.config.putAll(topic.config);
        }

        public Builder withName(TopicName name) {
            this.name = name;
            return this;
        }

        public Builder withName(String name) {
            this.name = new TopicName(name);
            return this;
        }

        public Builder withNumPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        public Builder withConfig(Map<String, String> config) {
            this.config.clear();
            this.config.putAll(config);
            return this;
        }

        public Builder withConfigEntry(String configKey, String configValue) {
            this.config.put(configKey, configValue);
            return this;
        }

        public Builder withoutConfigEntry(String configKey) {
            this.config.remove(configKey);
            return this;
        }

        public Topic build() {
            return new Topic(name, numPartitions, config);
        }
    }

    private final TopicName name;

    private final int numPartitions;

    private final Map<String, String> config;

    public TopicName getName() {
        return name;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    private Topic(TopicName name, int numPartitions, Map<String, String> config) {

        this.name = name;
        this.numPartitions = numPartitions;
        this.config = Collections.unmodifiableMap(config);
    }

    @Override
    public String toString() {
        return "Topic{" +
                "name=" + name +
                ", numPartitions=" + numPartitions +
                ", config=" + config +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Topic topic = (Topic) o;

        if (numPartitions != topic.numPartitions) return false;
        if (!name.equals(topic.name)) return false;
        return config.equals(topic.config);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + numPartitions;
        result = 31 * result + config.hashCode();
        return result;
    }
}
