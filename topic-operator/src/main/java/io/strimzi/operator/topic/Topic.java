/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.ObjectMeta;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** The Topic builder class */
public class Topic {

    /** Builder class used for building the topics */
    protected static class Builder {
        private TopicName topicName;
        private int numPartitions = -1;
        private short numReplicas = -1;
        private Map<String, String> config = new HashMap<>();
        private ObjectMeta metadata = new ObjectMeta();
        private ResourceName resourceName;

        protected Builder() {

        }

        protected Builder(String topicName, int numPartitions) {
            this(new TopicName(topicName), numPartitions, (short) -1, null, null);
        }

        protected Builder(TopicName topicName, int numPartitions) {
            this(topicName, numPartitions, (short) -1, null, null);
        }

        protected Builder(String topicName, int numPartitions, Map<String, String> config) {
            this(new TopicName(topicName), numPartitions, (short) -1, config, null);
        }

        protected Builder(TopicName topicName, int numPartitions, Map<String, String> config) {
            this(topicName, numPartitions, (short) -1, config, null);
        }

        protected Builder(String topicName, int numPartitions, short numReplicas, Map<String, String> config) {
            this(new TopicName(topicName), numPartitions, numReplicas, config, null);
        }

        protected Builder(TopicName topicName, int numPartitions, short numReplicas, Map<String, String> config) {
            this(topicName, topicName.asKubeName(), numPartitions, numReplicas, config, null);
        }

        protected Builder(String topicName, int numPartitions, short numReplicas, Map<String, String> config, ObjectMeta metadata) {
            this(new TopicName(topicName), numPartitions, numReplicas, config, metadata);
        }

        protected Builder(TopicName topicName, int numPartitions, short numReplicas, Map<String, String> config, ObjectMeta metadata) {
            this(topicName, topicName.asKubeName(), numPartitions, numReplicas, config, metadata);
        }

        protected Builder(String topicName, int numPartitions, Map<String, String> config, ObjectMeta metadata) {
            this(new TopicName(topicName), numPartitions, (short) -1, config, metadata);
        }

        protected Builder(TopicName topicName, int numPartitions, Map<String, String> config, ObjectMeta metadata) {
            this(topicName, numPartitions, (short) -1, config, metadata);
        }

        protected Builder(TopicName topicName, ResourceName resourceName, int numPartitions, short numReplicas, Map<String, String> config, ObjectMeta metadata) {
            this.topicName = topicName;
            this.resourceName = resourceName;
            this.numPartitions = numPartitions;
            this.numReplicas = numReplicas;
            if (config != null) {
                this.config.putAll(config);
            }
            this.metadata = metadata;
        }

        protected Builder(Topic topic) {
            this.topicName = topic.topicName;
            this.numPartitions = topic.numPartitions;
            this.numReplicas = topic.numReplicas;
            this.resourceName = topic.resourceName;
            this.config.putAll(topic.config);
            this.metadata = topic.metadata;
        }
        protected Builder withTopicName(String name) {
            this.topicName = new TopicName(name);
            return this;
        }

        protected Builder withMapName(ResourceName name) {
            this.resourceName = name;
            return this;
        }

        protected Builder withMapName(String name) {
            this.resourceName = new ResourceName(name);
            return this;
        }

        protected Builder withNumPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        protected Builder withNumReplicas(short numReplicas) {
            this.numReplicas = numReplicas;
            return this;
        }

        protected Builder withConfig(Map<String, String> config) {
            this.config.clear();
            this.config.putAll(config);
            return this;
        }

        protected Builder withMetadata(ObjectMeta metadata) {
            this.metadata = metadata;
            return this;
        }

        protected Builder withConfigEntry(String configKey, String configValue) {
            this.config.put(configKey, configValue);
            return this;
        }

        protected Builder withoutConfigEntry(String configKey) {
            this.config.remove(configKey);
            return this;
        }

        protected Topic build() {
            return new Topic(topicName, resourceName, numPartitions, numReplicas, config, metadata);
        }
    }

    private final TopicName topicName;

    private final ResourceName resourceName;

    private final int numPartitions;

    private final Map<String, String> config;

    private final ObjectMeta metadata;

    private final short numReplicas;

    protected TopicName getTopicName() {
        return topicName;
    }

    protected ResourceName getResourceName() {
        return resourceName;
    }

    protected ResourceName getOrAsKubeName() {
        if (resourceName != null) {
            return resourceName;
        } else {
            return topicName.asKubeName();
        }
    }

    protected int getNumPartitions() {
        return numPartitions;
    }

    protected short getNumReplicas() {
        return numReplicas;
    }

    protected Map<String, String> getConfig() {
        return config;
    }

    protected ObjectMeta getMetadata() {
        return metadata;
    }

    private Topic(TopicName topicName, ResourceName resourceName, int numPartitions, short numReplicas, Map<String, String> config, ObjectMeta metadata) {
        this.topicName = topicName;
        this.resourceName = resourceName;
        this.numPartitions = numPartitions;
        this.numReplicas = numReplicas;
        this.config = Collections.unmodifiableMap(config);
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "Topic{" +
                "name=" + topicName +
                ", numPartitions=" + numPartitions +
                ", numReplicas=" + numReplicas +
                ", config=" + config +
                ", metadata=" + metadata +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Topic topic = (Topic) o;

        if (numPartitions != topic.numPartitions) return false;
        if (numReplicas != topic.numReplicas) return false;
        if (!topicName.equals(topic.topicName)) return false;
        if (!config.equals(topic.config)) return false;
        if (metadata == null) {
            return topic.metadata == null;
        } else
            return metadata.equals(topic.metadata);
    }

    @Override
    public int hashCode() {
        int result = topicName.hashCode();
        result = 31 * result + numPartitions;
        result = 31 * result + numReplicas;
        result = 31 * result + config.hashCode();
        result = 31 * result + metadata.hashCode();
        return result;
    }
}
