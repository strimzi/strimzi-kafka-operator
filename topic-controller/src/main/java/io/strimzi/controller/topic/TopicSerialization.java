/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import kafka.log.LogConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidTopicException;
import scala.collection.Iterator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.lang.String.format;

/**
 * Serialization of a {@link }Topic} to and from various other representations.
 */
public class TopicSerialization {

    // These are the keys in the ConfigMap data
    public static final String CM_KEY_PARTITIONS = "partitions";
    public static final String CM_KEY_REPLICAS = "replicas";
    public static final String CM_KEY_NAME = "name";
    public static final String CM_KEY_CONFIG = "config";

    // These are the keys in the JSON we store in ZK
    public static final String JSON_KEY_TOPIC_NAME = "topic-name";
    public static final String JSON_KEY_MAP_NAME = "map-name";
    public static final String JSON_KEY_PARTITIONS = "partitions";
    public static final String JSON_KEY_REPLICAS = "replicas";
    public static final String JSON_KEY_CONFIG = "config";

    private static Map<String, String> topicConfigFromConfigMapString(ConfigMap cm) {
        Map<String, String> mapData = cm.getData();
        String value = mapData.get(CM_KEY_CONFIG);
        Map<Object, Object> result;
        if (value == null || value.isEmpty()) {
            result = Collections.emptyMap();
        } else {
            try {
                ObjectMapper mapper = objectMapper();
                result = mapper.readValue(new StringReader(value) {
                    @Override
                    public String toString() {
                        return "'config' key of 'data' section of ConfigMap '" + cm.getMetadata().getName() + "' in namespace '" + cm.getMetadata().getNamespace() + "'";
                    }
                }, Map.class);
            } catch (IOException e) {
                throw new InvalidConfigMapException(cm, "ConfigMap's 'data' section has invalid key '" +
                        CM_KEY_CONFIG + "': " + (e.getMessage() != null ? e.getMessage() : e.toString()));
            }
        }
        Set<String> supportedConfigs = getSupportedTopicConfigs();
        for (Map.Entry<Object, Object> entry : result.entrySet()) {
            Object key = entry.getKey();
            String msg = null;
            if (!(key instanceof String)) {
                msg = "The must be of type String, not of type " + key.getClass();
            }
            Object v = entry.getValue();
            if (v == null) {
                msg = "The value corresponding to the key must have a String value, not null";
            } else if (!(v instanceof String)) {
                msg = "The value corresponding to the key must have a String value, not a value of type " + v.getClass();
            }
            if (!supportedConfigs.contains(key)) {
                msg = "The allowed configs keys are " + supportedConfigs;
            }
            if (msg != null) {
                throw new InvalidConfigMapException(cm, "ConfigMap's 'data' section has invalid key '" +
                        CM_KEY_CONFIG + "': The key '" + key + "' of the topic config is invalid: " + msg);
            }
        }
        return (Map) result;
    }

    private static Set<String> getSupportedTopicConfigs() {
        Set<String> supportedKeys = new TreeSet<>();
        Iterator<String> it = LogConfig.configNames().iterator();
        while (it.hasNext()) {
            supportedKeys.add(it.next());
        }
        return supportedKeys;
    }

    private static String topicConfigToConfigMapString(Map<String, String> config) throws IOException {
        ObjectMapper mapper = objectMapper();
        StringWriter sw = new StringWriter();
        mapper.writeValue(sw, config);
        return sw.toString();
    }

    /**
     * Create a Topic to reflect the given ConfigMap.
     * @throws InvalidConfigMapException
     */
    public static Topic fromConfigMap(ConfigMap cm) {
        if (cm == null) {
            return null;
        }
        Topic.Builder builder = new Topic.Builder()
                .withMapName(cm.getMetadata().getName())
                .withTopicName(getTopicName(cm))
                .withNumPartitions(getPartitions(cm))
                .withNumReplicas(getReplicas(cm))
                .withConfig(topicConfigFromConfigMapString(cm));
        return builder.build();
    }

    private static String getTopicName(ConfigMap cm) {
        Map<String, String> mapData = cm.getData();
        String prefix = "ConfigMap's 'data' section has invalid '" + CM_KEY_NAME + "' key: ";
        String topicName = mapData.get(CM_KEY_NAME);
        if (topicName == null) {
            topicName = cm.getMetadata().getName();
            prefix = "ConfigMap's 'data' section lacks a '" + CM_KEY_NAME + "' key and ConfigMap's name is invalid as a topic name: ";
        }
        try {
            org.apache.kafka.common.internals.Topic.validate(topicName);
        } catch (InvalidTopicException e) {
            throw new InvalidConfigMapException(cm, prefix + e.getMessage());
        }
        return topicName;
    }

    private static short getReplicas(ConfigMap cm) {
        Map<String, String> mapData = cm.getData();
        String str = mapData.get(CM_KEY_REPLICAS);
        if (str == null) {
            throw new InvalidConfigMapException(cm, "ConfigMap's 'data' section lacks required key '" +
                    CM_KEY_REPLICAS + "', which should be a strictly positive integer");
        }
        short num;
        try {
            num = Short.parseShort(str);
            if (num <= 0) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            throw new InvalidConfigMapException(cm, "ConfigMap's 'data' section has invalid key '" +
                    CM_KEY_REPLICAS + "': should be a strictly positive integer but was '" + str + "'");
        }
        return num;
    }

    private static int getPartitions(ConfigMap cm) {
        Map<String, String> mapData = cm.getData();
        String str = mapData.get(CM_KEY_PARTITIONS);
        if (str == null) {
            throw new InvalidConfigMapException(cm, "ConfigMap's 'data' section lacks required key '" +
                    CM_KEY_PARTITIONS + "', which should be a strictly positive integer");
        }
        int num;
        try {
            num = Integer.parseInt(str);
            if (num <= 0) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            throw new InvalidConfigMapException(null, "ConfigMap's 'data' section has invalid key '" +
                    CM_KEY_PARTITIONS + "': should be a strictly positive integer but was '" + str + "'");
        }
        return num;
    }

    /**
     * Create a ConfigMap to reflect the given Topic.
     */
    public static ConfigMap toConfigMap(Topic topic, LabelPredicate cmPredicate) {
        Map<String, String> mapData = new HashMap<>();
        mapData.put(CM_KEY_NAME, topic.getTopicName().toString());
        mapData.put(CM_KEY_PARTITIONS, Integer.toString(topic.getNumPartitions()));
        mapData.put(CM_KEY_REPLICAS, Short.toString(topic.getNumReplicas()));
        try {
            mapData.put(CM_KEY_CONFIG, topicConfigToConfigMapString(topic.getConfig()));
        } catch (IOException e) {
            throw new RuntimeException("Error converting topic config to a string, for topic '" + topic.getTopicName() + "'", e);
        }
        MapName mapName = topic.getOrAsMapName();
        return new ConfigMapBuilder().withApiVersion("v1")
                    .withNewMetadata()
                    .withName(mapName.toString())
                    .withLabels(cmPredicate.labels())
                    // TODO .withUid()
                .endMetadata()
                .withData(mapData)
                .build();
    }


    /**
     * Create a NewTopic to reflect the given Topic.
     */
    public static NewTopic toNewTopic(Topic topic, Map<Integer, List<Integer>> assignment) {
        NewTopic newTopic;
        if (assignment != null) {
            if (topic.getNumPartitions() != assignment.size()) {
                throw new IllegalArgumentException(
                        format("Topic %s has %d partitions supplied, but the number of partitions " +
                                        "configured in ConfigMap %s is %d",
                                topic.getTopicName(), assignment.size(), topic.getMapName(), topic.getNumPartitions()));
            }
            for (int partition = 0; partition < assignment.size(); partition++) {
                final List<Integer> value = assignment.get(partition);
                if (topic.getNumReplicas() != value.size()) {
                    throw new IllegalArgumentException(
                            format("Partition %d of topic %s has %d assigned replicas, " +
                                    "but the number of replicas configured in ConfigMap %s for the topic is %d",
                                    partition, topic.getTopicName(), value.size(), topic.getMapName(), topic.getNumReplicas()));
                }
            }
            newTopic = new NewTopic(topic.getTopicName().toString(), assignment);
        } else {
            newTopic = new NewTopic(topic.getTopicName().toString(), topic.getNumPartitions(), topic.getNumReplicas());
        }

        newTopic.configs(topic.getConfig());
        return newTopic;
    }

    /**
     * Return a singleton map from the topic {@link ConfigResource} for the given topic,
     * to the {@link Config} of the given topic.
     */
    public static Map<ConfigResource, Config> toTopicConfig(Topic topic) {
        Set<ConfigEntry> configEntries = new HashSet<>();
        for (Map.Entry<String, String> entry : topic.getConfig().entrySet()) {
            configEntries.add(new ConfigEntry(entry.getKey(), entry.getValue()));
        }
        Config config = new Config(configEntries);
        return Collections.singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, topic.getTopicName().toString()),
                config);
    }

    /**
     * Create a Topic to reflect the given TopicMetadata.
     */
    public static Topic fromTopicMetadata(TopicMetadata meta) {
        if (meta == null) {
            return null;
        }
        Topic.Builder builder = new Topic.Builder()
                .withTopicName(meta.getDescription().name())
                .withNumPartitions(meta.getDescription().partitions().size())
                .withNumReplicas((short) meta.getDescription().partitions().get(0).replicas().size());
        for (ConfigEntry entry: meta.getConfig().entries()) {
            if (!entry.isDefault()) {
                builder.withConfigEntry(entry.name(), entry.value());
            }
        }
        return builder.build();
    }

    /**
     * Returns the UTF-8 encoded JSON to reflect the given Topic.
     * This is what is stored in the znodes owned by the {@link ZkTopicStore}.
     */
    public static byte[] toJson(Topic topic) {
        ObjectMapper mapper = objectMapper();
        ObjectNode root = mapper.createObjectNode();
        // TODO Do we store the k8s uid here?
        root.put(JSON_KEY_MAP_NAME, topic.getOrAsMapName().toString());
        root.put(JSON_KEY_TOPIC_NAME, topic.getTopicName().toString());
        root.put(JSON_KEY_PARTITIONS, topic.getNumPartitions());
        root.put(JSON_KEY_REPLICAS, topic.getNumReplicas());

        ObjectNode config = mapper.createObjectNode();
        for (Map.Entry<String, String> entry : topic.getConfig().entrySet()) {
            config.put(entry.getKey(), entry.getValue());
        }
        root.set(JSON_KEY_CONFIG, config);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            mapper.writeValue(baos, root);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }

    /**
     * Returns the Topic represented by the given UTF-8 encoded JSON.
     * This is what is stored in the znodes owned by the {@link ZkTopicStore}.
     */
    public static Topic fromJson(byte[] json) {
        ObjectMapper mapper = objectMapper();
        Map<String, Object> root = null;
        try {
            root = mapper.readValue(json, Map.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Topic.Builder builder = new Topic.Builder();
        builder.withTopicName((String) root.get(JSON_KEY_TOPIC_NAME))
                .withMapName((String) root.get(JSON_KEY_MAP_NAME))
                .withNumPartitions((Integer) root.get(JSON_KEY_PARTITIONS))
                .withNumReplicas(((Integer) root.get(JSON_KEY_REPLICAS)).shortValue());
        Map<String, String> config = (Map) root.get(JSON_KEY_CONFIG);
        for (Map.Entry<String, String> entry : config.entrySet()) {
            builder.withConfigEntry(entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

    private static ObjectMapper objectMapper() {
        JsonFactory jf = new JsonFactory();
        return new ObjectMapper(jf);
    }

}
