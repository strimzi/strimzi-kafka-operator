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

package io.strimzi.controller.topic;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;

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
    public static final String JSON_KEY_PARTITIONS = "partitions";
    public static final String JSON_KEY_REPLICAS = "replicas";
    public static final String JSON_KEY_CONFIG = "config";

    private static Map<String, String> topicConfigFromConfigMapString(Map<String, String> mapData) throws IOException {
        String value = mapData.get(CM_KEY_CONFIG);
        if (value == null || value.isEmpty()) {
            return Collections.emptyMap();
        } else {
            YAMLFactory yf = new YAMLFactory();
            ObjectMapper mapper = new ObjectMapper(yf);
            Map<String, String> result = mapper.readValue(new StringReader(value), Map.class);
            return result;
        }
    }

    private static String topicConfigToConfigMapString(Map<String, String> config) throws IOException {
        YAMLFactory yf = new YAMLFactory();
        ObjectMapper mapper = new ObjectMapper(yf);
        StringWriter sw = new StringWriter();
        mapper.writeValue(sw, config);
        return sw.toString();
    }

    /**
     * Create a Topic to reflect the given ConfigMap.
     */
    public static Topic fromConfigMap(ConfigMap cm) {
        if (cm == null) {
            return null;
        }
        Map<String, String> mapData = cm.getData();
        String name = cm.getMetadata().getName();
        Topic.Builder builder = new Topic.Builder()
                .withMapName(cm.getMetadata().getName())
                .withTopicName(mapData.getOrDefault(CM_KEY_NAME, name))
                .withNumPartitions(Integer.parseInt(mapData.get(CM_KEY_PARTITIONS)))
                .withNumReplicas(Short.parseShort(mapData.get(CM_KEY_REPLICAS)));
        try {
            builder.withConfig(topicConfigFromConfigMapString(mapData));
        } catch (IOException e) {
            throw new RuntimeException("Error parsing key '"+ CM_KEY_CONFIG +"' of ConfigMap '"+ name +"'", e);
        }
        return builder.build();
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
            throw new RuntimeException("Error converting topic config to a string, for topic '"+topic.getTopicName()+"'", e);
        }
        MapName mapName = topic.getMapName();
        if (mapName == null) {
            mapName = topic.getTopicName().asMapName();
        }
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
                .withNumReplicas((short)meta.getDescription().partitions().get(0).replicas().size());
        for (ConfigEntry entry: meta.getConfig().entries()) {
            builder.withConfigEntry(entry.name(), entry.value());
        }
        return builder.build();
    }

    /**
     * Returns the UTF-8 encoded JSON to reflect the given Topic.
     * This is what is stored in the znodes owned by the {@link ZkTopicStore}.
     */
    public static byte[] toJson(Topic topic) {
        JsonFactory yf = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(yf);
        ObjectNode root = mapper.createObjectNode();
        // TODO Do we store the k8s name here too?
        // TODO Do we store the k9s uid here?
        // If so, both of those need to be properties of the Topic class
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
        JsonFactory yf = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(yf);
        Map<String, Object> root = null;
        try {
            root = mapper.readValue(json, Map.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Topic.Builder builder = new Topic.Builder();
        builder.withTopicName((String)root.get(JSON_KEY_TOPIC_NAME))
        .withNumPartitions((Integer)root.get(JSON_KEY_PARTITIONS))
        .withNumReplicas(((Integer)root.get(JSON_KEY_REPLICAS)).shortValue());
        Map<String, String> config = (Map)root.get(JSON_KEY_CONFIG);
        for (Map.Entry<String, String> entry : config.entrySet()) {
            builder.withConfigEntry(entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

}
