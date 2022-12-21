/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static java.lang.String.format;

/**
 * Serialization of a {@link Topic} to and from various other representations.
 */
class TopicSerialization {

    // These are the keys in the JSON we store in ZK
    public static final String JSON_KEY_TOPIC_NAME = "topic-name";
    public static final String JSON_KEY_MAP_NAME = "map-name";
    public static final String JSON_KEY_PARTITIONS = "partitions";
    public static final String JSON_KEY_REPLICAS = "replicas";
    public static final String JSON_KEY_CONFIG = "config";

    @SuppressWarnings("unchecked")
    private static Map<String, String> topicConfigFromTopicConfig(KafkaTopic kafkaTopic) {
        if (kafkaTopic.getSpec().getConfig() != null) {
            Map<String, String> result = new HashMap<>(kafkaTopic.getSpec().getConfig().size());
            for (Map.Entry<String, Object> entry : kafkaTopic.getSpec().getConfig().entrySet()) {
                String key = entry.getKey();
                Object v = entry.getValue();
                boolean isNumberType = v instanceof Long
                        || v instanceof Integer
                        || v instanceof Short
                        || v instanceof Double
                        || v instanceof Float;
                if (v instanceof String
                        || isNumberType
                        || v instanceof Boolean) {
                    result.put(key, v.toString());
                } else {
                    String msg = "The value corresponding to the key must have a string, number or boolean value";
                    if (v == null) {
                        msg += " but the value was null";
                    } else {
                        msg += " but was of type " + v.getClass().getName();
                    }
                    throw new InvalidTopicException(kafkaTopic, "KafkaTopic's spec.config has invalid entry: " +
                            "The key '" + key + "' of the topic config is invalid: " + msg);
                }
            }
            return result;
        } else {
            return Collections.EMPTY_MAP;
        }
    }


    /**
     * Create a Topic to reflect the given KafkaTopic resource.
     * @throws InvalidTopicException
     */
    protected static Topic fromTopicResource(KafkaTopic kafkaTopic) {
        if (kafkaTopic == null) {
            return null;
        }
        Topic.Builder builder = new Topic.Builder()
                .withMapName(kafkaTopic.getMetadata().getName())
                .withTopicName(getTopicName(kafkaTopic))
                .withConfig(topicConfigFromTopicConfig(kafkaTopic))
                .withMetadata(kafkaTopic.getMetadata());

        if (kafkaTopic.getSpec().getPartitions() != null) {
            builder.withNumPartitions(getPartitions(kafkaTopic));
        }
        if (kafkaTopic.getSpec().getReplicas() != null) {
            builder.withNumReplicas(getReplicas(kafkaTopic));
        }
        return builder.build();
    }

    private static String getTopicName(KafkaTopic kafkaTopic) {
        String prefix = "KafkaTopics's spec.topicName property is invalid as a topic name: ";
        String topicName = kafkaTopic.getSpec().getTopicName();
        if (topicName == null) {
            topicName = kafkaTopic.getMetadata().getName();
            prefix = "KafkaTopics's spec.topicName property is absent and KafkaTopics's metadata.name is invalid as a topic name: ";
        }
        try {
            org.apache.kafka.common.internals.Topic.validate(topicName);
        } catch (org.apache.kafka.common.errors.InvalidTopicException e) {
            throw new InvalidTopicException(kafkaTopic, prefix + e.getMessage());
        }
        return topicName;
    }

    private static short getReplicas(KafkaTopic kafkaTopic) {
        int replicas = kafkaTopic.getSpec().getReplicas();
        if (replicas < 1 || replicas > Short.MAX_VALUE) {
            throw new InvalidTopicException(kafkaTopic, "KafkaTopic's spec.replicas should be between 1 and " + Short.MAX_VALUE + " inclusive");
        }
        return (short) replicas;
    }

    private static int getPartitions(KafkaTopic kafkaTopic) {
        int partitions = kafkaTopic.getSpec().getPartitions();
        if (partitions < 1) {
            throw new InvalidTopicException(kafkaTopic, "KafkaTopic's spec.partitions should be strictly greater than 0");
        }
        return partitions;
    }

    /**
     * Create a resource to reflect the given Topic.
     */
    protected static KafkaTopic toTopicResource(Topic topic, Labels labels) {
        ResourceName resourceName = topic.getOrAsKubeName();
        ObjectMeta om = topic.getMetadata();
        Map<String, String> lbls = new HashMap<>(labels.labels());
        if (om != null) {
            om.setName(resourceName.toString());
            if (topic.getMetadata().getLabels() != null)
                lbls.putAll(topic.getMetadata().getLabels());
            om.setLabels(lbls);
            om.setOwnerReferences(topic.getMetadata().getOwnerReferences());
            om.setAnnotations(topic.getMetadata().getAnnotations());
        } else {
            om = new ObjectMetaBuilder()
                    .withName(resourceName.toString())
                    .withLabels(lbls)
                    .build();
        }

        KafkaTopic kt = new KafkaTopicBuilder()
                .withMetadata(om)
                // TODO .withUid()
                .withNewSpec()
                    .withTopicName(topic.getTopicName().toString())
                    .withPartitions(topic.getNumPartitions())
                    .withReplicas((int) topic.getNumReplicas())
                    .withConfig(new LinkedHashMap<>(topic.getConfig()))
                .endSpec()
                .build();
        // for some reason when the `topic.getMetadata().getAnnotations()` is null
        // topic is created with annotations={} (empty map but should be null as well)
        if (topic.getMetadata() != null)
            kt.getMetadata().setAnnotations(topic.getMetadata().getAnnotations());
        return kt;
    }


    /**
     * Create a NewTopic to reflect the given Topic.
     */
    protected static NewTopic toNewTopic(Topic topic, Map<Integer, List<Integer>> assignment) {
        NewTopic newTopic;
        if (assignment != null) {
            if (topic.getNumPartitions() != assignment.size()) {
                throw new IllegalArgumentException(
                        format("Topic %s has %d partitions supplied, but the number of partitions " +
                                        "configured in KafkaTopic %s is %d",
                                topic.getTopicName(), assignment.size(), topic.getResourceName(), topic.getNumPartitions()));
            }
            for (int partition = 0; partition < assignment.size(); partition++) {
                final List<Integer> value = assignment.get(partition);
                if (topic.getNumReplicas() != value.size()) {
                    throw new IllegalArgumentException(
                            format("Partition %d of topic %s has %d assigned replicas, " +
                                    "but the number of replicas configured in KafkaTopic %s for the topic is %d",
                                    partition, topic.getTopicName(), value.size(), topic.getResourceName(), topic.getNumReplicas()));
                }
            }
            newTopic = new NewTopic(topic.getTopicName().toString(), assignment);
        } else {
            Optional<Integer> numPartitions = topic.getNumPartitions() == -1 ? Optional.empty() : Optional.of(topic.getNumPartitions());
            Optional<Short> numReplicas = topic.getNumReplicas() == -1 ? Optional.empty() : Optional.of(topic.getNumReplicas());
            newTopic = new NewTopic(topic.getTopicName().toString(), numPartitions, numReplicas);
        }

        newTopic.configs(topic.getConfig());
        return newTopic;
    }

    /**
     * Return a singleton map from the topic {@link ConfigResource} for the given topic,
     * to the {@link Config} of the given topic.
     * @return
     */
    public static Map<ConfigResource, Config> toTopicConfig(Topic topic) {
        List<ConfigEntry> entries = new ArrayList<>(topic.getConfig().size());

        for (Map.Entry<String, String> entry : topic.getConfig().entrySet()) {
            ConfigEntry configEntry = new ConfigEntry(entry.getKey(), entry.getValue());
            entries.add(configEntry);
        }

        return Collections.singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, topic.getTopicName().toString()),
                new Config(entries));
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
                .withNumReplicas((short) meta.getDescription().partitions().get(0).replicas().size())
                .withMetadata(null);
        for (ConfigEntry entry: meta.getConfig().entries()) {
            if (entry.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG
                && entry.source() != ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG) {
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
        return toBytes((mapper, root) -> {
            fillObjectNode(root, topic);
        });
    }

    /**
     * Write topic to JsonNode.
     *
     * @param topic the topic
     * @return topic as json node
     */
    public static JsonNode toJsonNode(Topic topic) {
        ObjectNode root = objectMapper().createObjectNode();
        fillObjectNode(root, topic);
        return root;
    }

    private static void fillObjectNode(ObjectNode root, Topic topic) {
        // TODO Do we store the k8s uid here?
        root.put(JSON_KEY_MAP_NAME, topic.getOrAsKubeName().toString());
        root.put(JSON_KEY_TOPIC_NAME, topic.getTopicName().toString());
        root.put(JSON_KEY_PARTITIONS, topic.getNumPartitions());
        root.put(JSON_KEY_REPLICAS, topic.getNumReplicas());

        ObjectNode config = objectMapper().createObjectNode();
        for (Map.Entry<String, String> entry : topic.getConfig().entrySet()) {
            config.put(entry.getKey(), entry.getValue());
        }
        root.set(JSON_KEY_CONFIG, config);
    }

    /**
     * Returns the Topic represented by the given UTF-8 encoded JSON.
     * This is what is stored in the znodes owned by the {@link ZkTopicStore}.
     */
    @SuppressWarnings("unchecked")
    protected static Topic fromJson(byte[] json) {
        return fromJson(json, (mapper, bytes) -> {
            Map<String, Object> root;
            try {
                root = mapper.readValue(json, Map.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return fromMap(root);
        });
    }

    /**
     * Read Topic from json node.
     *
     * @param root the map
     * @return topic from map
     */
    protected static Topic fromJsonNode(JsonNode root) {
        TypeReference<Map<String, Object>> ref = new TypeReference<>() {
        };
        Map<String, Object> map = objectMapper().convertValue(root, ref);
        return fromMap(map);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Topic fromMap(Map<String, Object> root) {
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

    static byte[] toBytes(BiConsumer<ObjectMapper, ObjectNode> consumer) {
        ObjectMapper mapper = objectMapper();
        ObjectNode root = mapper.createObjectNode();
        consumer.accept(mapper, root);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            mapper.writeValue(baos, root);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }

    static <T> T fromJson(byte[] json, BiFunction<ObjectMapper, byte[], T> fn) {
        ObjectMapper mapper = objectMapper();
        return fn.apply(mapper, json);
    }

    private static ObjectMapper objectMapper() {
        JsonFactory jf = new JsonFactory();
        jf.configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, false);
        return new ObjectMapper(jf);
    }

}
