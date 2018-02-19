/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TopicSerializationTest {

    private final LabelPredicate cmPredicate = new LabelPredicate("kind", "topic",
            "app", "strimzi");

    @Test
    public void testConfigMapSerializationRoundTrip() {

        Topic.Builder builder = new Topic.Builder();
        builder.withTopicName("tom");
        builder.withNumReplicas((short) 1);
        builder.withNumPartitions(2);
        builder.withConfigEntry("cleanup.policy", "bar");
        Topic wroteTopic = builder.build();
        ConfigMap cm = TopicSerialization.toConfigMap(wroteTopic, cmPredicate);

        assertEquals(wroteTopic.getTopicName().toString(), cm.getMetadata().getName());
        assertEquals(2, cm.getMetadata().getLabels().size());
        assertEquals("strimzi", cm.getMetadata().getLabels().get("app"));
        assertEquals("topic", cm.getMetadata().getLabels().get("kind"));
        assertEquals(wroteTopic.getTopicName().toString(), cm.getData().get(TopicSerialization.CM_KEY_NAME));
        assertEquals("2", cm.getData().get(TopicSerialization.CM_KEY_PARTITIONS));
        assertEquals("1", cm.getData().get(TopicSerialization.CM_KEY_REPLICAS));
        assertEquals("{\"cleanup.policy\":\"bar\"}", cm.getData().get(TopicSerialization.CM_KEY_CONFIG));

        Topic readTopic = TopicSerialization.fromConfigMap(cm);
        assertEquals(wroteTopic, readTopic);
    }


    @Test
    public void testJsonSerializationRoundTrip() throws UnsupportedEncodingException {
        Topic.Builder builder = new Topic.Builder();
        builder.withTopicName("tom");
        builder.withMapName("bob");
        builder.withNumReplicas((short) 1);
        builder.withNumPartitions(2);
        builder.withConfigEntry("foo", "bar");
        Topic wroteTopic = builder.build();
        byte[] bytes = TopicSerialization.toJson(wroteTopic);
        String json = new String(bytes, "UTF-8");
        assertEquals("{\"map-name\":\"bob\"," +
                "\"topic-name\":\"tom\"," +
                "\"partitions\":2," +
                "\"replicas\":1," +
                "\"config\":{\"foo\":\"bar\"}" +
                "}", json);
        Topic readTopic = TopicSerialization.fromJson(bytes);
        assertEquals(wroteTopic, readTopic);
    }


    @Test
    public void testToNewTopic() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Topic topic = new Topic.Builder()
                .withTopicName("test-topic")
                .withConfigEntry("foo", "bar")
                .withNumPartitions(3)
                .withNumReplicas((short) 2)
                .withMapName("gee")
                .build();
        NewTopic newTopic = TopicSerialization.toNewTopic(topic, null);
        assertEquals("test-topic", newTopic.name());
        assertEquals(3, newTopic.numPartitions());
        assertEquals(2, newTopic.replicationFactor());
        assertEquals(null, newTopic.replicasAssignments());
        // For some reason Kafka doesn't provide an accessor for the config
        // and only provides a package-access method to convert to topic details
        Method m = NewTopic.class.getDeclaredMethod("convertToTopicDetails");
        m.setAccessible(true);
        CreateTopicsRequest.TopicDetails details = (CreateTopicsRequest.TopicDetails) m.invoke(newTopic);
        assertEquals(singletonMap("foo", "bar"), details.configs);
    }

    @Test
    public void testToTopicConfig() {
        Topic topic = new Topic.Builder()
                .withTopicName("test-topic")
                .withConfigEntry("foo", "bar")
                .withNumPartitions(3)
                .withNumReplicas((short) 2)
                .withMapName("gee")
                .build();
        Map<ConfigResource, Config> config = TopicSerialization.toTopicConfig(topic);
        assertEquals(1, config.size());
        Map.Entry<ConfigResource, Config> c = config.entrySet().iterator().next();
        assertEquals(c.getKey().type(), ConfigResource.Type.TOPIC);
        assertEquals(c.getKey().name(), "test-topic");
        assertEquals(1, c.getValue().entries().size());
        assertEquals("foo", c.getValue().get("foo").name());
        assertEquals("bar", c.getValue().get("foo").value());

    }

    @Test
    public void testFromTopicMetadata() {
        Node node0 = new Node(0, "host0", 1234);
        Node node1 = new Node(1, "host1", 1234);
        Node node2 = new Node(1, "host2", 1234);
        List<Node> nodes02 = asList(node0, node1, node2);
        TopicDescription desc = new TopicDescription("test-topic", false,
                asList(
                        new TopicPartitionInfo(0, node0, asList(node0, node1, node2), nodes02),
                        new TopicPartitionInfo(1, node0, asList(node0, node1, node2), nodes02)));
        List<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry("foo", "bar"));
        Config topicConfig = new Config(entries);
        TopicMetadata meta = new TopicMetadata(desc, topicConfig);
        Topic topic = TopicSerialization.fromTopicMetadata(meta);
        assertEquals(new TopicName("test-topic"), topic.getTopicName());
        // Null map name because Kafka doesn't know about the map
        assertNull(topic.getMapName());
        assertEquals(singletonMap("foo", "bar"), topic.getConfig());
        assertEquals(2, topic.getNumPartitions());
        assertEquals(3, topic.getNumReplicas());
    }

    @Test
    public void testErrorInDefaultTopicName() {
        Map<String, String> data = new HashMap<>();
        data.put(TopicSerialization.CM_KEY_REPLICAS, "1");
        data.put(TopicSerialization.CM_KEY_PARTITIONS, "1");
        data.put(TopicSerialization.CM_KEY_CONFIG, "{}");

        // The problem with this configmap name is it's too long to be a legal topic name
        String illegalAsATopicName = "012345678901234567890123456789012345678901234567890123456789" +
                "01234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                "01234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                "012345678901234567890123456789";
        ConfigMap cm = new ConfigMapBuilder().editOrNewMetadata().withName(illegalAsATopicName)
                .endMetadata().withData(data).build();

        try {
            TopicSerialization.fromConfigMap(cm);
            fail("Should throw");
        } catch (InvalidConfigMapException e) {
            assertEquals("ConfigMap's 'data' section lacks a 'name' key and ConfigMap's name is invalid as a topic name: " +
                    "Topic name is illegal, it can't be longer than 249 characters, topic name: " +
                    illegalAsATopicName, e.getMessage());
        }
    }

    @Test
    public void testErrorInTopicName() {
        Map<String, String> data = new HashMap<>();
        data.put(TopicSerialization.CM_KEY_REPLICAS, "1");
        data.put(TopicSerialization.CM_KEY_PARTITIONS, "1");
        data.put(TopicSerialization.CM_KEY_CONFIG, "{}");
        data.put(TopicSerialization.CM_KEY_NAME, "An invalid topic name!");

        ConfigMap cm = new ConfigMapBuilder().editOrNewMetadata().withName("foo")
                .endMetadata().withData(data).build();

        try {
            TopicSerialization.fromConfigMap(cm);
            fail("Should throw");
        } catch (InvalidConfigMapException e) {
            assertEquals("ConfigMap's 'data' section has invalid 'name' key: Topic name \"An invalid topic name!\" is illegal, it contains a character other than ASCII alphanumerics, '.', '_' and '-'", e.getMessage());
        }
    }

    @Test
    public void testErrorInPartitions() {
        Map<String, String> data = new HashMap<>();
        data.put(TopicSerialization.CM_KEY_REPLICAS, "1");
        data.put(TopicSerialization.CM_KEY_PARTITIONS, "foo");
        data.put(TopicSerialization.CM_KEY_CONFIG, "{}");

        ConfigMap cm = new ConfigMapBuilder().editOrNewMetadata().withName("my-topic")
                .endMetadata().withData(data).build();

        try {
            TopicSerialization.fromConfigMap(cm);
            fail("Should throw");
        } catch (InvalidConfigMapException e) {
            assertEquals("ConfigMap's 'data' section has invalid key 'partitions': should be a strictly positive integer but was 'foo'", e.getMessage());
        }
    }

    @Test
    public void testErrorInReplicas() {
        Map<String, String> data = new HashMap<>();
        data.put(TopicSerialization.CM_KEY_REPLICAS, "foo");
        data.put(TopicSerialization.CM_KEY_PARTITIONS, "1");
        data.put(TopicSerialization.CM_KEY_CONFIG, "{}");

        ConfigMap cm = new ConfigMapBuilder().editOrNewMetadata().withName("my-topic")
                .endMetadata().withData(data).build();

        try {
            TopicSerialization.fromConfigMap(cm);
            fail("Should throw");
        } catch (InvalidConfigMapException e) {
            assertEquals("ConfigMap's 'data' section has invalid key 'replicas': should be a strictly positive integer but was 'foo'", e.getMessage());
        }
    }

    @Test
    public void testErrorInConfigNotJson() {
        Map<String, String> data = new HashMap<>();
        data.put(TopicSerialization.CM_KEY_REPLICAS, "1");
        data.put(TopicSerialization.CM_KEY_PARTITIONS, "1");
        data.put(TopicSerialization.CM_KEY_CONFIG, "foobar");

        ConfigMap cm = new ConfigMapBuilder().editOrNewMetadata().withName("my-topic")
                .endMetadata().withData(data).build();

        try {
            TopicSerialization.fromConfigMap(cm);
            fail("Should throw");
        } catch (InvalidConfigMapException e) {
            assertEquals("ConfigMap's 'data' section has invalid key 'config': " +
                    "Unrecognized token 'foobar': was expecting 'null', 'true', 'false' or NaN\n" +
                    " at [Source: 'config' key of 'data' section of " +
                    "ConfigMap 'my-topic' in namespace 'null'; line: 1, column: 13]",
                    e.getMessage());
        }
    }

    @Test
    public void testErrorInConfigInvalidKey() {
        Map<String, String> data = new HashMap<>();
        data.put(TopicSerialization.CM_KEY_REPLICAS, "1");
        data.put(TopicSerialization.CM_KEY_PARTITIONS, "1");
        data.put(TopicSerialization.CM_KEY_CONFIG, "{\"foo\":\"bar\"}");

        ConfigMap cm = new ConfigMapBuilder().editOrNewMetadata().withName("my-topic")
                .endMetadata().withData(data).build();

        try {
            TopicSerialization.fromConfigMap(cm);
            fail("Should throw");
        } catch (InvalidConfigMapException e) {
            assertTrue(e.getMessage().startsWith("ConfigMap's 'data' section has invalid key 'config': " +
                            "The key 'foo' of the topic config is invalid: " +
                            "The allowed configs keys are ["));
        }
    }

    @Test
    public void testErrorInConfigInvalidKeyNull() {
        Map<String, String> data = new HashMap<>();
        data.put(TopicSerialization.CM_KEY_REPLICAS, "1");
        data.put(TopicSerialization.CM_KEY_PARTITIONS, "1");
        data.put(TopicSerialization.CM_KEY_CONFIG, "{null:null}");

        ConfigMap cm = new ConfigMapBuilder().editOrNewMetadata().withName("my-topic")
                .endMetadata().withData(data).build();
        try {
            TopicSerialization.fromConfigMap(cm);
            fail("Should throw");
        } catch (InvalidConfigMapException e) {
            assertEquals("ConfigMap's 'data' section has invalid key 'config': " +
                            "Unexpected character ('n' (code 110)): was expecting double-quote to start field name\n" +
                            " at [Source: 'config' key of 'data' section of ConfigMap 'my-topic' in namespace 'null'; line: 1, column: 3]",
                    e.getMessage());
        }
    }

    @Test
    public void testErrorInConfigInvalidValueWrongType() {
        Map<String, String> data = new HashMap<>();
        data.put(TopicSerialization.CM_KEY_REPLICAS, "1");
        data.put(TopicSerialization.CM_KEY_PARTITIONS, "1");
        data.put(TopicSerialization.CM_KEY_CONFIG, "{\"cleanup.policy\":1}");

        ConfigMap cm = new ConfigMapBuilder().editOrNewMetadata().withName("my-topic")
                .endMetadata().withData(data).build();

        try {
            TopicSerialization.fromConfigMap(cm);
            fail("Should throw");
        } catch (InvalidConfigMapException e) {
            assertEquals("ConfigMap's 'data' section has invalid key 'config': " +
                    "The key 'cleanup.policy' of the topic config is invalid: " +
                    "The value corresponding to the key must have a String value, " +
                    "not a value of type class java.lang.Integer",
                    e.getMessage());
        }
    }

    @Test
    public void testErrorInConfigInvalidValueNull() {
        Map<String, String> data = new HashMap<>();
        data.put(TopicSerialization.CM_KEY_REPLICAS, "1");
        data.put(TopicSerialization.CM_KEY_PARTITIONS, "1");
        data.put(TopicSerialization.CM_KEY_CONFIG, "{\"cleanup.policy\":null}");

        ConfigMap cm = new ConfigMapBuilder().editOrNewMetadata().withName("my-topic")
                .endMetadata().withData(data).build();

        try {
            TopicSerialization.fromConfigMap(cm);
            fail("Should throw");
        } catch (InvalidConfigMapException e) {
            assertEquals("ConfigMap's 'data' section has invalid key 'config': " +
                    "The key 'cleanup.policy' of the topic config is invalid: " +
                    "The value corresponding to the key must have a String value, not null",
                    e.getMessage());
        }
    }

}

