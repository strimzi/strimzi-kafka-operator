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

import io.fabric8.kubernetes.api.model.ConfigMap;
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
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TopicSerializationTest {

    private final LabelPredicate cmPredicate = new LabelPredicate("kind", "topic",
            "app", "barnabas");

    @Test
    public void testConfigMapSerializationRoundTrip() {

        Topic.Builder builder = new Topic.Builder();
        builder.withTopicName("tom");
        builder.withNumReplicas((short) 1);
        builder.withNumPartitions(2);
        builder.withConfigEntry("foo", "bar");
        Topic wroteTopic = builder.build();
        ConfigMap cm = TopicSerialization.toConfigMap(wroteTopic, cmPredicate);

        assertEquals(wroteTopic.getTopicName().toString(), cm.getMetadata().getName());
        assertEquals(2, cm.getMetadata().getLabels().size());
        assertEquals("barnabas", cm.getMetadata().getLabels().get("app"));
        assertEquals("topic", cm.getMetadata().getLabels().get("kind"));
        assertEquals(wroteTopic.getTopicName().toString(), cm.getData().get(TopicSerialization.CM_KEY_NAME));
        assertEquals("2", cm.getData().get(TopicSerialization.CM_KEY_PARTITIONS));
        assertEquals("1", cm.getData().get(TopicSerialization.CM_KEY_REPLICAS));
        assertEquals("{\"foo\":\"bar\"}" , cm.getData().get(TopicSerialization.CM_KEY_CONFIG));

        Topic readTopic = TopicSerialization.fromConfigMap(cm);
        assertEquals(wroteTopic, readTopic);
    }


    @Test
    public void testJsonSerializationRoundTrip() throws UnsupportedEncodingException {
        Topic.Builder builder = new Topic.Builder();
        builder.withTopicName("tom");
        builder.withNumReplicas((short) 1);
        builder.withNumPartitions(2);
        builder.withConfigEntry("foo", "bar");
        Topic wroteTopic = builder.build();
        byte[] bytes = TopicSerialization.toJson(wroteTopic);
        String json = new String(bytes, "UTF-8");
        assertEquals("{\"topic-name\":\"tom\"," +
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
                .withNumReplicas((short)2)
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
        CreateTopicsRequest.TopicDetails details = (CreateTopicsRequest.TopicDetails)m.invoke(newTopic);
        assertEquals(singletonMap("foo", "bar"), details.configs);
    }

    @Test
    public void testToTopicConfig() {
        Topic topic = new Topic.Builder()
                .withTopicName("test-topic")
                .withConfigEntry("foo", "bar")
                .withNumPartitions(3)
                .withNumReplicas((short)2)
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

}
