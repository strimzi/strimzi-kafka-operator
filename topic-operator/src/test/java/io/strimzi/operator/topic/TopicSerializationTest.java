/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.KafkaTopicSpec;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TopicSerializationTest {

    private final Labels labels = new Labels(
            "app", "strimzi");

    @Test
    public void testResourceSerializationRoundTrip() {

        String topicName = "tom";
        Topic.Builder builder = new Topic.Builder();
        builder.withTopicName(topicName);
        builder.withNumReplicas((short) 1);
        builder.withNumPartitions(2);
        builder.withConfigEntry("cleanup.policy", "bar");
        ObjectMeta metadata = new ObjectMeta();
        metadata.setAnnotations(new HashMap<>());
        builder.withMetadata(metadata);
        Topic wroteTopic = builder.build();
        KafkaTopic kafkaTopic = TopicSerialization.toTopicResource(wroteTopic, labels);

        assertEquals(wroteTopic.getTopicName().toString(), kafkaTopic.getMetadata().getName());
        assertEquals(4, kafkaTopic.getMetadata().getLabels().size());
        assertEquals("strimzi", kafkaTopic.getMetadata().getLabels().get("app"));
        assertEquals(
            io.strimzi.operator.common.model.Labels.KUBERNETES_NAME,
            kafkaTopic.getMetadata().getLabels().get(io.strimzi.operator.common.model.Labels.KUBERNETES_NAME_LABEL));
        assertEquals(topicName, kafkaTopic.getMetadata().getLabels().get(io.strimzi.operator.common.model.Labels.KUBERNETES_INSTANCE_LABEL));
        assertEquals(TopicOperator.KAFKA_TOPIC_OPERATOR_NAME, kafkaTopic.getMetadata().getLabels().get(io.strimzi.operator.common.model.Labels.KUBERNETES_MANAGED_BY_LABEL));
        assertEquals(wroteTopic.getTopicName().toString(), kafkaTopic.getSpec().getTopicName());
        assertEquals(Integer.valueOf(2), kafkaTopic.getSpec().getPartitions());
        assertEquals(Integer.valueOf(1), kafkaTopic.getSpec().getReplicas());
        assertEquals(singletonMap("cleanup.policy", "bar"), kafkaTopic.getSpec().getConfig());

        Topic readTopic = TopicSerialization.fromTopicResource(kafkaTopic);
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
    public void testToNewTopic() {
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
        assertEquals(singletonMap("foo", "bar"), newTopic.configs());
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
        Map<ConfigResource, Collection<AlterConfigOp>> config = TopicSerialization.toTopicConfig(topic);
        assertEquals(1, config.size());
        Map.Entry<ConfigResource, Collection<AlterConfigOp>> c = config.entrySet().iterator().next();
        assertEquals(c.getKey().type(), ConfigResource.Type.TOPIC);
        assertEquals(c.getKey().name(), "test-topic");
        assertEquals(1, c.getValue().size());
        AlterConfigOp alterConfigOp = c.getValue().iterator().next();
        assertEquals("foo", alterConfigOp.configEntry().name());
        assertEquals("bar", alterConfigOp.configEntry().value());
        assertEquals(AlterConfigOp.OpType.SET, alterConfigOp.opType());
    }

    @Test
    public void testFromTopicMetadata() {
        List<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry("foo", "bar"));
        Config topicConfig = new Config(entries);
        TopicMetadata meta = Utils.getTopicMetadata("test-topic", topicConfig);
        Topic topic = TopicSerialization.fromTopicMetadata(meta);
        assertEquals(new TopicName("test-topic"), topic.getTopicName());
        // Null map name because Kafka doesn't know about the map
        assertNull(topic.getResourceName());
        assertEquals(singletonMap("foo", "bar"), topic.getConfig());
        assertEquals(2, topic.getNumPartitions());
        assertEquals(3, topic.getNumReplicas());
    }

    @Test
    public void testErrorInDefaultTopicName() {

        // The problem with this resource name is it's too long to be a legal topic name
        String illegalAsATopicName = "012345678901234567890123456789012345678901234567890123456789" +
                "01234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                "01234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                "012345678901234567890123456789";
        KafkaTopic kafkaTopic = new KafkaTopicBuilder().withMetadata(new ObjectMetaBuilder().withName(illegalAsATopicName)
                .build()).withNewSpec()
                    .withReplicas(1)
                    .withPartitions(1)
                    .withConfig(emptyMap())
                .endSpec()
            .build();

        try {
            TopicSerialization.fromTopicResource(kafkaTopic);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertEquals("KafkaTopics's spec.topicName property is absent and KafkaTopics's metadata.name is invalid as a topic name: " +
                    "Topic name is illegal, it can't be longer than 249 characters, topic name: " +
                    illegalAsATopicName, e.getMessage());
        }
    }

    @Test
    public void testErrorInTopicName() {
        KafkaTopic kafkaTopic = new KafkaTopicBuilder().withMetadata(new ObjectMetaBuilder().withName("foo")
                .build()).withNewSpec()
                    .withReplicas(1)
                    .withPartitions(1)
                    .withConfig(emptyMap())
                    .withTopicName("An invalid topic name!")
                .endSpec()
            .build();
        try {
            TopicSerialization.fromTopicResource(kafkaTopic);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertEquals("KafkaTopics's spec.topicName property is invalid as a topic name: Topic name \"An invalid topic name!\" is illegal, it contains a character other than ASCII alphanumerics, '.', '_' and '-'", e.getMessage());
        }
    }

    @Test
    public void testErrorInPartitions() {
        KafkaTopic kafkaTopic = new KafkaTopicBuilder()
                .withMetadata(new ObjectMetaBuilder().withName("my-topic").build())
                .withNewSpec()
                    .withReplicas(1)
                    .withPartitions(-1)
                    .withConfig(emptyMap())
                .endSpec()
            .build();

        try {
            TopicSerialization.fromTopicResource(kafkaTopic);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertEquals("KafkaTopic's spec.partitions should be strictly greater than 0", e.getMessage());
        }
    }

    @Test
    public void testErrorInReplicas() {
        KafkaTopic kafkaTopic = new KafkaTopicBuilder()
                .withMetadata(new ObjectMetaBuilder().withName("my-topic").build())
                .withNewSpec()
                    .withReplicas(-1)
                    .withPartitions(1)
                    .withConfig(emptyMap())
                .endSpec()
            .build();

        try {
            TopicSerialization.fromTopicResource(kafkaTopic);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertEquals("KafkaTopic's spec.replicas should be between 1 and 32767 inclusive", e.getMessage());
        }
    }

    @Test
    public void testErrorInConfigInvalidValueWrongType() {
        KafkaTopic kafkaTopic = new KafkaTopicBuilder()
                .withMetadata(new ObjectMetaBuilder().withName("my-topic").build())
                .withNewSpec()
                    .withReplicas(1)
                    .withPartitions(1)
                    .withConfig(singletonMap("foo", new Object()))
                .endSpec()
            .build();

        try {
            TopicSerialization.fromTopicResource(kafkaTopic);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertEquals("KafkaTopic's spec.config has invalid entry: The key 'foo' of the topic config is invalid: The value corresponding to the key must have a string, number or boolean value but was of type java.lang.Object",
                    e.getMessage());
        }
    }

    @Test
    public void testErrorInConfigInvalidValueNull() {
        KafkaTopic kafkaTopic = new KafkaTopicBuilder()
                .withMetadata(new ObjectMetaBuilder().withName("my-topic").build())
                .withNewSpec()
                    .withReplicas(1)
                    .withPartitions(1)
                    .withConfig(singletonMap("foo", null))
                .endSpec()
            .build();

        try {
            TopicSerialization.fromTopicResource(kafkaTopic);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertEquals("KafkaTopic's spec.config has invalid entry: The key 'foo' of the topic config is invalid: The value corresponding to the key must have a string, number or boolean value but the value was null",
                    e.getMessage());
        }
    }

    @Test
    public void testNoConfig() {
        KafkaTopicSpec spec = new KafkaTopicSpec();
        spec.setPartitions(3);
        spec.setReplicas(3);
        KafkaTopic kafkaTopic = new KafkaTopic();
        kafkaTopic.setMetadata(new ObjectMetaBuilder().withName("my-topic").build());
        kafkaTopic.setSpec(spec);

        TopicSerialization.fromTopicResource(kafkaTopic);
    }
}

