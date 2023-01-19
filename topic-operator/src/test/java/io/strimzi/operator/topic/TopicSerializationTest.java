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
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class TopicSerializationTest {

    private final Labels labels = new Labels(
            "app", "strimzi");

    @Test
    public void testTopicCommandSerde() {
        TopicCommandSerde serde = new TopicCommandSerde();

        Topic.Builder builder = new Topic.Builder();
        builder.withTopicName("foobar");
        builder.withNumReplicas((short) 1);
        builder.withNumPartitions(2);
        builder.withConfigEntry("cleanup.policy", "bar");
        ObjectMeta metadata = new ObjectMeta();
        builder.withMetadata(metadata);
        Topic topic = builder.build();

        TopicCommand data = TopicCommand.create(topic);
        byte[] bytes = serde.serialize("dummy", data);
        data = serde.deserialize("dummy", bytes);
        Assertions.assertEquals(TopicCommand.Type.CREATE, data.getType());
        Assertions.assertEquals(topic, data.getTopic());
        Assertions.assertEquals(TopicCommand.CURRENT_VERSION, data.getVersion());

        TopicName tn = new TopicName("deleteme");
        data = TopicCommand.delete(tn);
        bytes = serde.serialize("dummy", data);
        data = serde.deserialize("dummy", bytes);
        Assertions.assertEquals(TopicCommand.Type.DELETE, data.getType());
        Assertions.assertEquals(tn, data.getName());
        Assertions.assertEquals(TopicCommand.CURRENT_VERSION, data.getVersion());
    }

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

        assertThat(kafkaTopic.getMetadata().getName(), is(wroteTopic.getTopicName().toString()));
        assertThat(kafkaTopic.getMetadata().getLabels().size(), is(1));
        assertThat(kafkaTopic.getMetadata().getLabels().get("app"), is("strimzi"));
        assertThat(kafkaTopic.getSpec().getTopicName(), is(wroteTopic.getTopicName().toString()));
        assertThat(kafkaTopic.getSpec().getPartitions(), is(Integer.valueOf(2)));
        assertThat(kafkaTopic.getSpec().getReplicas(), is(Integer.valueOf(1)));
        assertThat(kafkaTopic.getSpec().getConfig(), is(singletonMap("cleanup.policy", "bar")));

        Topic readTopic = TopicSerialization.fromTopicResource(kafkaTopic);
        assertThat(readTopic, is(wroteTopic));
    }

    @Test
    public void testResourceSerializationRoundTripWithKubernetesLabels() {
        String topicName = "tom";
        Topic.Builder builder = new Topic.Builder();
        builder.withTopicName(topicName);
        builder.withNumReplicas((short) 1);
        builder.withNumPartitions(2);
        builder.withConfigEntry("cleanup.policy", "bar");
        ObjectMeta metadata = new ObjectMeta();
        metadata.setAnnotations(new HashMap<>());

        Map<String, String> kubeLabels = new HashMap<>(3);
        kubeLabels.put("app.kubernetes.io/name", "kstreams");
        kubeLabels.put("app.kubernetes.io/instance", "fraud-detection");
        kubeLabels.put("app.kubernetes.io/managed-by", "helm");

        metadata.setLabels(kubeLabels);
        builder.withMetadata(metadata);
        Topic wroteTopic = builder.build();
        KafkaTopic kafkaTopic = TopicSerialization.toTopicResource(wroteTopic, labels);

        assertThat(kafkaTopic.getMetadata().getName(), is(wroteTopic.getTopicName().toString()));
        assertThat(kafkaTopic.getMetadata().getLabels().size(), is(4));
        assertThat(kafkaTopic.getMetadata().getLabels().get("app"), is("strimzi"));
        assertThat(kafkaTopic.getMetadata().getLabels().get("app.kubernetes.io/name"), is("kstreams"));
        assertThat(kafkaTopic.getMetadata().getLabels().get("app.kubernetes.io/instance"), is("fraud-detection"));
        assertThat(kafkaTopic.getMetadata().getLabels().get("app.kubernetes.io/managed-by"), is("helm"));
        assertThat(kafkaTopic.getSpec().getTopicName(), is(wroteTopic.getTopicName().toString()));
        assertThat(kafkaTopic.getSpec().getPartitions(), is(Integer.valueOf(2)));
        assertThat(kafkaTopic.getSpec().getReplicas(), is(Integer.valueOf(1)));
        assertThat(kafkaTopic.getSpec().getConfig(), is(singletonMap("cleanup.policy", "bar")));

        Topic readTopic = TopicSerialization.fromTopicResource(kafkaTopic);
        assertThat(readTopic, is(wroteTopic));
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
        assertThat(json, is("{\"map-name\":\"bob\"," +
                "\"topic-name\":\"tom\"," +
                "\"partitions\":2," +
                "\"replicas\":1," +
                "\"config\":{\"foo\":\"bar\"}" +
                "}"));
        Topic readTopic = TopicSerialization.fromJson(bytes);
        assertThat(readTopic, is(wroteTopic));
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
        assertThat(newTopic.name(), is("test-topic"));
        assertThat(newTopic.numPartitions(), is(3));
        assertThat(newTopic.replicationFactor(), is((short) 2));
        assertThat(newTopic.replicasAssignments(), is(nullValue()));
        assertThat(newTopic.configs(), is(singletonMap("foo", "bar")));
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
        assertThat(config.size(), is(1));
        Map.Entry<ConfigResource, Config> c = config.entrySet().iterator().next();
        assertThat(c.getKey().type(), is(ConfigResource.Type.TOPIC));
        assertThat(c.getValue().entries().size(), is(1));
        assertThat(c.getKey().name(), is("test-topic"));
        assertThat(c.getValue().get("foo").value(), is("bar"));
    }

    @Test
    public void testFromTopicMetadata() {
        List<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry("foo", "bar"));
        Config topicConfig = new Config(entries);
        TopicMetadata meta = Utils.getTopicMetadata("test-topic", topicConfig);
        Topic topic = TopicSerialization.fromTopicMetadata(meta);
        assertThat(topic.getTopicName(), is(new TopicName("test-topic")));
        // Null map name because Kafka doesn't know about the map
        assertThat(topic.getResourceName(), is(nullValue()));
        assertThat(topic.getConfig(), is(singletonMap("foo", "bar")));
        assertThat(topic.getNumPartitions(), is(2));
        assertThat(topic.getNumReplicas(), is((short) 3));
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
            assertThat(e.getMessage(), is("KafkaTopics's spec.topicName property is absent and KafkaTopics's metadata.name is invalid as a topic name: " +
                    "Topic name is invalid: the length of '" + illegalAsATopicName + "' is longer than the max allowed length 249"));
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
            assertThat(e.getMessage(), is("KafkaTopics's spec.topicName property is invalid as a topic name: " +
                    "Topic name is invalid: 'An invalid topic name!' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'"));
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
            assertThat(e.getMessage(), is("KafkaTopic's spec.partitions should be strictly greater than 0"));
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
            assertThat(e.getMessage(), is("KafkaTopic's spec.replicas should be between 1 and 32767 inclusive"));
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
            assertThat(e.getMessage(), is("KafkaTopic's spec.config has invalid entry: The key 'foo' of the topic config is invalid: The value corresponding to the key must have a string, number or boolean value but was of type java.lang.Object"));
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
            assertThat(e.getMessage(), is("KafkaTopic's spec.config has invalid entry: The key 'foo' of the topic config is invalid: The value corresponding to the key must have a string, number or boolean value but the value was null"));
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