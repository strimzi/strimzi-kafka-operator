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
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.fail;

public class TopicSerializationTest {

    private final Labels labels = new Labels(
            "app", "strimzi");

    String kafkaVersion = Utils.getLatestKafkaVersion();

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
        builder.withConfigEntry("cleanup.policy", "compact");
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
        assertThat(kafkaTopic.getSpec().getConfig(), is(singletonMap("cleanup.policy", "compact")));

        Topic readTopic = TopicSerialization.fromTopicResource(kafkaTopic, kafkaVersion);
        assertThat(readTopic, is(wroteTopic));
    }

    @Test
    public void testResourceSerializationRoundTripWithKubernetesLabels() {
        String topicName = "tom";
        Topic.Builder builder = new Topic.Builder();
        builder.withTopicName(topicName);
        builder.withNumReplicas((short) 1);
        builder.withNumPartitions(2);
        builder.withConfigEntry("cleanup.policy", "delete");
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
        assertThat(kafkaTopic.getSpec().getConfig(), is(singletonMap("cleanup.policy", "delete")));

        Topic readTopic = TopicSerialization.fromTopicResource(kafkaTopic, kafkaVersion);
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
            TopicSerialization.fromTopicResource(kafkaTopic, kafkaVersion);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertThat(e.getMessage(), is("KafkaTopics's spec.topicName property is absent and KafkaTopics's metadata.name is invalid as a topic name: " +
                    "Topic name is illegal, it can't be longer than 249 characters, Topic name: " +
                    illegalAsATopicName));
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
            TopicSerialization.fromTopicResource(kafkaTopic, kafkaVersion);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertThat(e.getMessage(), is("KafkaTopics's spec.topicName property is invalid as a topic name: Topic name \"An invalid topic name!\" is illegal, it contains a character other than ASCII alphanumerics, '.', '_' and '-'"));
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
            TopicSerialization.fromTopicResource(kafkaTopic, kafkaVersion);
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
            TopicSerialization.fromTopicResource(kafkaTopic, kafkaVersion);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertThat(e.getMessage(), is("KafkaTopic's spec.replicas should be between 1 and 32767 inclusive"));
        }
    }

    @Test
    public void testErrorInConfigInvalidValueWrongType() {
        KafkaTopic kafkaTopic = new KafkaTopicBuilder()
                .withMetadata(new ObjectMetaBuilder().withNamespace("test").withName("my-topic").build())
                .withNewSpec()
                    .withReplicas(1)
                    .withPartitions(1)
                    .withConfig(singletonMap("foo", new Object()))
                .endSpec()
            .build();

        try {
            TopicSerialization.fromTopicResource(kafkaTopic, kafkaVersion);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertThat(e.getMessage(), matchesPattern("KafkaTopic test/my-topic has invalid spec.config: foo with value 'java.lang.Object@.*' is not one of the known options"));
        }
    }

    @Test
    public void testErrorInConfigInvalidValueNull() {
        KafkaTopic kafkaTopic = new KafkaTopicBuilder()
                .withMetadata(new ObjectMetaBuilder().withNamespace("test").withName("my-topic").build())
                .withNewSpec()
                    .withReplicas(1)
                    .withPartitions(1)
                    .withConfig(singletonMap("foo", null))
                .endSpec()
            .build();

        try {
            TopicSerialization.fromTopicResource(kafkaTopic, kafkaVersion);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertThat(e.getMessage(), is("KafkaTopic test/my-topic has invalid spec.config: foo with value 'null' is not one of the known options"));
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

        TopicSerialization.fromTopicResource(kafkaTopic, kafkaVersion);
    }

    @Test
    public void testInConfigInvalidValueWrongType() {
        KafkaTopic kafkaTopic = new KafkaTopicBuilder()
                .withMetadata(new ObjectMetaBuilder().withName("my-topic").withNamespace("test").build())
                .withNewSpec()
                .withReplicas(1)
                .withPartitions(1)
                .withConfig(singletonMap("compression.type", 42))
                .endSpec()
                .build();

        try {
            TopicSerialization.fromTopicResource(kafkaTopic, kafkaVersion);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertThat(e.getMessage(), is("KafkaTopic test/my-topic has invalid spec.config: compression.type has value '42' which is not one of the allowed values: [uncompressed, zstd, lz4, snappy, gzip, producer]"));
        }
    }

    @Test
    public void testInConfigInvalidValueWrongType2() {
        KafkaTopic kafkaTopic = new KafkaTopicBuilder()
                .withMetadata(new ObjectMetaBuilder().withName("my-topic").withNamespace("test").build())
                .withNewSpec()
                .withReplicas(1)
                .withPartitions(1)
                .withConfig(singletonMap("delete.retention.ms", "week"))
                .endSpec()
                .build();

        try {
            TopicSerialization.fromTopicResource(kafkaTopic, kafkaVersion);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertThat(e.getMessage(), is("KafkaTopic test/my-topic has invalid spec.config: delete.retention.ms has value 'week' which is not a long"));
        }
    }

    @Test
    public void testInConfigInvalidValueWrongType3() {
        KafkaTopic kafkaTopic = new KafkaTopicBuilder()
                .withMetadata(new ObjectMetaBuilder().withName("my-topic").withNamespace("test").build())
                .withNewSpec()
                .withReplicas(1)
                .withPartitions(1)
                .withConfig(singletonMap("preallocate", "yes"))
                .endSpec()
                .build();

        try {
            TopicSerialization.fromTopicResource(kafkaTopic, kafkaVersion);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertThat(e.getMessage(), is("KafkaTopic test/my-topic has invalid spec.config: preallocate has value 'yes' which is not a boolean"));
        }
    }

    @Test
    public void testInConfigInvalidValueWrongTypes() {
        KafkaTopic kafkaTopic = new KafkaTopicBuilder()
                .withMetadata(new ObjectMetaBuilder().withName("my-topic").withNamespace("test").build())
                .withNewSpec()
                .withReplicas(1)
                .withPartitions(1)
                .withConfig(Map.of("preallocate", "yes", "retention.bytes", "tisic"))
                .endSpec()
                .build();

        try {
            TopicSerialization.fromTopicResource(kafkaTopic, kafkaVersion);
            fail("Should throw");
        } catch (InvalidTopicException e) {
            assertThat(e.getMessage(), anyOf(is("KafkaTopic test/my-topic has invalid spec.config: retention.bytes has value 'tisic' which is not a long, preallocate has value 'yes' which is not a boolean"),
                    is("KafkaTopic test/my-topic has invalid spec.config: preallocate has value 'yes' which is not a boolean, retention.bytes has value 'tisic' which is not a long")));
        }
    }
}