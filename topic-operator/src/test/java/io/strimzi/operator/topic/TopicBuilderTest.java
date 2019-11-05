/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TopicBuilderTest {

    @Test
    public void testConstructorWithNameAndPartitions() {
        Topic topic = new Topic.Builder("my_topic", 1).build();
        assertThat(topic.getTopicName(), is(new TopicName("my_topic")));
        assertThat(topic.getNumPartitions(), is(1));
        assertThat(topic.getNumReplicas(), is((short) -1));
        assertThat(topic.getConfig(), is(emptyMap()));
    }

    @Test
    public void testConstructorWithNameAndPartitions2() {
        Topic topic = new Topic.Builder(new TopicName("my_topic"), 1).build();
        assertThat(topic.getTopicName(), is(new TopicName("my_topic")));
        assertThat(topic.getNumPartitions(), is(1));
        assertThat(topic.getNumReplicas(), is((short) -1));
        assertThat(topic.getConfig(), is(emptyMap()));
    }

    @Test
    public void testConstructorWithConfig() {
        Topic topic = new Topic.Builder("my_topic", 1, singletonMap("foo", "bar")).build();
        assertThat(topic.getTopicName(), is(new TopicName("my_topic")));
        assertThat(topic.getNumPartitions(), is(1));
        assertThat(topic.getNumReplicas(), is((short) -1));
        assertThat(topic.getConfig(), is(singletonMap("foo", "bar")));

        // Check we take a copy of the config
        Map<String, String> config = new HashMap<>();
        config.put("foo", "bar");
        Topic.Builder builder = new Topic.Builder("my_topic", 1, config);
        config.clear();
        topic = builder.build();
        assertThat(topic.getTopicName(), is(new TopicName("my_topic")));
        assertThat(topic.getNumPartitions(), is(1));
        assertThat(topic.getNumReplicas(), is((short) -1));
        assertThat(topic.getConfig(), is(singletonMap("foo", "bar")));
    }

    // TODO testConstructorWithTopic
    // TODO testWithMapName

    @Test
    public void testWithConfig() {
        Topic.Builder builder = new Topic.Builder("my_topic", 1);
        builder.withConfig(singletonMap("foo", "bar"));
        Topic topic = builder.build();
        assertThat(topic.getTopicName(), is(new TopicName("my_topic")));
        assertThat(topic.getNumPartitions(), is(1));
        assertThat(topic.getNumReplicas(), is((short) -1));
        assertThat(topic.getConfig(), is(singletonMap("foo", "bar")));

        // Check we take a copy of the config
        Map<String, String> config = new HashMap<>();
        config.put("foo", "bar");
        builder = new Topic.Builder("my_topic", 1);
        builder.withConfig(config);
        config.clear();
        topic = builder.build();
        assertThat(topic.getTopicName(), is(new TopicName("my_topic")));
        assertThat(topic.getNumPartitions(), is(1));
        assertThat(topic.getNumReplicas(), is((short) -1));
        assertThat(topic.getConfig(), is(singletonMap("foo", "bar")));
    }

    @Test
    public void testWithConfigEntry() {
        Topic.Builder builder = new Topic.Builder("my_topic", 1);
        builder.withConfigEntry("foo", "bar");
        Topic topic = builder.build();
        assertThat(topic.getTopicName(), is(new TopicName("my_topic")));
        assertThat(topic.getNumPartitions(), is(1));
        assertThat(topic.getNumReplicas(), is((short) -1));
        assertThat(topic.getConfig(), is(singletonMap("foo", "bar")));
    }

    @Test
    public void testWithoutConfigEntry() {
        Topic.Builder builder = new Topic.Builder("my_topic", 1, singletonMap("foo", "bar"));
        builder.withoutConfigEntry("foo");
        Topic topic = builder.build();
        assertThat(topic.getTopicName(), is(new TopicName("my_topic")));
        assertThat(topic.getNumPartitions(), is(1));
        assertThat(topic.getNumReplicas(), is((short) -1));
        assertThat(topic.getConfig(), is(emptyMap()));
    }
}
