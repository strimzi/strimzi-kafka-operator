/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;


public class TopicDiffTest {

    Map<String, String> config = new HashMap<>();

    {
        config.put("a", "1");
        config.put("b", "2");
        config.put("c", "3");
    }
    Topic topicA = new Topic.Builder("test", 2, config).build();

    {
        config.clear();
        config.put("b", "two");
        config.put("c", "3");
        config.put("d", "4");
    }
    Topic topicB = new Topic.Builder("test", 3, config).build();

    @Test
    public void testDiff() {
        TopicDiff diffAB = TopicDiff.diff(topicA, topicB);
        assertThat(diffAB.apply(topicA), is(topicB));

        TopicDiff diffBA = TopicDiff.diff(topicB, topicA);
        assertThat(diffBA.apply(topicB), is(topicA));

        Topic topicWrongName = new Topic.Builder("another_name", 3, config).build();
        try {
            TopicDiff.diff(topicA, topicWrongName);
            fail("Should throw");
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testConflicts() {
        final TopicDiff diffAB = TopicDiff.diff(topicA, topicB);
        TopicDiff diffABAgain = TopicDiff.diff(topicA, topicB);
        assertThat(diffAB == diffABAgain, is(false));
        assertThat(diffABAgain, is(diffAB));
        assertThat(diffAB.conflicts(diffABAgain), is(false));
        assertThat(diffABAgain.conflicts(diffAB), is(false));

        Topic topicC = new Topic.Builder(topicA.getTopicName(), 4, topicA.getConfig()).build();
        TopicDiff diffAC = TopicDiff.diff(topicA, topicC);
        assertThat(diffAB.conflicts(diffAC), is(true));
        assertThat(diffAC.conflicts(diffAB), is(true));

        Topic topicD = new Topic.Builder(topicA.getTopicName(), topicA.getNumPartitions(), topicB.getConfig()).build();
        TopicDiff diffAD = TopicDiff.diff(topicA, topicD);
        assertThat(diffAB.conflicts(diffAD), is(false));
        assertThat(diffAD.conflicts(diffAB), is(false));

        // Both change the same config
        Map<String, String> conflictingConfig = new HashMap(topicB.getConfig());
        conflictingConfig.put("b", "deux");
        Topic topicE = new Topic.Builder(topicA.getTopicName(), topicA.getNumPartitions(), conflictingConfig).build();
        TopicDiff diffAE = TopicDiff.diff(topicA, topicE);
        assertThat(diffAD.conflicts(diffAE), is(true));
        assertThat(diffAE.conflicts(diffAD), is(true));
        assertThat(diffAE.conflict(diffAD), is("config:b, "));

        // One changes and one removes
        conflictingConfig = new HashMap(topicB.getConfig());
        conflictingConfig.remove("b");
        Topic topicF = new Topic.Builder(topicA.getTopicName(), topicA.getNumPartitions(), conflictingConfig).build();
        TopicDiff diffAF = TopicDiff.diff(topicA, topicF);
        assertThat(diffAD.conflicts(diffAF), is(true));
        assertThat(diffAF.conflicts(diffAD), is(true));
        assertThat(diffAF.conflict(diffAD), is("config:b, "));

        // Both add, different values
        conflictingConfig = new HashMap(topicB.getConfig());
        conflictingConfig.put("d", "5");
        Topic topicG = new Topic.Builder(topicA.getTopicName(), topicA.getNumPartitions(), conflictingConfig).build();
        TopicDiff diffAG = TopicDiff.diff(topicA, topicG);
        assertThat(diffAD.conflicts(diffAG), is(true));
        assertThat(diffAG.conflicts(diffAD), is(true));
        assertThat(diffAG.conflict(diffAD), is("config:d, "));
    }

    @Test
    public void testMerge() {
        final TopicDiff diffAB = TopicDiff.diff(topicA, topicB);
        TopicDiff diffABAgain = TopicDiff.diff(topicA, topicB);
        assertThat(diffAB.merge(diffABAgain), is(diffAB));

        Topic topicD = new Topic.Builder(topicA.getTopicName(), topicA.getNumPartitions(), topicB.getConfig()).build();
        TopicDiff diffAD = TopicDiff.diff(topicA, topicD);
        TopicDiff merged = diffAB.merge(diffAD);
        Topic end = merged.apply(topicA);
        assertThat(end, is(new Topic.Builder(topicA.getTopicName(), topicB.getNumPartitions(), topicB.getConfig()).build()));

        Map<String, String> configX = new HashMap<>();
        configX.put("x", "24");
        configX.put("b", "2");
        configX.put("c", "tres");

        Topic topicX = new Topic.Builder(topicA.getTopicName(), topicA.getNumPartitions(), configX).build();
        TopicDiff diffAX = TopicDiff.diff(topicA, topicX);
        merged = diffAB.merge(diffAX);
        end = merged.apply(topicA);

        Map<String, String> configEnd = new HashMap<>();
        configEnd.put("b", "two");
        configEnd.put("c", "tres");
        configEnd.put("d", "4");
        configEnd.put("x", "24");
        assertThat(end, is(new Topic.Builder(topicA.getTopicName(), topicB.getNumPartitions(), configEnd).build()));
    }

    @Test
    public void testChangesNumPartitions() {
        Topic topicC = new Topic.Builder(topicB.getTopicName(), topicB.getNumPartitions(), topicB.getConfig()).build();
        assertThat(TopicDiff.diff(topicB, topicA).changesNumPartitions(), is(true));
        assertThat(TopicDiff.diff(topicB, topicA).numPartitionsDelta(), is(-1));
        assertThat(TopicDiff.diff(topicA, topicB).changesNumPartitions(), is(true));
        assertThat(TopicDiff.diff(topicA, topicB).numPartitionsDelta(), is(1));
        assertThat(TopicDiff.diff(topicB, topicC).changesNumPartitions(), is(false));
        assertThat(TopicDiff.diff(topicB, topicC).numPartitionsDelta(), is(0));
    }

    @Test
    public void testEmptyDiff() {
        assertThat(TopicDiff.diff(topicA, topicA).isEmpty(), is(true));
        assertThat(TopicDiff.diff(topicA, topicB).isEmpty(), is(false));
        assertThat(TopicDiff.diff(topicB, topicA).isEmpty(), is(false));
    }

    @Test
    public void testChangesConfig() {
        Topic topicC = new Topic.Builder(topicB.getTopicName(), topicB.getNumPartitions(), topicB.getConfig()).build();
        assertThat(TopicDiff.diff(topicB, topicA).changesConfig(), is(true));
        assertThat(TopicDiff.diff(topicA, topicB).changesConfig(), is(true));
        assertThat(TopicDiff.diff(topicB, topicC).changesConfig(), is(false));
    }

    @Test
    public void testChangesReplicationFactor() {
        Topic topicC = new Topic.Builder(topicB.getTopicName(), topicB.getNumPartitions(), (short) 2, topicB.getConfig()).build();
        Topic topicD = new Topic.Builder(topicB.getTopicName(), topicC.getNumPartitions() + 1, (short) 2, topicB.getConfig()).build();
        Topic topicE = new Topic.Builder(topicB.getTopicName(), topicB.getNumPartitions(), (short) 3, topicB.getConfig()).build();
        assertThat(TopicDiff.diff(topicC, topicD).changesReplicationFactor(), is(false));
        assertThat(TopicDiff.diff(topicD, topicE).changesReplicationFactor(), is(true));
        assertThat(TopicDiff.diff(topicC, topicE).changesReplicationFactor(), is(true));
    }
}
