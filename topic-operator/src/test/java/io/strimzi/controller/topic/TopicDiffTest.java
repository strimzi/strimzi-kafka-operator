/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


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
        assertEquals(topicB, diffAB.apply(topicA));

        TopicDiff diffBA = TopicDiff.diff(topicB, topicA);
        assertEquals(topicA, diffBA.apply(topicB));

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
        assertFalse(diffAB == diffABAgain);
        assertEquals(diffAB, diffABAgain);
        assertFalse(diffAB.conflicts(diffABAgain));
        assertFalse(diffABAgain.conflicts(diffAB));

        Topic topicC = new Topic.Builder(topicA.getTopicName(), 4, topicA.getConfig()).build();
        TopicDiff diffAC = TopicDiff.diff(topicA, topicC);
        assertTrue(diffAB.conflicts(diffAC));
        assertTrue(diffAC.conflicts(diffAB));

        Topic topicD = new Topic.Builder(topicA.getTopicName(), topicA.getNumPartitions(), topicB.getConfig()).build();
        TopicDiff diffAD = TopicDiff.diff(topicA, topicD);
        assertFalse(diffAB.conflicts(diffAD));
        assertFalse(diffAD.conflicts(diffAB));

        // Both change the same config
        Map<String, String> conflictingConfig = new HashMap(topicB.getConfig());
        conflictingConfig.put("b", "deux");
        Topic topicE = new Topic.Builder(topicA.getTopicName(), topicA.getNumPartitions(), conflictingConfig).build();
        TopicDiff diffAE = TopicDiff.diff(topicA, topicE);
        assertTrue(diffAD.conflicts(diffAE));
        assertTrue(diffAE.conflicts(diffAD));
        assertEquals("config:b, ", diffAE.conflict(diffAD));

        // One changes and one removes
        conflictingConfig = new HashMap(topicB.getConfig());
        conflictingConfig.remove("b");
        Topic topicF = new Topic.Builder(topicA.getTopicName(), topicA.getNumPartitions(), conflictingConfig).build();
        TopicDiff diffAF = TopicDiff.diff(topicA, topicF);
        assertTrue(diffAD.conflicts(diffAF));
        assertTrue(diffAF.conflicts(diffAD));
        assertEquals("config:b, ", diffAF.conflict(diffAD));

        // Both add, different values
        conflictingConfig = new HashMap(topicB.getConfig());
        conflictingConfig.put("d", "5");
        Topic topicG = new Topic.Builder(topicA.getTopicName(), topicA.getNumPartitions(), conflictingConfig).build();
        TopicDiff diffAG = TopicDiff.diff(topicA, topicG);
        assertTrue(diffAD.conflicts(diffAG));
        assertTrue(diffAG.conflicts(diffAD));
        assertEquals("config:d, ", diffAG.conflict(diffAD));
    }

    @Test
    public void testMerge() {
        final TopicDiff diffAB = TopicDiff.diff(topicA, topicB);
        TopicDiff diffABAgain = TopicDiff.diff(topicA, topicB);
        assertEquals(diffAB, diffAB.merge(diffABAgain));

        Topic topicD = new Topic.Builder(topicA.getTopicName(), topicA.getNumPartitions(), topicB.getConfig()).build();
        TopicDiff diffAD = TopicDiff.diff(topicA, topicD);
        TopicDiff merged = diffAB.merge(diffAD);
        Topic end = merged.apply(topicA);
        assertEquals(end, new Topic.Builder(topicA.getTopicName(), topicB.getNumPartitions(), topicB.getConfig()).build());

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
        assertEquals(end, new Topic.Builder(topicA.getTopicName(), topicB.getNumPartitions(), configEnd).build());
    }

    @Test
    public void testChangesNumPartitions() {
        Topic topicC = new Topic.Builder(topicB.getTopicName(), topicB.getNumPartitions(), topicB.getConfig()).build();
        assertTrue(TopicDiff.diff(topicB, topicA).changesNumPartitions());
        assertEquals(-1, TopicDiff.diff(topicB, topicA).numPartitionsDelta());
        assertTrue(TopicDiff.diff(topicA, topicB).changesNumPartitions());
        assertEquals(1, TopicDiff.diff(topicA, topicB).numPartitionsDelta());
        assertFalse(TopicDiff.diff(topicB, topicC).changesNumPartitions());
        assertEquals(0, TopicDiff.diff(topicB, topicC).numPartitionsDelta());
    }

    @Test
    public void testEmptyDiff() {
        assertTrue(TopicDiff.diff(topicA, topicA).isEmpty());
        assertFalse(TopicDiff.diff(topicA, topicB).isEmpty());
        assertFalse(TopicDiff.diff(topicB, topicA).isEmpty());
    }

    @Test
    public void testChangesConfig() {
        Topic topicC = new Topic.Builder(topicB.getTopicName(), topicB.getNumPartitions(), topicB.getConfig()).build();
        assertTrue(TopicDiff.diff(topicB, topicA).changesConfig());
        assertTrue(TopicDiff.diff(topicA, topicB).changesConfig());
        assertFalse(TopicDiff.diff(topicB, topicC).changesConfig());
    }

    @Test
    public void testChangesReplicationFactor() {
        Topic topicC = new Topic.Builder(topicB.getTopicName(), topicB.getNumPartitions(), (short) 2, topicB.getConfig()).build();
        Topic topicD = new Topic.Builder(topicB.getTopicName(), topicC.getNumPartitions() + 1, (short) 2, topicB.getConfig()).build();
        Topic topicE = new Topic.Builder(topicB.getTopicName(), topicB.getNumPartitions(), (short) 3, topicB.getConfig()).build();
        assertFalse(TopicDiff.diff(topicC, topicD).changesReplicationFactor());
        assertTrue(TopicDiff.diff(topicD, topicE).changesReplicationFactor());
        assertTrue(TopicDiff.diff(topicC, topicE).changesReplicationFactor());
    }
}
