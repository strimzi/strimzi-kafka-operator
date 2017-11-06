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

package io.enmasse.barnabas.operator.topic;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TopicDiffTest {

    Map<String, String> config = new HashMap<>();

    {
        config.put("a", "1");
        config.put("b", "2");
        config.put("c", "3");
    }
    Topic topicA = new Topic.Builder("test" , 2, config).build();

    {
        config.clear();
        config.put("b", "two");
        config.put("c", "3");
        config.put("d", "4");
    }
    Topic topicB = new Topic.Builder("test" , 3, config).build();

    @Test
    public void testDiff() {
        TopicDiff diffAB = TopicDiff.diff(topicA, topicB);
        assertEquals(topicB, diffAB.apply(topicA));

        TopicDiff diffBA = TopicDiff.diff(topicB, topicA);
        assertEquals(topicA, diffBA.apply(topicB));

        Topic topicWrongName = new Topic.Builder("another_name" , 3, config).build();
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

        Topic topicC = new Topic.Builder(topicA.getName(), 4, topicA.getConfig()).build();
        TopicDiff diffAC = TopicDiff.diff(topicA, topicC);
        assertTrue(diffAB.conflicts(diffAC));
        assertTrue(diffAC.conflicts(diffAB));

        Topic topicD = new Topic.Builder(topicA.getName(), topicA.getNumPartitions(), topicB.getConfig()).build();
        TopicDiff diffAD = TopicDiff.diff(topicA, topicD);
        assertFalse(diffAB.conflicts(diffAD));
        assertFalse(diffAD.conflicts(diffAB));

        // Both change the same config
        Map<String, String> conflictingConfig = new HashMap(topicB.getConfig());
        conflictingConfig.put("b", "deux");
        Topic topicE = new Topic.Builder(topicA.getName(), topicA.getNumPartitions(), conflictingConfig).build();
        TopicDiff diffAE = TopicDiff.diff(topicA, topicE);
        assertTrue(diffAD.conflicts(diffAE));
        assertTrue(diffAE.conflicts(diffAD));
        assertEquals("config:b, ", diffAE.conflict(diffAD));

        // One changes and one removes
        conflictingConfig = new HashMap(topicB.getConfig());
        conflictingConfig.remove("b");
        Topic topicF = new Topic.Builder(topicA.getName(), topicA.getNumPartitions(), conflictingConfig).build();
        TopicDiff diffAF = TopicDiff.diff(topicA, topicF);
        assertTrue(diffAD.conflicts(diffAF));
        assertTrue(diffAF.conflicts(diffAD));
        assertEquals("config:b, ", diffAF.conflict(diffAD));

        // Both add, different values
        conflictingConfig = new HashMap(topicB.getConfig());
        conflictingConfig.put("d", "5");
        Topic topicG = new Topic.Builder(topicA.getName(), topicA.getNumPartitions(), conflictingConfig).build();
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

        Topic topicD = new Topic.Builder(topicA.getName(), topicA.getNumPartitions(), topicB.getConfig()).build();
        TopicDiff diffAD = TopicDiff.diff(topicA, topicD);
        TopicDiff merged = diffAB.merge(diffAD);
        Topic end = merged.apply(topicA);
        assertEquals(end, new Topic.Builder(topicA.getName(), topicB.getNumPartitions(), topicB.getConfig()).build());

        Map<String, String> configX = new HashMap<>();
        configX.put("x", "24");
        configX.put("b", "2");
        configX.put("c", "tres");

        Topic topicX = new Topic.Builder(topicA.getName(), topicA.getNumPartitions(), configX).build();
        TopicDiff diffAX = TopicDiff.diff(topicA, topicX);
        merged = diffAB.merge(diffAX);
        end = merged.apply(topicA);

        Map<String, String> configEnd = new HashMap<>();
        configEnd.put("b", "two");
        configEnd.put("c", "tres");
        configEnd.put("d", "4");
        configEnd.put("x", "24");
        assertEquals(end, new Topic.Builder(topicA.getName(), topicB.getNumPartitions(), configEnd).build());
    }
}
