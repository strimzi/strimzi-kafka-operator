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

import io.fabric8.kubernetes.api.model.ConfigMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfigMapSerializationTest {
    @Test
    public void testRoundTrip() {
        Topic.Builder builder = new Topic.Builder();
        builder.withTopicName("tom");
        builder.withNumReplicas((short) 1);
        builder.withNumPartitions(2);
        builder.withConfigEntry("foo", "bar");
        Topic wroteTopic = builder.build();
        ConfigMap cm = TopicSerialization.toConfigMap(wroteTopic);

        assertEquals(wroteTopic.getTopicName().toString(), cm.getMetadata().getName());
        assertEquals(3, cm.getMetadata().getLabels().size());
        assertEquals("barnabas", cm.getMetadata().getLabels().get("app"));
        assertEquals("topic", cm.getMetadata().getLabels().get("kind"));
        assertEquals("runtime", cm.getMetadata().getLabels().get("type"));
        assertEquals(wroteTopic.getTopicName().toString(), cm.getData().get(TopicSerialization.CM_KEY_NAME));
        assertEquals("2", cm.getData().get(TopicSerialization.CM_KEY_PARTITIONS));
        assertEquals("1", cm.getData().get(TopicSerialization.CM_KEY_REPLICAS));
        assertEquals("---\n" +
                "foo: \"bar\"\n", cm.getData().get(TopicSerialization.CM_KEY_CONFIG));

        Topic readTopic = TopicSerialization.fromConfigMap(cm);
        assertEquals(wroteTopic, readTopic);
    }

    // TODO where name cannot be mapped: __consumer_offsets
}
