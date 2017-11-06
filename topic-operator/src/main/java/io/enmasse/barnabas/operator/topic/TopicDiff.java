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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Represents the difference between two topics.
 * {@code TopicDiff}s are {@linkplain #diff(Topic, Topic) computed} from a source topic to a target topic
 * with the invariant that:
 * <pre></pre><code>
 *     TopicDiff.diff(topicA, topicB).apply(topicA).equals(topicB)
 * </code></pre>
 */
public class TopicDiff {

    private static abstract class Difference {
        private Difference() {}

        protected abstract Object address();

        protected abstract void apply(Topic.Builder builder);
    }

    private static class NumPartitionsDifference extends Difference {
        private int newNumPartitions;

        public NumPartitionsDifference(int newNumPartitions) {
            this.newNumPartitions = newNumPartitions;
        }

        @Override
        public Object address() {
            return "numPartitions";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NumPartitionsDifference that = (NumPartitionsDifference) o;

            return newNumPartitions == that.newNumPartitions;
        }

        @Override
        public int hashCode() {
            return newNumPartitions;
        }

        @Override
        public String toString() {
            return "newNumPartitions=" + newNumPartitions;
        }

        @Override
        protected void apply(Topic.Builder builder) {
            builder.withNumPartitions(this.newNumPartitions);
        }
    }

    private static class AddedConfigEntry extends Difference {
        private final String configKey;
        private final String configValue;

        public AddedConfigEntry(String configKey, String configValue) {
            assert(configKey != null && configValue != null);
            this.configKey = configKey;
            this.configValue = configValue;
        }

        @Override
        public Object address() {
            return "config:"+configKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AddedConfigEntry that = (AddedConfigEntry) o;

            if (!configKey.equals(that.configKey)) return false;
            return configValue.equals(that.configValue);
        }

        @Override
        public int hashCode() {
            int result = configKey.hashCode();
            result = 31 * result + configValue.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "config +'" + configKey + '\'' +
                    "='" + configValue + '\'';
        }

        @Override
        protected void apply(Topic.Builder builder) {
            builder.withConfigEntry(this.configKey, this.configValue);
        }
    }

    private static class RemovedConfigEntry extends Difference {
        private String configKey;

        public RemovedConfigEntry(String configKey) {
            this.configKey = configKey;
        }

        @Override
        public Object address() {
            return "config:"+configKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RemovedConfigEntry that = (RemovedConfigEntry) o;

            return configKey.equals(that.configKey);
        }

        @Override
        public int hashCode() {
            return configKey.hashCode();
        }

        @Override
        public String toString() {
            return "config -'" + configKey + '\'';
        }

        @Override
        protected void apply(Topic.Builder builder) {
            builder.withoutConfigEntry(this.configKey);
        }
    }

    private final Map<Object, Difference> differences;

    private TopicDiff(Map<Object, Difference> differences) {
        this.differences = differences;
    }

    /**
     * Return the TopicDiff that will transform the given source topic into the given target topic.
     * @param source
     * @param target
     * @return The difference between the source and target.
     */
    public static TopicDiff diff(Topic source, Topic target) {
        if (!source.getName().equals(target.getName())) {
            throw new IllegalArgumentException();
        }
        Map<Object, Difference> differences = new HashMap<>();
        if (source.getNumPartitions() != target.getNumPartitions()) {
            NumPartitionsDifference numPartitionsDifference = new NumPartitionsDifference(target.getNumPartitions());
            differences.put(numPartitionsDifference.address(), numPartitionsDifference);
        }
        if (!source.getConfig().equals(target.getConfig())) {
            //Removed keys
            HashSet<String> removed = new HashSet<>(source.getConfig().keySet());
            removed.removeAll(target.getConfig().keySet());
            for (String removedKey : removed) {
                RemovedConfigEntry removedConfigEntry = new RemovedConfigEntry(removedKey);
                differences.put(removedConfigEntry.address(), removedConfigEntry);
            }
            //Added keys
            HashSet<String> added = new HashSet<>(target.getConfig().keySet());
            added.removeAll(source.getConfig().keySet());
            for (String addedKey : added) {
                AddedConfigEntry addedConfigEntry = new AddedConfigEntry(addedKey, target.getConfig().get(addedKey));
                differences.put(addedConfigEntry.address(), addedConfigEntry);
            }
            //Changed values
            HashSet<String> retained = new HashSet<>(source.getConfig().keySet());
            retained.retainAll(target.getConfig().keySet());
            for (String retainedKey : retained) {
                if (!source.getConfig().get(retainedKey).equals(target.getConfig().get(retainedKey))) {
                    AddedConfigEntry addedConfigEntry = new AddedConfigEntry(retainedKey, target.getConfig().get(retainedKey));
                    differences.put(addedConfigEntry.address(), addedConfigEntry);
                }
            }
        }
        return new TopicDiff(differences);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicDiff topicDiff = (TopicDiff) o;

        return differences.equals(topicDiff.differences);
    }

    @Override
    public int hashCode() {
        return differences.hashCode();
    }

    @Override
    public String toString() {
        return "TopicDiff{" +
                "differences=" + differences +
                '}';
    }

    /**
     * Apply this diff to this given topic, returning a new topic.
     * @param topic
     * @return
     */
    public Topic apply(Topic topic) {
        Topic.Builder builder = new Topic.Builder(topic);
        for (Difference d : differences.values()) {
            d.apply(builder);
        }
        return builder.build();
    }

    /**
     * Return true if this TopicDiff conflicts with the given other TopicDiff
     */
    public String conflict(TopicDiff other) {
        Set<Difference> intersection = new HashSet(this.differences.keySet());
        intersection.retainAll(other.differences.keySet());
        // they could still be OK if they're applying the _same_ differences
        StringBuilder sb = new StringBuilder();
        for (Object address : intersection) {
            Difference difference = this.differences.get(address);
            Difference otherDifference = other.differences.get(address);
            if (!difference.equals(otherDifference)) {
                sb.append(address).append(", ");
            }
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    public boolean conflicts(TopicDiff other) {
        return conflict(other) != null;
    }

    /**
     * Merge this TopicDiff with the given other TopicDiff, returning a
     * single diff which combines the two.
     * @param other The diff to merge with.
     * @return
     * @throws IllegalArgumentException if the topics conflict.
     */
    public TopicDiff merge(TopicDiff other) {
        String confict = this.conflict(other);
        if (confict != null) {
            throw new IllegalArgumentException("Conflict: " + confict);
        }
        Map<Object, Difference> union = new HashMap(this.differences);
        union.putAll(other.differences);
        return new TopicDiff(union);
    }
}
/*
0. Set up some persistent ZK nodes for us
1. When updating CM, we also update our ZK nodes
2. When updating Kafka, we also update our ZK nodes
3. When reconciling we get all three versions of the Topic, k8s, kafka and ours
   - If ours doesn't exist:
     - If k8s doesn't exist, we reason it's been created in kafka and we create it k8s from kafka
     - If kafka doesn't exist, we reason it's been created in k8s, and we create it in kafka from k8s
     - If both exist, and are the same: That's fine
     - If both exist, and are different: We use whichever has the most recent mtime.
     - In all above cases we create ours
   - If ours does exist:
     - If k8s doesn't exist, we reason it was deleted, and delete kafka
     - If kafka doesn't exist, we reason it was delete and we delete k8s
     - If neither exists, we delete ours.
     - If both exist then all three exist, and we need to reconcile:
       - We compute diff ours->k8s and ours->kafka and we merge the two
         - If there are conflicts we're fucked
         - Otherwise we apply the apply the merged diff to ours, and use that for both k8s and kafka
     - In all above cases we update ours

Topic identification should be by uid/cxid, not by name.
*/
