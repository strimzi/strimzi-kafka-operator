/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.ObjectMeta;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the difference between two topics.
 * {@code TopicDiff}s are {@linkplain #diff(Topic, Topic) computed} from a source topic to a target topic
 * with the invariant that:
 * <pre><code>
 *     TopicDiff.diff(topicA, topicB).apply(topicA).equals(topicB)
 * </code></pre>
 */
class TopicDiff {

    private final ObjectMeta objectMeta;

    private static abstract class Difference {
        private Difference() {}

        protected abstract String address();

        protected abstract void apply(Topic.Builder builder);
    }

    private static class NumPartitionsDifference extends Difference {
        public static final String ADDRESS = "numPartitions";
        private final int oldNumPartitions;
        private final int newNumPartitions;

        public NumPartitionsDifference(int oldNumPartitions, int newNumPartitions) {
            this.oldNumPartitions = oldNumPartitions;
            this.newNumPartitions = newNumPartitions;
        }

        @Override
        public String address() {
            return ADDRESS;
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

        int numPartitionsChange() {
            // strictly speaking there's an issue here with when the - overflows
            return newNumPartitions - oldNumPartitions;
        }
    }

    private static class NumReplicasDifference extends Difference {
        public static final String ADDRESS = "numReplicas";
        private short newNumReplicas;

        public NumReplicasDifference(short newNumReplicas) {
            this.newNumReplicas = newNumReplicas;
        }

        @Override
        public String address() {
            return ADDRESS;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NumReplicasDifference that = (NumReplicasDifference) o;

            return newNumReplicas == that.newNumReplicas;
        }

        @Override
        public int hashCode() {
            return newNumReplicas;
        }

        @Override
        public String toString() {
            return "newNumReplicas=" + newNumReplicas;
        }

        @Override
        protected void apply(Topic.Builder builder) {
            builder.withNumReplicas(this.newNumReplicas);
        }
    }



    private static class AddedConfigEntry extends Difference {
        public static final String ADDRESS_PREFIX = "config:";
        private final String configKey;
        private final String configValue;

        public AddedConfigEntry(String configKey, String configValue) {
            if (configKey == null || configValue == null) {
                throw new IllegalArgumentException();
            }
            this.configKey = configKey;
            this.configValue = configValue;
        }

        @Override
        public String address() {
            return ADDRESS_PREFIX + configKey;
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
        public static final String ADDRESS_PREFIX = "config:";
        private String configKey;

        public RemovedConfigEntry(String configKey) {
            this.configKey = configKey;
        }

        @Override
        public String address() {
            return ADDRESS_PREFIX + configKey;
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

    private final Map<String, Difference> differences;

    protected TopicDiff(Map<String, Difference> differences, ObjectMeta objectMeta) {
        this.differences = differences;
        this.objectMeta = objectMeta;
    }

    /**
     * Return the TopicDiff that will transform the given source topic into the given target topic.
     *
     * @param source
     * @param target
     * @return The difference between the source and target.
     */
    protected static TopicDiff diff(Topic source, Topic target) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(target);
        Objects.requireNonNull(source.getTopicName());
        Objects.requireNonNull(target.getTopicName());
        if (!source.getTopicName().equals(target.getTopicName())) {
            throw new IllegalArgumentException("Kafka topics cannot be renamed, but KafkaTopic's spec.topicName has changed.");
        }
        Map<String, Difference> differences = new HashMap<>();
        if (target.getNumPartitions() != -1 && source.getNumPartitions() != target.getNumPartitions()) {
            NumPartitionsDifference numPartitionsDifference = new NumPartitionsDifference(source.getNumPartitions(), target.getNumPartitions());
            differences.put(numPartitionsDifference.address(), numPartitionsDifference);
        }
        if (target.getNumReplicas() != -1 && source.getNumReplicas() != target.getNumReplicas()) {
            NumReplicasDifference numReplicasDifference = new NumReplicasDifference(target.getNumReplicas());
            differences.put(numReplicasDifference.address(), numReplicasDifference);
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
        return new TopicDiff(differences, target.getMetadata());
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
     * Whether the diff is empty. Applying an empty diff is a noop.
     */
    protected boolean isEmpty() {
        return this.differences.isEmpty();
    }

    protected boolean changesNumPartitions() {
        return this.differences.containsKey(NumPartitionsDifference.ADDRESS);
    }

    protected int numPartitionsDelta() {
        NumPartitionsDifference newP = (NumPartitionsDifference) this.differences.get(NumPartitionsDifference.ADDRESS);
        return newP == null ? 0 : newP.numPartitionsChange();
    }

    protected boolean changesConfig() {
        for (String address : differences.keySet()) {
            if (address.startsWith(AddedConfigEntry.ADDRESS_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    protected boolean changesReplicationFactor() {
        return this.differences.containsKey(NumReplicasDifference.ADDRESS);
    }


    /**
     * Apply this diff to this given topic, returning a new topic.
     *
     * @param topic
     * @return
     */
    protected Topic apply(Topic topic) {
        Topic.Builder builder = new Topic.Builder(topic);
        for (Difference d : differences.values()) {
            d.apply(builder);
        }
        builder.withMetadata(objectMeta);
        return builder.build();
    }

    // TODO we actually need a higher level apply, that basically decomposes the differences into
    // a sequence of operations to be performed when updating one or both ends
    // this can be quite subtle. For example, if increasing the min.insync.replicas
    // and also increasing the number of replicas, we might be necessary to do
    // the partition reassignment before updating the config.

    /**
     * Return true if this TopicDiff conflicts with the given other TopicDiff
     */
    protected String conflict(TopicDiff other) {
        Set<String> intersection = new HashSet<>(this.differences.keySet());
        intersection.retainAll(other.differences.keySet());
        // they could still be OK if they're applying the _same_ differences
        StringBuilder sb = new StringBuilder();
        for (String address : intersection) {
            Difference difference = this.differences.get(address);
            Difference otherDifference = other.differences.get(address);
            if (!difference.equals(otherDifference)) {
                sb.append(address).append(", ");
            }
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    protected boolean conflicts(TopicDiff other) {
        return conflict(other) != null;
    }

    /**
     * Merge this TopicDiff with the given other TopicDiff, returning a
     * single diff which combines the two.
     *
     * @param other The diff to merge with.
     * @return
     * @throws IllegalArgumentException if the topics conflict.
     */
    protected TopicDiff merge(TopicDiff other) {
        String confict = this.conflict(other);
        if (confict != null) {
            throw new IllegalArgumentException("Conflict: " + confict);
        }
        Map<String, Difference> union = new HashMap<>(this.differences);
        union.putAll(other.differences);
        return new TopicDiff(union, other.objectMeta);
    }
}
