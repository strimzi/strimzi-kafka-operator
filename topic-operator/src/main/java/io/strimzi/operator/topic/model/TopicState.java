/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.model;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Topic state holder.
 * 
 * @param description Description.
 * @param configs Configurations.
 */
public record TopicState(TopicDescription description, Config configs) {
    /**
     * @return The number of partitions.
     */
    public int numPartitions() {
        return description.partitions().size();
    }

    /**
     * @return the unique replication factor for all partitions of this topic, or
     * {@link Integer#MIN_VALUE} if there is no unique replication factor.
     */
    public int uniqueReplicationFactor() {
        int uniqueRf = Integer.MIN_VALUE;
        for (var partition : description.partitions()) {
            int thisPartitionRf = partition.replicas().size();
            if (uniqueRf != Integer.MIN_VALUE && uniqueRf != thisPartitionRf) {
                return Integer.MIN_VALUE;
            }
            uniqueRf = thisPartitionRf;
        }
        return uniqueRf;
    }

    /**
     * @param rf Known replication factor.
     * @return Partition with different RF.
     */
    public Set<Integer> partitionsWithDifferentRfThan(int rf) {
        return description.partitions().stream()
            .filter(partition -> rf != partition.replicas().size())
            .map(TopicPartitionInfo::partition)
            .collect(Collectors.toSet());
    }
}
