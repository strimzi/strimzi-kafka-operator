/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients.admin;

import com.fasterxml.jackson.annotation.JsonAlias;

/**
 * Represents a description of a Kafka topic used for admin-client.
 *
 * @param name The name of the Kafka topic.
 * @param partitionCount The number of partitions in the Kafka topic.
 * @param replicaCount The number of replicas of the Kafka topic.
 */
public record KafkaTopicDescription(
    @JsonAlias("name") String name,
    @JsonAlias("partitionCount") int partitionCount,
    @JsonAlias("replicaCount") int replicaCount) {
}
