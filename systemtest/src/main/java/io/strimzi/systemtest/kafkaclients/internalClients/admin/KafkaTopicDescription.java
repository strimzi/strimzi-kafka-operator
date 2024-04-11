/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients.admin;

import com.fasterxml.jackson.annotation.JsonAlias;

public record KafkaTopicDescription(
    @JsonAlias({"name"}) String name,
    @JsonAlias("partitionCount") int partitionCount,
    @JsonAlias("replicaCount") int replicaCount) {
}
