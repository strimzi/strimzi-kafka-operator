/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.gatekeeper;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;

import java.util.List;

/**
 * Wrapper holding a {@link Kafka} custom resource together with its {@link KafkaNodePool} resources. It is returned by
 * the entry method of the mutating Kafka plugin, which can mutate both the {@code Kafka} resource and its node pools.
 *
 * @param kafka             The (possibly mutated) Kafka custom resource
 * @param kafkaNodePools    The (possibly mutated) list of KafkaNodePool resources belonging to the Kafka cluster
 */
public record KafkaAndKafkaNodePools(Kafka kafka, List<KafkaNodePool> kafkaNodePools) { }
