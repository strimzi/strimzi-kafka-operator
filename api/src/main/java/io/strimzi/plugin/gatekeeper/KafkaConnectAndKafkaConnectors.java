/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.gatekeeper;

import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connector.KafkaConnector;

import java.util.List;

/**
 * Wrapper holding a {@link KafkaConnect} custom resource together with its {@link KafkaConnector} resources. It is
 * returned by the entry method of the mutating Kafka Connect plugin, which can mutate both the {@code KafkaConnect}
 * resource and its connectors.
 *
 * @param kafkaConnect      The (possibly mutated) KafkaConnect custom resource
 * @param kafkaConnectors   The (possibly mutated) list of KafkaConnector resources belonging to the Kafka Connect cluster
 */
public record KafkaConnectAndKafkaConnectors(KafkaConnect kafkaConnect, List<KafkaConnector> kafkaConnectors) { }
