/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.gatekeeper;

import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorStatus;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Mutating Gatekeeper plugin for the {@link KafkaConnect} operand and its {@link KafkaConnector} resources. The entry
 * method is invoked at the start of a Kafka Connect reconciliation and can mutate the {@code KafkaConnect} resource and
 * its connectors. The exit methods are invoked after the reconciliation completes and can mutate the status sections of
 * the {@code KafkaConnect} resource and its connectors.
 * <p>
 * All methods have default no-op implementations, so a plugin only needs to override the methods it is interested in.
 */
public interface GatekeeperKafkaConnectMutatingPlugin extends GatekeeperPlugin {
    /**
     * Invoked at the start of a Kafka Connect reconciliation. The default implementation returns the resources unchanged.
     *
     * @param context           Context for the entry phase of the mutating Kafka Connect plugin
     * @param kafkaConnect      The KafkaConnect custom resource being reconciled
     * @param kafkaConnectors   The list of KafkaConnector resources belonging to the Kafka Connect cluster
     *
     * @return  A completion stage with the (possibly mutated) KafkaConnect resource and its connectors
     */
    default CompletionStage<KafkaConnectAndKafkaConnectors> kafkaConnectEntry(GatekeeperKafkaConnectEntryContext context, KafkaConnect kafkaConnect, List<KafkaConnector> kafkaConnectors) {
        return CompletableFuture.completedFuture(new KafkaConnectAndKafkaConnectors(kafkaConnect, kafkaConnectors));
    }

    /**
     * Invoked after a Kafka Connect reconciliation completes, once for each KafkaConnector. The default implementation
     * returns the status unchanged.
     *
     * @param context                   Context for the exit phase of the mutating Kafka Connect plugin
     * @param kafkaConnect              The KafkaConnect custom resource being reconciled
     * @param kafkaConnector            The KafkaConnector resource whose status is being updated
     * @param newKafkaConnectorStatus   The new status computed for the KafkaConnector resource
     *
     * @return  A completion stage with the (possibly mutated) KafkaConnector status
     */
    default CompletionStage<KafkaConnectorStatus> kafkaConnectorExit(GatekeeperKafkaConnectExitContext context, KafkaConnect kafkaConnect, KafkaConnector kafkaConnector, KafkaConnectorStatus newKafkaConnectorStatus) {
        return CompletableFuture.completedFuture(newKafkaConnectorStatus);
    }

    /**
     * Invoked after a Kafka Connect reconciliation completes. The default implementation returns the status unchanged.
     *
     * @param context               Context for the exit phase of the mutating Kafka Connect plugin
     * @param kafkaConnect          The KafkaConnect custom resource being reconciled
     * @param kafkaConnectors       The list of KafkaConnector resources belonging to the Kafka Connect cluster
     * @param newKafkaConnectStatus The new status computed for the KafkaConnect resource
     *
     * @return  A completion stage with the (possibly mutated) KafkaConnect status
     */
    default CompletionStage<KafkaConnectStatus> kafkaConnectExit(GatekeeperKafkaConnectExitContext context, KafkaConnect kafkaConnect, List<KafkaConnector> kafkaConnectors, KafkaConnectStatus newKafkaConnectStatus) {
        return CompletableFuture.completedFuture(newKafkaConnectStatus);
    }

    /**
     * Invoked when a {@link KafkaConnect} is being deleted. The default implementation does nothing. The hook cannot mutate
     * anything (the resource no longer exists); it can react to the deletion or reject it by completing the returned
     * stage exceptionally.
     *
     * @param context   Context for the deletion of the KafkaConnect plugin
     * @param namespace The namespace of the KafkaConnect being deleted
     * @param name      The name of the KafkaConnect being deleted
     *
     * @return  A completion stage that completes when the plugin is done handling the deletion
     */
    default CompletionStage<Void> kafkaConnectDeletion(GatekeeperKafkaConnectDeletionContext context, String namespace, String name) {
        return CompletableFuture.completedFuture(null);
    }
}
