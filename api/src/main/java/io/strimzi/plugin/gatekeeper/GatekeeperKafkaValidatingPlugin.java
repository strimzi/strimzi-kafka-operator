/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.gatekeeper;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Validating Gatekeeper plugin for the {@link Kafka} operand and its {@link KafkaNodePool} resources. The entry method
 * is invoked at the start of a Kafka reconciliation and the exit methods after it completes. Validating plugins do not
 * mutate the resources; they can reject the reconciliation by completing the returned stage exceptionally.
 * <p>
 * All methods have default no-op implementations, so a plugin only needs to override the methods it is interested in.
 */
public interface GatekeeperKafkaValidatingPlugin extends GatekeeperPlugin {
    /**
     * Invoked at the start of a Kafka reconciliation. The default implementation does nothing.
     *
     * @param context           Context for the entry phase of the validating Kafka plugin
     * @param kafka             The Kafka custom resource being reconciled
     * @param kafkaNodePools    The list of KafkaNodePool resources belonging to the Kafka cluster
     *
     * @return  A completion stage that completes when the validation is done
     */
    default CompletionStage<Void> kafkaEntry(GatekeeperKafkaEntryContext context, Kafka kafka, List<KafkaNodePool> kafkaNodePools) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Invoked after a Kafka reconciliation completes, once for each KafkaNodePool. The default implementation does
     * nothing.
     *
     * @param context                   Context for the exit phase of the validating Kafka plugin
     * @param kafka                     The Kafka custom resource being reconciled
     * @param kafkaNodePool             The KafkaNodePool resource whose status is being validated
     * @param newKafkaNodePoolStatus    The new status computed for the KafkaNodePool resource
     *
     * @return  A completion stage that completes when the validation is done
     */
    default CompletionStage<Void> kafkaNodePoolExit(GatekeeperKafkaExitContext context, Kafka kafka, KafkaNodePool kafkaNodePool, KafkaNodePoolStatus newKafkaNodePoolStatus) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Invoked after a Kafka reconciliation completes. The default implementation does nothing.
     *
     * @param context           Context for the exit phase of the validating Kafka plugin
     * @param kafka             The Kafka custom resource being reconciled
     * @param kafkaNodePools    The list of KafkaNodePool resources belonging to the Kafka cluster
     * @param newKafkaStatus    The new status computed for the Kafka resource
     *
     * @return  A completion stage that completes when the validation is done
     */
    default CompletionStage<Void> kafkaExit(GatekeeperKafkaExitContext context, Kafka kafka, List<KafkaNodePool> kafkaNodePools, KafkaStatus newKafkaStatus) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Invoked when a {@link Kafka} is being deleted. The default implementation does nothing. The hook cannot mutate
     * anything (the resource no longer exists); it can react to the deletion or reject it by completing the returned
     * stage exceptionally.
     *
     * @param context   Context for the deletion of the Kafka plugin
     * @param namespace The namespace of the Kafka being deleted
     * @param name      The name of the Kafka being deleted
     *
     * @return  A completion stage that completes when the plugin is done handling the deletion
     */
    default CompletionStage<Void> kafkaDeletion(GatekeeperKafkaDeletionContext context, String namespace, String name) {
        return CompletableFuture.completedFuture(null);
    }
}
