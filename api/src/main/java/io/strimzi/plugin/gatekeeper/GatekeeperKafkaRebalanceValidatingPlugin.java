/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.gatekeeper;

import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Validating Gatekeeper plugin for the {@link KafkaRebalance} operand. The entry method is invoked at the start of a KafkaRebalance
 * reconciliation and the exit method after it completes. Validating plugins do not mutate the resource; they can reject
 * the reconciliation by completing the returned stage exceptionally.
 * <p>
 * All methods have default no-op implementations, so a plugin only needs to override the methods it is interested in.
 */
public interface GatekeeperKafkaRebalanceValidatingPlugin extends GatekeeperPlugin {
    /**
     * Invoked at the start of a KafkaRebalance reconciliation. The default implementation does nothing.
     *
     * @param context   Context for the entry phase of the validating KafkaRebalance plugin
     * @param kafkaRebalance   The KafkaRebalance custom resource being reconciled
     *
     * @return  A completion stage that completes when the validation is done
     */
    default CompletionStage<Void> kafkaRebalanceEntry(GatekeeperKafkaRebalanceEntryContext context, KafkaRebalance kafkaRebalance) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Invoked after a KafkaRebalance reconciliation completes. The default implementation does nothing.
     *
     * @param context           Context for the exit phase of the validating KafkaRebalance plugin
     * @param kafkaRebalance           The KafkaRebalance custom resource being reconciled
     * @param newKafkaRebalanceStatus   The new status computed for the KafkaRebalance resource
     *
     * @return  A completion stage that completes when the validation is done
     */
    default CompletionStage<Void> kafkaRebalanceExit(GatekeeperKafkaRebalanceExitContext context, KafkaRebalance kafkaRebalance, KafkaRebalanceStatus newKafkaRebalanceStatus) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Invoked when a {@link KafkaRebalance} is being deleted. The default implementation does nothing. The hook cannot mutate
     * anything (the resource no longer exists); it can react to the deletion or reject it by completing the returned
     * stage exceptionally.
     *
     * @param context   Context for the deletion of the KafkaRebalance plugin
     * @param namespace The namespace of the KafkaRebalance being deleted
     * @param name      The name of the KafkaRebalance being deleted
     *
     * @return  A completion stage that completes when the plugin is done handling the deletion
     */
    default CompletionStage<Void> kafkaRebalanceDeletion(GatekeeperKafkaRebalanceDeletionContext context, String namespace, String name) {
        return CompletableFuture.completedFuture(null);
    }
}
