/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.gatekeeper;

import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserStatus;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Mutating Gatekeeper plugin for the {@link KafkaUser} operand. The entry method is invoked at the start of a KafkaUser
 * reconciliation and can mutate the {@code KafkaUser} resource. The exit method is invoked after the reconciliation
 * completes and can mutate the status section of the {@code KafkaUser} resource.
 * <p>
 * All methods have default no-op implementations, so a plugin only needs to override the methods it is interested in.
 */
public interface GatekeeperKafkaUserMutatingPlugin extends GatekeeperPlugin {
    /**
     * Invoked at the start of a KafkaUser reconciliation. The default implementation returns the resource unchanged.
     *
     * @param context   Context for the entry phase of the mutating KafkaUser plugin
     * @param kafkaUser   The KafkaUser custom resource being reconciled
     *
     * @return  A completion stage with the (possibly mutated) KafkaUser resource
     */
    default CompletionStage<KafkaUser> kafkaUserEntry(GatekeeperKafkaUserEntryContext context, KafkaUser kafkaUser) {
        return CompletableFuture.completedFuture(kafkaUser);
    }

    /**
     * Invoked after a KafkaUser reconciliation completes. The default implementation returns the status unchanged.
     *
     * @param context           Context for the exit phase of the mutating KafkaUser plugin
     * @param kafkaUser           The KafkaUser custom resource being reconciled
     * @param newKafkaUserStatus   The new status computed for the KafkaUser resource
     *
     * @return  A completion stage with the (possibly mutated) KafkaUser status
     */
    default CompletionStage<KafkaUserStatus> kafkaUserExit(GatekeeperKafkaUserExitContext context, KafkaUser kafkaUser, KafkaUserStatus newKafkaUserStatus) {
        return CompletableFuture.completedFuture(newKafkaUserStatus);
    }

    /**
     * Invoked when a {@link KafkaUser} is being deleted. The default implementation does nothing. The hook cannot mutate
     * anything (the resource no longer exists); it can react to the deletion or reject it by completing the returned
     * stage exceptionally.
     *
     * @param context   Context for the deletion of the KafkaUser plugin
     * @param namespace The namespace of the KafkaUser being deleted
     * @param name      The name of the KafkaUser being deleted
     *
     * @return  A completion stage that completes when the plugin is done handling the deletion
     */
    default CompletionStage<Void> kafkaUserDeletion(GatekeeperKafkaUserDeletionContext context, String namespace, String name) {
        return CompletableFuture.completedFuture(null);
    }
}
