/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.gatekeeper;

import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeStatus;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Validating Gatekeeper plugin for the {@link KafkaBridge} operand. The entry method is invoked at the start of a KafkaBridge
 * reconciliation and the exit method after it completes. Validating plugins do not mutate the resource; they can reject
 * the reconciliation by completing the returned stage exceptionally.
 * <p>
 * All methods have default no-op implementations, so a plugin only needs to override the methods it is interested in.
 */
public interface GatekeeperKafkaBridgeValidatingPlugin extends GatekeeperPlugin {
    /**
     * Invoked at the start of a KafkaBridge reconciliation. The default implementation does nothing.
     *
     * @param context   Context for the entry phase of the validating KafkaBridge plugin
     * @param kafkaBridge   The KafkaBridge custom resource being reconciled
     *
     * @return  A completion stage that completes when the validation is done
     */
    default CompletionStage<Void> kafkaBridgeEntry(GatekeeperKafkaBridgeEntryContext context, KafkaBridge kafkaBridge) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Invoked after a KafkaBridge reconciliation completes. The default implementation does nothing.
     *
     * @param context           Context for the exit phase of the validating KafkaBridge plugin
     * @param kafkaBridge           The KafkaBridge custom resource being reconciled
     * @param newKafkaBridgeStatus   The new status computed for the KafkaBridge resource
     *
     * @return  A completion stage that completes when the validation is done
     */
    default CompletionStage<Void> kafkaBridgeExit(GatekeeperKafkaBridgeExitContext context, KafkaBridge kafkaBridge, KafkaBridgeStatus newKafkaBridgeStatus) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Invoked when a {@link KafkaBridge} is being deleted. The default implementation does nothing. The hook cannot mutate
     * anything (the resource no longer exists); it can react to the deletion or reject it by completing the returned
     * stage exceptionally.
     *
     * @param context   Context for the deletion of the KafkaBridge plugin
     * @param namespace The namespace of the KafkaBridge being deleted
     * @param name      The name of the KafkaBridge being deleted
     *
     * @return  A completion stage that completes when the plugin is done handling the deletion
     */
    default CompletionStage<Void> kafkaBridgeDeletion(GatekeeperKafkaBridgeDeletionContext context, String namespace, String name) {
        return CompletableFuture.completedFuture(null);
    }
}
