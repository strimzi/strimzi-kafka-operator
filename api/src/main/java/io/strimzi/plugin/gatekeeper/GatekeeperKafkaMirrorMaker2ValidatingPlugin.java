/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.gatekeeper;

import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Validating Gatekeeper plugin for the {@link KafkaMirrorMaker2} operand. The entry method is invoked at the start of a KafkaMirrorMaker2
 * reconciliation and the exit method after it completes. Validating plugins do not mutate the resource; they can reject
 * the reconciliation by completing the returned stage exceptionally.
 * <p>
 * All methods have default no-op implementations, so a plugin only needs to override the methods it is interested in.
 */
public interface GatekeeperKafkaMirrorMaker2ValidatingPlugin extends GatekeeperPlugin {
    /**
     * Invoked at the start of a KafkaMirrorMaker2 reconciliation. The default implementation does nothing.
     *
     * @param context   Context for the entry phase of the validating KafkaMirrorMaker2 plugin
     * @param kafkaMirrorMaker2   The KafkaMirrorMaker2 custom resource being reconciled
     *
     * @return  A completion stage that completes when the validation is done
     */
    default CompletionStage<Void> kafkaMirrorMaker2Entry(GatekeeperKafkaMirrorMaker2EntryContext context, KafkaMirrorMaker2 kafkaMirrorMaker2) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Invoked after a KafkaMirrorMaker2 reconciliation completes. The default implementation does nothing.
     *
     * @param context           Context for the exit phase of the validating KafkaMirrorMaker2 plugin
     * @param kafkaMirrorMaker2           The KafkaMirrorMaker2 custom resource being reconciled
     * @param newKafkaMirrorMaker2Status   The new status computed for the KafkaMirrorMaker2 resource
     *
     * @return  A completion stage that completes when the validation is done
     */
    default CompletionStage<Void> kafkaMirrorMaker2Exit(GatekeeperKafkaMirrorMaker2ExitContext context, KafkaMirrorMaker2 kafkaMirrorMaker2, KafkaMirrorMaker2Status newKafkaMirrorMaker2Status) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Invoked when a {@link KafkaMirrorMaker2} is being deleted. The default implementation does nothing. The hook cannot mutate
     * anything (the resource no longer exists); it can react to the deletion or reject it by completing the returned
     * stage exceptionally.
     *
     * @param context   Context for the deletion of the KafkaMirrorMaker2 plugin
     * @param namespace The namespace of the KafkaMirrorMaker2 being deleted
     * @param name      The name of the KafkaMirrorMaker2 being deleted
     *
     * @return  A completion stage that completes when the plugin is done handling the deletion
     */
    default CompletionStage<Void> kafkaMirrorMaker2Deletion(GatekeeperKafkaMirrorMaker2DeletionContext context, String namespace, String name) {
        return CompletableFuture.completedFuture(null);
    }
}
