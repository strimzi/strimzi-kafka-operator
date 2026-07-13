/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.gatekeeper;

import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatus;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Validating Gatekeeper plugin for the {@link KafkaTopic} operand. The entry method is invoked at the start of a KafkaTopic
 * reconciliation and the exit method after it completes. Validating plugins do not mutate the resource; they can reject
 * the reconciliation by completing the returned stage exceptionally.
 * <p>
 * All methods have default no-op implementations, so a plugin only needs to override the methods it is interested in.
 */
public interface GatekeeperKafkaTopicValidatingPlugin extends GatekeeperPlugin {
    /**
     * Invoked at the start of a KafkaTopic reconciliation. The default implementation does nothing.
     *
     * @param context   Context for the entry phase of the validating KafkaTopic plugin
     * @param kafkaTopic   The KafkaTopic custom resource being reconciled
     *
     * @return  A completion stage that completes when the validation is done
     */
    default CompletionStage<Void> kafkaTopicEntry(GatekeeperKafkaTopicEntryContext context, KafkaTopic kafkaTopic) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Invoked after a KafkaTopic reconciliation completes. The default implementation does nothing.
     *
     * @param context           Context for the exit phase of the validating KafkaTopic plugin
     * @param kafkaTopic           The KafkaTopic custom resource being reconciled
     * @param newKafkaTopicStatus   The new status computed for the KafkaTopic resource
     *
     * @return  A completion stage that completes when the validation is done
     */
    default CompletionStage<Void> kafkaTopicExit(GatekeeperKafkaTopicExitContext context, KafkaTopic kafkaTopic, KafkaTopicStatus newKafkaTopicStatus) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Invoked when a {@link KafkaTopic} is being deleted. The default implementation does nothing. The hook cannot mutate
     * anything (the resource no longer exists); it can react to the deletion or reject it by completing the returned
     * stage exceptionally.
     *
     * @param context   Context for the deletion of the KafkaTopic plugin
     * @param namespace The namespace of the KafkaTopic being deleted
     * @param name      The name of the KafkaTopic being deleted
     *
     * @return  A completion stage that completes when the plugin is done handling the deletion
     */
    default CompletionStage<Void> kafkaTopicDeletion(GatekeeperKafkaTopicDeletionContext context, String namespace, String name) {
        return CompletableFuture.completedFuture(null);
    }
}
