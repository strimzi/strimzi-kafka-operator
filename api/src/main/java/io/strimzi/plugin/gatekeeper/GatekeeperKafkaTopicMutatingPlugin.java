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
 * Mutating Gatekeeper plugin for the {@link KafkaTopic} operand. The entry method is invoked at the start of a KafkaTopic
 * reconciliation and can mutate the {@code KafkaTopic} resource. The exit method is invoked after the reconciliation
 * completes and can mutate the status section of the {@code KafkaTopic} resource.
 * <p>
 * All methods have default no-op implementations, so a plugin only needs to override the methods it is interested in.
 */
public interface GatekeeperKafkaTopicMutatingPlugin extends GatekeeperPlugin {
    /**
     * Invoked at the start of a KafkaTopic reconciliation. The default implementation returns the resource unchanged.
     *
     * @param context   Context for the entry phase of the mutating KafkaTopic plugin
     * @param kafkaTopic   The KafkaTopic custom resource being reconciled
     *
     * @return  A completion stage with the (possibly mutated) KafkaTopic resource
     */
    default CompletionStage<KafkaTopic> kafkaTopicEntry(GatekeeperKafkaTopicEntryContext context, KafkaTopic kafkaTopic) {
        return CompletableFuture.completedFuture(kafkaTopic);
    }

    /**
     * Invoked after a KafkaTopic reconciliation completes. The default implementation returns the status unchanged.
     *
     * @param context           Context for the exit phase of the mutating KafkaTopic plugin
     * @param kafkaTopic           The KafkaTopic custom resource being reconciled
     * @param newKafkaTopicStatus   The new status computed for the KafkaTopic resource
     *
     * @return  A completion stage with the (possibly mutated) KafkaTopic status
     */
    default CompletionStage<KafkaTopicStatus> kafkaTopicExit(GatekeeperKafkaTopicExitContext context, KafkaTopic kafkaTopic, KafkaTopicStatus newKafkaTopicStatus) {
        return CompletableFuture.completedFuture(newKafkaTopicStatus);
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
