/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.gatekeeper.plugin;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatus;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaBridgeDeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaBridgeEntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaBridgeExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaBridgeValidatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaConnectDeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaConnectEntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaConnectExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaConnectValidatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaDeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaEntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaMirrorMaker2DeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaMirrorMaker2EntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaMirrorMaker2ExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaMirrorMaker2ValidatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaRebalanceDeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaRebalanceEntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaRebalanceExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaRebalanceValidatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaTopicDeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaTopicEntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaTopicExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaTopicValidatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserDeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserEntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserValidatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaValidatingPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Gatekeeper plugin useful for testing how the operators handle failures of the Gatekeeper plugins. It is a
 * validating plugin that never modifies the resources. It only completes the returned stage exceptionally to
 * simulate a plugin rejecting a resource.
 * <p>
 * The entry and exit failures are opted into per-resource through annotations, so a failure can be injected for a single
 * resource being tested without affecting the others:
 * <ul>
 *     <li>The <em>entry</em> method fails when the custom resource has the
 *     {@code injected.failure.gatekeeper.strimzi.io/entry: true} annotation.</li>
 *     <li>The <em>exit</em> method fails when the custom resource has the
 *     {@code injected.failure.gatekeeper.strimzi.io/exit: true} annotation.</li>
 * </ul>
 * <p>
 * The deletion hook does not receive the resource (only its namespace and name), so it cannot read an
 * annotation. It therefore fails unconditionally whenever the plugin is enabled.
 * <p>
 * This plugin also serves as an example of how validating plugins can reject resources.
 */
@SuppressWarnings("checkstyle:ClassFanOutComplexity") // This class intentionally references the types of all the operands, which raises its fan-out above the limit
public class InjectedFailureGatekeeperPlugin implements
        GatekeeperKafkaValidatingPlugin,
        GatekeeperKafkaConnectValidatingPlugin,
        GatekeeperKafkaMirrorMaker2ValidatingPlugin,
        GatekeeperKafkaBridgeValidatingPlugin,
        GatekeeperKafkaRebalanceValidatingPlugin,
        GatekeeperKafkaTopicValidatingPlugin,
        GatekeeperKafkaUserValidatingPlugin {
    private static final Logger LOGGER = LogManager.getLogger(InjectedFailureGatekeeperPlugin.class);

    /**
     * Annotation which, when set to {@code true} on a custom resource, makes the plugin fail during the entry phase.
     */
    /* test */ static final String ENTRY_FAILURE_ANNOTATION = "injected.failure.gatekeeper.strimzi.io/entry";

    /**
     * Annotation which, when set to {@code true} on a custom resource, makes the plugin fail during the exit phase.
     */
    /* test */ static final String EXIT_FAILURE_ANNOTATION = "injected.failure.gatekeeper.strimzi.io/exit";

    /**
     * Creates the injected failure plugin.
     */
    public InjectedFailureGatekeeperPlugin() { }

    // Kafka

    @Override
    public CompletionStage<Void> kafkaEntry(GatekeeperKafkaEntryContext context, Kafka kafka, List<KafkaNodePool> kafkaNodePools) {
        return maybeFailEntry(kafka);
    }

    @Override
    public CompletionStage<Void> kafkaExit(GatekeeperKafkaExitContext context, Kafka kafka, List<KafkaNodePool> kafkaNodePools, KafkaStatus newKafkaStatus) {
        return maybeFailExit(kafka);
    }

    @Override
    public CompletionStage<Void> kafkaNodePoolExit(GatekeeperKafkaExitContext context, Kafka kafka, KafkaNodePool kafkaNodePool, KafkaNodePoolStatus newKafkaNodePoolStatus) {
        return maybeFailExit(kafkaNodePool);
    }

    @Override
    public CompletionStage<Void> kafkaDeletion(GatekeeperKafkaDeletionContext context, String namespace, String name) {
        return failDeletion("Kafka", namespace, name);
    }

    // KafkaConnect

    @Override
    public CompletionStage<Void> kafkaConnectEntry(GatekeeperKafkaConnectEntryContext context, KafkaConnect kafkaConnect, List<KafkaConnector> kafkaConnectors) {
        return maybeFailEntry(kafkaConnect);
    }

    @Override
    public CompletionStage<Void> kafkaConnectExit(GatekeeperKafkaConnectExitContext context, KafkaConnect kafkaConnect, List<KafkaConnector> kafkaConnectors, KafkaConnectStatus newKafkaConnectStatus) {
        return maybeFailExit(kafkaConnect);
    }

    @Override
    public CompletionStage<Void> kafkaConnectorExit(GatekeeperKafkaConnectExitContext context, KafkaConnect kafkaConnect, KafkaConnector kafkaConnector, KafkaConnectorStatus newKafkaConnectorStatus) {
        return maybeFailExit(kafkaConnector);
    }

    @Override
    public CompletionStage<Void> kafkaConnectDeletion(GatekeeperKafkaConnectDeletionContext context, String namespace, String name) {
        return failDeletion("KafkaConnect", namespace, name);
    }

    // KafkaMirrorMaker2

    @Override
    public CompletionStage<Void> kafkaMirrorMaker2Entry(GatekeeperKafkaMirrorMaker2EntryContext context, KafkaMirrorMaker2 kafkaMirrorMaker2) {
        return maybeFailEntry(kafkaMirrorMaker2);
    }

    @Override
    public CompletionStage<Void> kafkaMirrorMaker2Exit(GatekeeperKafkaMirrorMaker2ExitContext context, KafkaMirrorMaker2 kafkaMirrorMaker2, KafkaMirrorMaker2Status newKafkaMirrorMaker2Status) {
        return maybeFailExit(kafkaMirrorMaker2);
    }

    @Override
    public CompletionStage<Void> kafkaMirrorMaker2Deletion(GatekeeperKafkaMirrorMaker2DeletionContext context, String namespace, String name) {
        return failDeletion("KafkaMirrorMaker2", namespace, name);
    }

    // KafkaBridge

    @Override
    public CompletionStage<Void> kafkaBridgeEntry(GatekeeperKafkaBridgeEntryContext context, KafkaBridge kafkaBridge) {
        return maybeFailEntry(kafkaBridge);
    }

    @Override
    public CompletionStage<Void> kafkaBridgeExit(GatekeeperKafkaBridgeExitContext context, KafkaBridge kafkaBridge, KafkaBridgeStatus newKafkaBridgeStatus) {
        return maybeFailExit(kafkaBridge);
    }

    @Override
    public CompletionStage<Void> kafkaBridgeDeletion(GatekeeperKafkaBridgeDeletionContext context, String namespace, String name) {
        return failDeletion("KafkaBridge", namespace, name);
    }

    // KafkaRebalance

    @Override
    public CompletionStage<Void> kafkaRebalanceEntry(GatekeeperKafkaRebalanceEntryContext context, KafkaRebalance kafkaRebalance) {
        return maybeFailEntry(kafkaRebalance);
    }

    @Override
    public CompletionStage<Void> kafkaRebalanceExit(GatekeeperKafkaRebalanceExitContext context, KafkaRebalance kafkaRebalance, KafkaRebalanceStatus newKafkaRebalanceStatus) {
        return maybeFailExit(kafkaRebalance);
    }

    @Override
    public CompletionStage<Void> kafkaRebalanceDeletion(GatekeeperKafkaRebalanceDeletionContext context, String namespace, String name) {
        return failDeletion("KafkaRebalance", namespace, name);
    }

    // KafkaTopic

    @Override
    public CompletionStage<Void> kafkaTopicEntry(GatekeeperKafkaTopicEntryContext context, KafkaTopic kafkaTopic) {
        return maybeFailEntry(kafkaTopic);
    }

    @Override
    public CompletionStage<Void> kafkaTopicExit(GatekeeperKafkaTopicExitContext context, KafkaTopic kafkaTopic, KafkaTopicStatus newKafkaTopicStatus) {
        return maybeFailExit(kafkaTopic);
    }

    @Override
    public CompletionStage<Void> kafkaTopicDeletion(GatekeeperKafkaTopicDeletionContext context, String namespace, String name) {
        return failDeletion("KafkaTopic", namespace, name);
    }

    // KafkaUser

    @Override
    public CompletionStage<Void> kafkaUserEntry(GatekeeperKafkaUserEntryContext context, KafkaUser kafkaUser) {
        return maybeFailEntry(kafkaUser);
    }

    @Override
    public CompletionStage<Void> kafkaUserExit(GatekeeperKafkaUserExitContext context, KafkaUser kafkaUser, KafkaUserStatus newKafkaUserStatus) {
        return maybeFailExit(kafkaUser);
    }

    @Override
    public CompletionStage<Void> kafkaUserDeletion(GatekeeperKafkaUserDeletionContext context, String namespace, String name) {
        return failDeletion("KafkaUser", namespace, name);
    }

    /**
     * Fails the entry phase when the resource carries the annotation. Otherwise, the returned stage completes normally.
     *
     * @param resource  The custom resource entering the reconciliation
     *
     * @return  A completion stage that completes exceptionally when the failure is injected, normally otherwise
     */
    private CompletionStage<Void> maybeFailEntry(HasMetadata resource) {
        return maybeFail(resource, ENTRY_FAILURE_ANNOTATION, "entry");
    }

    /**
     * Fails the exit phase when the resource carries the annotation. Otherwise, the returned stage completes normally.
     *
     * @param resource  The custom resource exiting the reconciliation
     *
     * @return  A completion stage that completes exceptionally when the failure is injected, normally otherwise
     */
    private CompletionStage<Void> maybeFailExit(HasMetadata resource) {
        return maybeFail(resource, EXIT_FAILURE_ANNOTATION, "exit");
    }

    /**
     * Fails the given phase when the resource carries the given annotation set to true. Otherwise, the returned
     * stage completes normally.
     *
     * @param resource      The custom resource being reconciled
     * @param annotation    The annotation that enables the failure
     * @param phase         The reconciliation phase (used in the log and exception messages)
     *
     * @return  A completion stage that completes exceptionally when the failure is injected, normally otherwise
     */
    private CompletionStage<Void> maybeFail(HasMetadata resource, String annotation, String phase) {
        if (Annotations.booleanAnnotation(resource, annotation, false)) {
            LOGGER.warn("[GATEKEEPER] Injecting a Gatekeeper failure for {} {}/{} during the {} phase", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName(), phase);
            return CompletableFuture.failedFuture(new RuntimeException("Injected Gatekeeper failure for " + resource.getKind() + " " + resource.getMetadata().getNamespace() + "/" + resource.getMetadata().getName() + " during the " + phase + " phase"));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Fails the deletion hook unconditionally. The deletion hook does not receive the resource (only its namespace and
     * name), so it cannot read an annotation and always injects the failure.
     *
     * @param kind          The kind of the operand being deleted
     * @param namespace     The namespace of the resource being deleted
     * @param name          The name of the resource being deleted
     *
     * @return  A completion stage that always completes exceptionally
     */
    private CompletionStage<Void> failDeletion(String kind, String namespace, String name) {
        LOGGER.warn("[GATEKEEPER] Injecting a Gatekeeper failure for {} {}/{} during the deletion phase", kind, namespace, name);
        return CompletableFuture.failedFuture(new RuntimeException("Injected Gatekeeper failure for " + kind + " " + namespace + "/" + name + " during the deletion phase"));
    }
}
