/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.gatekeeper.plugin;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.utils.Serialization;
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
 * Gatekeeper plugin useful for debugging. For every entry, exit, and deletion it logs a short human-readable
 * message at the INFO level.
 * <p>
 * Optionally, it can dump the full resource as YAML, but only when the custom resource opts into it through an
 * annotation.
 * <ul>
 *     <li>On <em>entry</em>, the custom resource is logged as YAML when it has the
 *     {@code debug.gatekeeper.strimzi.io/entry: true} annotation.</li>
 *     <li>On <em>exit</em>, the status of the custom resource is logged as YAML when it has the
 *     {@code debug.gatekeeper.strimzi.io/exit: true} annotation.</li>
 * </ul>
 * <p>
 * This plugin also serves as an example of validating plugins.
 */
@SuppressWarnings("checkstyle:ClassFanOutComplexity") // This class intentionally references the types of all the operands, which raises its fan-out above the limit
public class DebugGatekeeperPlugin implements
        GatekeeperKafkaValidatingPlugin,
        GatekeeperKafkaConnectValidatingPlugin,
        GatekeeperKafkaMirrorMaker2ValidatingPlugin,
        GatekeeperKafkaBridgeValidatingPlugin,
        GatekeeperKafkaRebalanceValidatingPlugin,
        GatekeeperKafkaTopicValidatingPlugin,
        GatekeeperKafkaUserValidatingPlugin {
    private static final Logger LOGGER = LogManager.getLogger(DebugGatekeeperPlugin.class);

    /**
     * Prefix added to every message logged by this plugin, so the debug output is easy to find and filter.
     */
    /* test */ static final String LOG_PREFIX = "[GATEKEEPER] ";

    /**
     * Annotation which, when set to {@code true} on a custom resource, makes the plugin log the resource as YAML during
     * the entry phase.
     */
    /* test */ static final String ENTRY_DEBUG_ANNOTATION = "debug.gatekeeper.strimzi.io/entry";

    /**
     * Annotation which, when set to {@code true} on a custom resource, makes the plugin log the resource status as YAML
     * during the exit phase.
     */
    /* test */ static final String EXIT_DEBUG_ANNOTATION = "debug.gatekeeper.strimzi.io/exit";

    /**
     * Creates the debug plugin.
     */
    public DebugGatekeeperPlugin() { }

    // Kafka

    @Override
    public CompletionStage<Void> kafkaEntry(GatekeeperKafkaEntryContext context, Kafka kafka, List<KafkaNodePool> kafkaNodePools) {
        logEntry(kafka);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaExit(GatekeeperKafkaExitContext context, Kafka kafka, List<KafkaNodePool> kafkaNodePools, KafkaStatus newKafkaStatus) {
        logExit(kafka, newKafkaStatus);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaNodePoolExit(GatekeeperKafkaExitContext context, Kafka kafka, KafkaNodePool kafkaNodePool, KafkaNodePoolStatus newKafkaNodePoolStatus) {
        logExit(kafkaNodePool, newKafkaNodePoolStatus);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaDeletion(GatekeeperKafkaDeletionContext context, String namespace, String name) {
        logDeletion("Kafka", namespace, name);
        return CompletableFuture.completedFuture(null);
    }

    // KafkaConnect

    @Override
    public CompletionStage<Void> kafkaConnectEntry(GatekeeperKafkaConnectEntryContext context, KafkaConnect kafkaConnect, List<KafkaConnector> kafkaConnectors) {
        logEntry(kafkaConnect);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaConnectExit(GatekeeperKafkaConnectExitContext context, KafkaConnect kafkaConnect, List<KafkaConnector> kafkaConnectors, KafkaConnectStatus newKafkaConnectStatus) {
        logExit(kafkaConnect, newKafkaConnectStatus);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaConnectorExit(GatekeeperKafkaConnectExitContext context, KafkaConnect kafkaConnect, KafkaConnector kafkaConnector, KafkaConnectorStatus newKafkaConnectorStatus) {
        logExit(kafkaConnector, newKafkaConnectorStatus);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaConnectDeletion(GatekeeperKafkaConnectDeletionContext context, String namespace, String name) {
        logDeletion("KafkaConnect", namespace, name);
        return CompletableFuture.completedFuture(null);
    }

    // KafkaMirrorMaker2

    @Override
    public CompletionStage<Void> kafkaMirrorMaker2Entry(GatekeeperKafkaMirrorMaker2EntryContext context, KafkaMirrorMaker2 kafkaMirrorMaker2) {
        logEntry(kafkaMirrorMaker2);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaMirrorMaker2Exit(GatekeeperKafkaMirrorMaker2ExitContext context, KafkaMirrorMaker2 kafkaMirrorMaker2, KafkaMirrorMaker2Status newKafkaMirrorMaker2Status) {
        logExit(kafkaMirrorMaker2, newKafkaMirrorMaker2Status);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaMirrorMaker2Deletion(GatekeeperKafkaMirrorMaker2DeletionContext context, String namespace, String name) {
        logDeletion("KafkaMirrorMaker2", namespace, name);
        return CompletableFuture.completedFuture(null);
    }

    // KafkaBridge

    @Override
    public CompletionStage<Void> kafkaBridgeEntry(GatekeeperKafkaBridgeEntryContext context, KafkaBridge kafkaBridge) {
        logEntry(kafkaBridge);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaBridgeExit(GatekeeperKafkaBridgeExitContext context, KafkaBridge kafkaBridge, KafkaBridgeStatus newKafkaBridgeStatus) {
        logExit(kafkaBridge, newKafkaBridgeStatus);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaBridgeDeletion(GatekeeperKafkaBridgeDeletionContext context, String namespace, String name) {
        logDeletion("KafkaBridge", namespace, name);
        return CompletableFuture.completedFuture(null);
    }

    // KafkaRebalance

    @Override
    public CompletionStage<Void> kafkaRebalanceEntry(GatekeeperKafkaRebalanceEntryContext context, KafkaRebalance kafkaRebalance) {
        logEntry(kafkaRebalance);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaRebalanceExit(GatekeeperKafkaRebalanceExitContext context, KafkaRebalance kafkaRebalance, KafkaRebalanceStatus newKafkaRebalanceStatus) {
        logExit(kafkaRebalance, newKafkaRebalanceStatus);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaRebalanceDeletion(GatekeeperKafkaRebalanceDeletionContext context, String namespace, String name) {
        logDeletion("KafkaRebalance", namespace, name);
        return CompletableFuture.completedFuture(null);
    }

    // KafkaTopic

    @Override
    public CompletionStage<Void> kafkaTopicEntry(GatekeeperKafkaTopicEntryContext context, KafkaTopic kafkaTopic) {
        logEntry(kafkaTopic);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaTopicExit(GatekeeperKafkaTopicExitContext context, KafkaTopic kafkaTopic, KafkaTopicStatus newKafkaTopicStatus) {
        logExit(kafkaTopic, newKafkaTopicStatus);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaTopicDeletion(GatekeeperKafkaTopicDeletionContext context, String namespace, String name) {
        logDeletion("KafkaTopic", namespace, name);
        return CompletableFuture.completedFuture(null);
    }

    // KafkaUser

    @Override
    public CompletionStage<Void> kafkaUserEntry(GatekeeperKafkaUserEntryContext context, KafkaUser kafkaUser) {
        logEntry(kafkaUser);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaUserExit(GatekeeperKafkaUserExitContext context, KafkaUser kafkaUser, KafkaUserStatus newKafkaUserStatus) {
        logExit(kafkaUser, newKafkaUserStatus);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> kafkaUserDeletion(GatekeeperKafkaUserDeletionContext context, String namespace, String name) {
        logDeletion("KafkaUser", namespace, name);
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Logs the entry event for a custom resource. When the resource carries the debug annotation, the resource itself
     * is also logged as YAML.
     *
     * @param resource  The custom resource entering the reconciliation
     */
    private void logEntry(HasMetadata resource) {
        String yamlMessage = "";
        if (Annotations.booleanAnnotation(resource, ENTRY_DEBUG_ANNOTATION, false)) {
            yamlMessage = yamlMessage("resource", resource);
        }

        LOGGER.info(eventMessage(resource, "entered the reconciliation", yamlMessage));
    }

    /**
     * Logs the exit event for a custom resource. When the resource carries the debug annotation, the resource status
     * is also logged as YAML.
     *
     * @param resource  The custom resource exiting the reconciliation
     * @param status    The status computed for the custom resource (might be null)
     */
    private void logExit(HasMetadata resource, Object status) {
        String yamlMessage = "";
        if (Annotations.booleanAnnotation(resource, EXIT_DEBUG_ANNOTATION, false)) {
            yamlMessage = yamlMessage("status", status);
        }

        LOGGER.info(eventMessage(resource, "exited the reconciliation", yamlMessage));
    }

    /**
     * Logs the deletion event for a custom resource.
     *
     * @param kind          The kind of the operand being deleted (for example {@code KafkaUser})
     * @param namespace     The namespace of the resource being deleted
     * @param name          The name of the resource being deleted
     */
    private void logDeletion(String kind, String namespace, String name) {
        LOGGER.info(deletionMessage(kind, namespace, name));
    }

    /**
     * Builds the short event message describing what is happening to a resource. The message is prefixed with
     * {@link #LOG_PREFIX} and includes the kind, namespace, and name of the resource.
     *
     * @param resource          The custom resource the event relates to
     * @param eventDescription  The event being described
     * @param yaml              The YAML representation of the resource
     *
     * @return  The formatted event message
     */
    /* test */ static String eventMessage(HasMetadata resource, String eventDescription, String yaml) {
        return LOG_PREFIX + resource.getKind() + " " + resource.getMetadata().getNamespace() + "/" + resource.getMetadata().getName() + " " + eventDescription + yaml;
    }

    /**
     * Builds the message with the given resource or status serialized as YAML.
     *
     * @param what      What is being serialized (for example {@code resource} or {@code status})
     * @param object    The object to serialize as YAML
     *
     * @return  The formatted YAML message
     */
    /* test */ static String yamlMessage(String what, Object object) {
        return " with " + what + ":" + System.lineSeparator() + Serialization.asYaml(object) + System.lineSeparator();
    }

    /**
     * Builds the message for a deletion. There is no resource to serialize (it no longer exists), so only the kind,
     * namespace, and name are included.
     *
     * @param kind          The kind of the operand being deleted (for example {@code KafkaUser})
     * @param namespace     The namespace of the resource being deleted
     * @param name          The name of the resource being deleted
     *
     * @return  The formatted deletion message
     */
    /* test */ static String deletionMessage(String kind, String namespace, String name) {
        return LOG_PREFIX + " " + kind + " " + namespace + "/" + name + " is being deleted";
    }
}
