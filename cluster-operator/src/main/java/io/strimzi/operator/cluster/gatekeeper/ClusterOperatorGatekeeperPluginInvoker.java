/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.gatekeeper;

import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeStatusBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatusBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.connector.KafkaConnectorStatusBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.KafkaStatusBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2StatusBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatusBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatusBuilder;
import io.strimzi.operator.common.gatekeeper.AbstractGatekeeperPluginInvoker;
import io.strimzi.operator.common.gatekeeper.GatekeeperPluginFactory;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaBridgeDeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaBridgeEntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaBridgeExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaBridgeMutatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaBridgeValidatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaConnectDeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaConnectEntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaConnectExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaConnectMutatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaConnectValidatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaDeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaEntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaMirrorMaker2DeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaMirrorMaker2EntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaMirrorMaker2ExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaMirrorMaker2MutatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaMirrorMaker2ValidatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaMutatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaRebalanceDeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaRebalanceEntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaRebalanceExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaRebalanceMutatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaRebalanceValidatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaValidatingPlugin;
import io.strimzi.plugin.gatekeeper.KafkaAndKafkaNodePools;
import io.strimzi.plugin.gatekeeper.KafkaConnectAndKafkaConnectors;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Invokes the Strimzi Gatekeeper plugins for Kafka (+KafkaNodePools), KafkaConnect (+KafkaConnectors),
 * KafkaMirrorMaker2, KafkaBridge, and KafkaRebalance reconciliations. It provides entry, exit, and deletion methods
 * which invoke all the Kafka, KafkaConnect, KafkaMirrorMaker2, KafkaBridge, and KafkaRebalance plugins (both mutating
 * and validating) as one ordered chain. The plugins (and their order) are provided by the {@link GatekeeperPluginFactory}.
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class ClusterOperatorGatekeeperPluginInvoker extends AbstractGatekeeperPluginInvoker {
    private ClusterOperatorGatekeeperPluginInvoker() { }

    /**
     * Invokes the entry of all the KafkaMirrorMaker2 plugins (mutating and validating) as a single ordered chain. Mutating plugins
     * can modify the resource; validating plugins receive a copy of it and let the original pass through unchanged.
     *
     * @param context   The entry context passed to the plugins
     * @param kafkaMirrorMaker2   The KafkaMirrorMaker2 custom resource being reconciled
     *
     * @return  A completion stage with the KafkaMirrorMaker2 resource after it passed through all the plugins
     */
    public static CompletionStage<KafkaMirrorMaker2> kafkaMirrorMaker2Entry(GatekeeperKafkaMirrorMaker2EntryContext context, KafkaMirrorMaker2 kafkaMirrorMaker2) {
        return chain(
                GatekeeperKafkaMirrorMaker2MutatingPlugin.class,
                GatekeeperKafkaMirrorMaker2ValidatingPlugin.class,
                Phase.ENTRY,
                kafkaMirrorMaker2,
                (plugin, current) -> plugin.kafkaMirrorMaker2Entry(context, current),
                (plugin, current) -> plugin.kafkaMirrorMaker2Entry(context, copy(current, item -> new KafkaMirrorMaker2Builder(item).build())));
    }

    /**
     * Invokes the exit of all the KafkaMirrorMaker2 plugins (mutating and validating) as a single ordered chain, in the reverse of
     * the configured order. Mutating plugins can modify the status; validating plugins receive copies of the resource and
     * the status and let the original status pass through unchanged.
     *
     * @param context   The exit context passed to the plugins
     * @param kafkaMirrorMaker2   The KafkaMirrorMaker2 custom resource being reconciled
     * @param status    The status computed for the KafkaMirrorMaker2 resource
     *
     * @return  A completion stage with the KafkaMirrorMaker2 status after it passed through all the plugins
     */
    public static CompletionStage<KafkaMirrorMaker2Status> kafkaMirrorMaker2Exit(GatekeeperKafkaMirrorMaker2ExitContext context, KafkaMirrorMaker2 kafkaMirrorMaker2, KafkaMirrorMaker2Status status) {
        return chain(
                GatekeeperKafkaMirrorMaker2MutatingPlugin.class,
                GatekeeperKafkaMirrorMaker2ValidatingPlugin.class,
                Phase.EXIT,
                status,
                (plugin, current) -> plugin.kafkaMirrorMaker2Exit(context, kafkaMirrorMaker2, current),
                (plugin, current) -> plugin.kafkaMirrorMaker2Exit(context, copy(kafkaMirrorMaker2, item -> new KafkaMirrorMaker2Builder(item).build()), copy(current, item -> new KafkaMirrorMaker2StatusBuilder(item).build())));
    }

    /**
     * Invokes the entry of all the KafkaBridge plugins (mutating and validating) as a single ordered chain. Mutating plugins
     * can modify the resource; validating plugins receive a copy of it and let the original pass through unchanged.
     *
     * @param context   The entry context passed to the plugins
     * @param kafkaBridge   The KafkaBridge custom resource being reconciled
     *
     * @return  A completion stage with the KafkaBridge resource after it passed through all the plugins
     */
    public static CompletionStage<KafkaBridge> kafkaBridgeEntry(GatekeeperKafkaBridgeEntryContext context, KafkaBridge kafkaBridge) {
        return chain(
                GatekeeperKafkaBridgeMutatingPlugin.class,
                GatekeeperKafkaBridgeValidatingPlugin.class,
                Phase.ENTRY,
                kafkaBridge,
                (plugin, current) -> plugin.kafkaBridgeEntry(context, current),
                (plugin, current) -> plugin.kafkaBridgeEntry(context, copy(current, item -> new KafkaBridgeBuilder(item).build())));
    }

    /**
     * Invokes the exit of all the KafkaBridge plugins (mutating and validating) as a single ordered chain, in the reverse of
     * the configured order. Mutating plugins can modify the status; validating plugins receive copies of the resource and
     * the status and let the original status pass through unchanged.
     *
     * @param context   The exit context passed to the plugins
     * @param kafkaBridge   The KafkaBridge custom resource being reconciled
     * @param status    The status computed for the KafkaBridge resource
     *
     * @return  A completion stage with the KafkaBridge status after it passed through all the plugins
     */
    public static CompletionStage<KafkaBridgeStatus> kafkaBridgeExit(GatekeeperKafkaBridgeExitContext context, KafkaBridge kafkaBridge, KafkaBridgeStatus status) {
        return chain(
                GatekeeperKafkaBridgeMutatingPlugin.class,
                GatekeeperKafkaBridgeValidatingPlugin.class,
                Phase.EXIT,
                status,
                (plugin, current) -> plugin.kafkaBridgeExit(context, kafkaBridge, current),
                (plugin, current) -> plugin.kafkaBridgeExit(context, copy(kafkaBridge, item -> new KafkaBridgeBuilder(item).build()), copy(current, item -> new KafkaBridgeStatusBuilder(item).build())));
    }

    /**
     * Invokes the entry of all the KafkaRebalance plugins (mutating and validating) as a single ordered chain. Mutating plugins
     * can modify the resource; validating plugins receive a copy of it and let the original pass through unchanged.
     *
     * @param context   The entry context passed to the plugins
     * @param kafkaRebalance   The KafkaRebalance custom resource being reconciled
     *
     * @return  A completion stage with the KafkaRebalance resource after it passed through all the plugins
     */
    public static CompletionStage<KafkaRebalance> kafkaRebalanceEntry(GatekeeperKafkaRebalanceEntryContext context, KafkaRebalance kafkaRebalance) {
        return chain(
                GatekeeperKafkaRebalanceMutatingPlugin.class,
                GatekeeperKafkaRebalanceValidatingPlugin.class,
                Phase.ENTRY,
                kafkaRebalance,
                (plugin, current) -> plugin.kafkaRebalanceEntry(context, current),
                (plugin, current) -> plugin.kafkaRebalanceEntry(context, copy(current, item -> new KafkaRebalanceBuilder(item).build())));
    }

    /**
     * Invokes the exit of all the KafkaRebalance plugins (mutating and validating) as a single ordered chain, in the reverse of
     * the configured order. Mutating plugins can modify the status; validating plugins receive copies of the resource and
     * the status and let the original status pass through unchanged.
     *
     * @param context   The exit context passed to the plugins
     * @param kafkaRebalance   The KafkaRebalance custom resource being reconciled
     * @param status    The status computed for the KafkaRebalance resource
     *
     * @return  A completion stage with the KafkaRebalance status after it passed through all the plugins
     */
    public static CompletionStage<KafkaRebalanceStatus> kafkaRebalanceExit(GatekeeperKafkaRebalanceExitContext context, KafkaRebalance kafkaRebalance, KafkaRebalanceStatus status) {
        return chain(
                GatekeeperKafkaRebalanceMutatingPlugin.class,
                GatekeeperKafkaRebalanceValidatingPlugin.class,
                Phase.EXIT,
                status,
                (plugin, current) -> plugin.kafkaRebalanceExit(context, kafkaRebalance, current),
                (plugin, current) -> plugin.kafkaRebalanceExit(context, copy(kafkaRebalance, item -> new KafkaRebalanceBuilder(item).build()), copy(current, item -> new KafkaRebalanceStatusBuilder(item).build())));
    }

    /**
     * Invokes the entry of all the Kafka plugins (mutating and validating) as a single ordered chain. Mutating plugins
     * can modify the Kafka resource and its KafkaNodePool resources; validating plugins receive copies of them and let the
     * originals pass through unchanged.
     *
     * @param context           The entry context passed to the plugins
     * @param kafka           The Kafka custom resource being reconciled
     * @param kafkaNodePools   The KafkaNodePool resources belonging to the Kafka
     *
     * @return  A completion stage with the Kafka resource and its KafkaNodePool resources after they passed through all the plugins
     */
    public static CompletionStage<KafkaAndKafkaNodePools> kafkaEntry(GatekeeperKafkaEntryContext context, Kafka kafka, List<KafkaNodePool> kafkaNodePools) {
        return chain(
                GatekeeperKafkaMutatingPlugin.class,
                GatekeeperKafkaValidatingPlugin.class,
                Phase.ENTRY,
                new KafkaAndKafkaNodePools(kafka, kafkaNodePools),
                (plugin, current) -> plugin.kafkaEntry(context, current.kafka(), current.kafkaNodePools()),
                (plugin, current) -> plugin.kafkaEntry(context, copy(current.kafka(), item -> new KafkaBuilder(item).build()), copyList(current.kafkaNodePools(), item -> new KafkaNodePoolBuilder(item).build())));
    }

    /**
     * Invokes the Kafka exit of all the Kafka plugins (mutating and validating) as a single ordered chain, in the
     * reverse of the configured order. Mutating plugins can modify the status; validating plugins receive copies of the
     * resources and the status and let the original status pass through unchanged.
     *
     * @param context           The exit context passed to the plugins
     * @param kafka           The Kafka custom resource being reconciled
     * @param kafkaNodePools   The KafkaNodePool resources belonging to the Kafka
     * @param status            The status computed for the Kafka resource
     *
     * @return  A completion stage with the Kafka status after it passed through all the plugins
     */
    public static CompletionStage<KafkaStatus> kafkaExit(GatekeeperKafkaExitContext context, Kafka kafka, List<KafkaNodePool> kafkaNodePools, KafkaStatus status) {
        return chain(
                GatekeeperKafkaMutatingPlugin.class,
                GatekeeperKafkaValidatingPlugin.class,
                Phase.EXIT,
                status,
                (plugin, current) -> plugin.kafkaExit(context, kafka, kafkaNodePools, current),
                (plugin, current) -> plugin.kafkaExit(context, copy(kafka, item -> new KafkaBuilder(item).build()), copyList(kafkaNodePools, item -> new KafkaNodePoolBuilder(item).build()), copy(current, item -> new KafkaStatusBuilder(item).build())));
    }

    /**
     * Invokes the KafkaNodePool exit of all the Kafka plugins (mutating and validating) as a single ordered chain for a single
     * KafkaNodePool, in the reverse of the configured order. Mutating plugins can modify the status; validating plugins receive
     * copies of the resources and the status and let the original status pass through unchanged.
     *
     * @param context           The exit context passed to the plugins
     * @param kafka           The Kafka custom resource being reconciled
     * @param kafkaNodePool     The KafkaNodePool resource whose status is being processed
     * @param status            The status computed for the KafkaNodePool resource
     *
     * @return  A completion stage with the KafkaNodePool status after it passed through all the plugins
     */
    public static CompletionStage<KafkaNodePoolStatus> kafkaNodePoolExit(GatekeeperKafkaExitContext context, Kafka kafka, KafkaNodePool kafkaNodePool, KafkaNodePoolStatus status) {
        return chain(
                GatekeeperKafkaMutatingPlugin.class,
                GatekeeperKafkaValidatingPlugin.class,
                Phase.EXIT,
                status,
                (plugin, current) -> plugin.kafkaNodePoolExit(context, kafka, kafkaNodePool, current),
                (plugin, current) -> plugin.kafkaNodePoolExit(context, copy(kafka, item -> new KafkaBuilder(item).build()), copy(kafkaNodePool, item -> new KafkaNodePoolBuilder(item).build()), copy(current, item -> new KafkaNodePoolStatusBuilder(item).build())));
    }

    /**
     * Invokes the entry of all the KafkaConnect plugins (mutating and validating) as a single ordered chain. Mutating plugins
     * can modify the KafkaConnect resource and its KafkaConnector resources; validating plugins receive copies of them and let the
     * originals pass through unchanged.
     *
     * @param context           The entry context passed to the plugins
     * @param kafkaConnect           The KafkaConnect custom resource being reconciled
     * @param kafkaConnectors   The KafkaConnector resources belonging to the KafkaConnect
     *
     * @return  A completion stage with the KafkaConnect resource and its KafkaConnector resources after they passed through all the plugins
     */
    public static CompletionStage<KafkaConnectAndKafkaConnectors> kafkaConnectEntry(GatekeeperKafkaConnectEntryContext context, KafkaConnect kafkaConnect, List<KafkaConnector> kafkaConnectors) {
        return chain(
                GatekeeperKafkaConnectMutatingPlugin.class,
                GatekeeperKafkaConnectValidatingPlugin.class,
                Phase.ENTRY,
                new KafkaConnectAndKafkaConnectors(kafkaConnect, kafkaConnectors),
                (plugin, current) -> plugin.kafkaConnectEntry(context, current.kafkaConnect(), current.kafkaConnectors()),
                (plugin, current) -> plugin.kafkaConnectEntry(context, copy(current.kafkaConnect(), item -> new KafkaConnectBuilder(item).build()), copyList(current.kafkaConnectors(), item -> new KafkaConnectorBuilder(item).build())));
    }

    /**
     * Invokes the KafkaConnect exit of all the KafkaConnect plugins (mutating and validating) as a single ordered chain, in the
     * reverse of the configured order. Mutating plugins can modify the status; validating plugins receive copies of the
     * resources and the status and let the original status pass through unchanged.
     *
     * @param context           The exit context passed to the plugins
     * @param kafkaConnect           The KafkaConnect custom resource being reconciled
     * @param kafkaConnectors   The KafkaConnector resources belonging to the KafkaConnect
     * @param status            The status computed for the KafkaConnect resource
     *
     * @return  A completion stage with the KafkaConnect status after it passed through all the plugins
     */
    public static CompletionStage<KafkaConnectStatus> kafkaConnectExit(GatekeeperKafkaConnectExitContext context, KafkaConnect kafkaConnect, List<KafkaConnector> kafkaConnectors, KafkaConnectStatus status) {
        return chain(
                GatekeeperKafkaConnectMutatingPlugin.class,
                GatekeeperKafkaConnectValidatingPlugin.class,
                Phase.EXIT,
                status,
                (plugin, current) -> plugin.kafkaConnectExit(context, kafkaConnect, kafkaConnectors, current),
                (plugin, current) -> plugin.kafkaConnectExit(context, copy(kafkaConnect, item -> new KafkaConnectBuilder(item).build()), copyList(kafkaConnectors, item -> new KafkaConnectorBuilder(item).build()), copy(current, item -> new KafkaConnectStatusBuilder(item).build())));
    }

    /**
     * Invokes the KafkaConnector exit of all the KafkaConnect plugins (mutating and validating) as a single ordered chain for a single
     * KafkaConnector, in the reverse of the configured order. Mutating plugins can modify the status; validating plugins receive
     * copies of the resources and the status and let the original status pass through unchanged.
     *
     * @param context           The exit context passed to the plugins
     * @param kafkaConnect           The KafkaConnect custom resource being reconciled
     * @param kafkaConnector     The KafkaConnector resource whose status is being processed
     * @param status            The status computed for the KafkaConnector resource
     *
     * @return  A completion stage with the KafkaConnector status after it passed through all the plugins
     */
    public static CompletionStage<KafkaConnectorStatus> kafkaConnectorExit(GatekeeperKafkaConnectExitContext context, KafkaConnect kafkaConnect, KafkaConnector kafkaConnector, KafkaConnectorStatus status) {
        return chain(
                GatekeeperKafkaConnectMutatingPlugin.class,
                GatekeeperKafkaConnectValidatingPlugin.class,
                Phase.EXIT,
                status,
                (plugin, current) -> plugin.kafkaConnectorExit(context, kafkaConnect, kafkaConnector, current),
                (plugin, current) -> plugin.kafkaConnectorExit(context, copy(kafkaConnect, item -> new KafkaConnectBuilder(item).build()), copy(kafkaConnector, item -> new KafkaConnectorBuilder(item).build()), copy(current, item -> new KafkaConnectorStatusBuilder(item).build())));
    }

    /**
     * Invokes the deletion hook of all the Kafka plugins (both mutating and validating, since the deletion hook is
     * defined on their common interface) as a single ordered chain. There is no resource to mutate during a deletion, so
     * the hooks only react to the deletion; any of them can reject it by completing exceptionally, which stops the chain.
     *
     * @param context   The deletion context passed to the plugins
     * @param namespace The namespace of the Kafka being deleted
     * @param name      The name of the Kafka being deleted
     *
     * @return  A completion stage which completes when all the deletion hooks completed
     */
    public static CompletionStage<Void> kafkaDeletion(GatekeeperKafkaDeletionContext context, String namespace, String name) {
        return deletion(
                GatekeeperKafkaMutatingPlugin.class,
                GatekeeperKafkaValidatingPlugin.class,
                plugin -> plugin instanceof GatekeeperKafkaMutatingPlugin mutating
                        ? mutating.kafkaDeletion(context, namespace, name)
                        : ((GatekeeperKafkaValidatingPlugin) plugin).kafkaDeletion(context, namespace, name));
    }

    /**
     * Invokes the deletion hook of all the KafkaConnect plugins as a single ordered chain. There is no resource to
     * mutate during a deletion, so the hooks only react to the deletion; any of them can reject it by completing
     * exceptionally, which stops the chain.
     *
     * @param context   The deletion context passed to the plugins
     * @param namespace The namespace of the KafkaConnect being deleted
     * @param name      The name of the KafkaConnect being deleted
     *
     * @return  A completion stage which completes when all the deletion hooks completed
     */
    public static CompletionStage<Void> kafkaConnectDeletion(GatekeeperKafkaConnectDeletionContext context, String namespace, String name) {
        return deletion(
                GatekeeperKafkaConnectMutatingPlugin.class,
                GatekeeperKafkaConnectValidatingPlugin.class,
                plugin -> plugin instanceof GatekeeperKafkaConnectMutatingPlugin mutating
                        ? mutating.kafkaConnectDeletion(context, namespace, name)
                        : ((GatekeeperKafkaConnectValidatingPlugin) plugin).kafkaConnectDeletion(context, namespace, name));
    }

    /**
     * Invokes the deletion hook of all the KafkaMirrorMaker2 plugins as a single ordered chain. There is no resource to
     * mutate during a deletion, so the hooks only react to the deletion; any of them can reject it by completing
     * exceptionally, which stops the chain.
     *
     * @param context   The deletion context passed to the plugins
     * @param namespace The namespace of the KafkaMirrorMaker2 being deleted
     * @param name      The name of the KafkaMirrorMaker2 being deleted
     *
     * @return  A completion stage which completes when all the deletion hooks completed
     */
    public static CompletionStage<Void> kafkaMirrorMaker2Deletion(GatekeeperKafkaMirrorMaker2DeletionContext context, String namespace, String name) {
        return deletion(
                GatekeeperKafkaMirrorMaker2MutatingPlugin.class,
                GatekeeperKafkaMirrorMaker2ValidatingPlugin.class,
                plugin -> plugin instanceof GatekeeperKafkaMirrorMaker2MutatingPlugin mutating
                        ? mutating.kafkaMirrorMaker2Deletion(context, namespace, name)
                        : ((GatekeeperKafkaMirrorMaker2ValidatingPlugin) plugin).kafkaMirrorMaker2Deletion(context, namespace, name));
    }

    /**
     * Invokes the deletion hook of all the KafkaBridge plugins as a single ordered chain. There is no resource to mutate
     * during a deletion, so the hooks only react to the deletion; any of them can reject it by completing exceptionally,
     * which stops the chain.
     *
     * @param context   The deletion context passed to the plugins
     * @param namespace The namespace of the KafkaBridge being deleted
     * @param name      The name of the KafkaBridge being deleted
     *
     * @return  A completion stage which completes when all the deletion hooks completed
     */
    public static CompletionStage<Void> kafkaBridgeDeletion(GatekeeperKafkaBridgeDeletionContext context, String namespace, String name) {
        return deletion(
                GatekeeperKafkaBridgeMutatingPlugin.class,
                GatekeeperKafkaBridgeValidatingPlugin.class,
                plugin -> plugin instanceof GatekeeperKafkaBridgeMutatingPlugin mutating
                        ? mutating.kafkaBridgeDeletion(context, namespace, name)
                        : ((GatekeeperKafkaBridgeValidatingPlugin) plugin).kafkaBridgeDeletion(context, namespace, name));
    }

    /**
     * Invokes the deletion hook of all the KafkaRebalance plugins as a single ordered chain. There is no resource to
     * mutate during a deletion, so the hooks only react to the deletion; any of them can reject it by completing
     * exceptionally, which stops the chain.
     *
     * @param context   The deletion context passed to the plugins
     * @param namespace The namespace of the KafkaRebalance being deleted
     * @param name      The name of the KafkaRebalance being deleted
     *
     * @return  A completion stage which completes when all the deletion hooks completed
     */
    public static CompletionStage<Void> kafkaRebalanceDeletion(GatekeeperKafkaRebalanceDeletionContext context, String namespace, String name) {
        return deletion(
                GatekeeperKafkaRebalanceMutatingPlugin.class,
                GatekeeperKafkaRebalanceValidatingPlugin.class,
                plugin -> plugin instanceof GatekeeperKafkaRebalanceMutatingPlugin mutating
                        ? mutating.kafkaRebalanceDeletion(context, namespace, name)
                        : ((GatekeeperKafkaRebalanceValidatingPlugin) plugin).kafkaRebalanceDeletion(context, namespace, name));
    }
}
