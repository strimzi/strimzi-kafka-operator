/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.gatekeeper;

import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.KafkaStatusBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the ClusterOperatorGatekeeperPluginInvoker. The invoker delegates to the same shared chaining logic
 * as the User and Topic Operator invokers, so the Kafka tests focus on the Cluster-Operator-specific parts (the
 * compound KafkaAndKafkaNodePools resource and the KafkaNodePool exit). The tests for the other operands
 * (KafkaConnect, KafkaMirrorMaker2, KafkaBridge, and KafkaRebalance) exercise the entry, exit, and deletion methods
 * of each so that every operand-specific invoker method is covered.
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ClassDataAbstractionCoupling"})
public class ClusterOperatorGatekeeperPluginInvokerTest {
    private final List<String> order = new ArrayList<>();

    @AfterEach
    public void cleanup() {
        GatekeeperPluginFactory.clearTestPlugins();
    }

    @Test
    public void testEntryChainsMutatingPluginsInRequestedOrder() {
        GatekeeperPluginFactory.initializeForTests(List.of(new KafkaMutator("A"), new KafkaMutator("B")));

        KafkaAndKafkaNodePools result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaEntry(null, kafka(""), List.of())
                .toCompletableFuture().join();

        assertThat(result.kafka().getMetadata().getName(), is("AB"));
        assertThat(order, contains("A", "B"));
    }

    @Test
    public void testExitChainsPluginsInReverseOrder() {
        GatekeeperPluginFactory.initializeForTests(List.of(new KafkaMutator("A"), new KafkaMutator("B")));

        ClusterOperatorGatekeeperPluginInvoker
                .kafkaExit(null, kafka(""), List.of(), new KafkaStatus())
                .toCompletableFuture().join();

        assertThat(order, contains("B", "A"));
    }

    @Test
    public void testMutatingAndValidatingPluginsRunInOneOrderedChain() {
        GatekeeperPluginFactory.initializeForTests(List.of(new KafkaMutator("A"), new KafkaValidator("B", false)));

        KafkaAndKafkaNodePools result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaEntry(null, kafka(""), List.of())
                .toCompletableFuture().join();

        assertThat(order, contains("A", "B"));
        assertThat(result.kafka().getMetadata().getName(), is("A"));
    }

    @Test
    public void testEntryWithNoPluginsReturnsResourceUnchanged() {
        GatekeeperPluginFactory.initializeForTests(List.of());

        KafkaAndKafkaNodePools result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaEntry(null, kafka("unchanged"), List.of())
                .toCompletableFuture().join();

        assertThat(result.kafka().getMetadata().getName(), is("unchanged"));
        assertThat(order, is(empty()));
    }

    @Test
    public void testChainStopsAtFirstValidationFailure() {
        GatekeeperPluginFactory.initializeForTests(List.of(new KafkaValidator("fail", true), new KafkaValidator("ok", false)));

        CompletableFuture<KafkaAndKafkaNodePools> result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaEntry(null, kafka(""), List.of())
                .toCompletableFuture();

        assertThrows(CompletionException.class, result::join);
        assertThat(order, contains("fail"));
    }

    @Test
    public void testValidatingPluginCannotModifyTheResource() {
        Kafka base = kafka("original");
        GatekeeperPluginFactory.initializeForTests(List.of(new KafkaResourceMutatingValidator()));

        KafkaAndKafkaNodePools result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaEntry(null, base, List.of())
                .toCompletableFuture().join();

        assertThat(result.kafka().getMetadata().getName(), is("original"));
        assertThat(base.getMetadata().getName(), is("original"));
    }

    @Test
    public void testExitMutatingPluginCanModifyStatus() {
        GatekeeperPluginFactory.initializeForTests(List.of(new StatusMutator()));

        KafkaStatus status = new KafkaStatusBuilder().withObservedGeneration(1L).build();

        KafkaStatus result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaExit(null, kafka("my-kafka"), List.of(), status)
                .toCompletableFuture().join();

        assertThat(result.getObservedGeneration(), is(1874L));
    }

    @Test
    public void testExitValidatingPluginCannotModifyStatus() {
        GatekeeperPluginFactory.initializeForTests(List.of(new StatusMutatingValidator()));

        KafkaStatus status = new KafkaStatusBuilder().withObservedGeneration(1L).build();

        KafkaStatus result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaExit(null, kafka("my-kafka"), List.of(), status)
                .toCompletableFuture().join();

        assertThat(result.getObservedGeneration(), is(1L));
    }

    @Test
    public void testNodePoolExitMutatingPluginCanModifyStatus() {
        GatekeeperPluginFactory.initializeForTests(List.of(new NodePoolStatusMutator()));

        KafkaNodePoolStatus status = new KafkaNodePoolStatus();
        status.setObservedGeneration(1L);

        KafkaNodePoolStatus result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaNodePoolExit(null, kafka("my-kafka"), null, status)
                .toCompletableFuture().join();

        assertThat(result.getObservedGeneration(), is(1874L));
    }

    @Test
    public void testDeletionInvokesAllPluginsInRequestedOrder() {
        GatekeeperPluginFactory.initializeForTests(List.of(new KafkaDeletionPlugin("A", false), new KafkaDeletionPlugin("B", false)));

        ClusterOperatorGatekeeperPluginInvoker
                .kafkaDeletion(null, "my-namespace", "my-kafka")
                .toCompletableFuture().join();

        assertThat(order, contains("A", "B"));
    }

    @Test
    public void testDeletionPassesNamespaceAndName() {
        KafkaDeletionPlugin plugin = new KafkaDeletionPlugin("A", false);
        GatekeeperPluginFactory.initializeForTests(List.of(plugin));

        ClusterOperatorGatekeeperPluginInvoker
                .kafkaDeletion(null, "my-namespace", "my-kafka")
                .toCompletableFuture().join();

        assertThat(plugin.namespace, is("my-namespace"));
        assertThat(plugin.name, is("my-kafka"));
    }

    @Test
    public void testDeletionStopsAtFirstFailure() {
        GatekeeperPluginFactory.initializeForTests(List.of(new KafkaDeletionPlugin("fail", true), new KafkaDeletionPlugin("ok", false)));

        CompletableFuture<Void> result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaDeletion(null, "my-namespace", "my-kafka")
                .toCompletableFuture();

        assertThrows(CompletionException.class, result::join);
        assertThat(order, contains("fail"));
    }

    ///////////////////////////////////////////////////
    // KafkaConnect
    ///////////////////////////////////////////////////

    @Test
    public void testKafkaConnectEntryChainsPluginsAndMutatesResourceAndConnectors() {
        GatekeeperPluginFactory.initializeForTests(List.of(new ConnectMutator("A"), new ConnectValidator("B", false)));

        KafkaConnectAndKafkaConnectors result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaConnectEntry(null, kafkaConnect(""), List.of(kafkaConnector("c")))
                .toCompletableFuture().join();

        assertThat(order, contains("A", "B"));
        assertThat(result.kafkaConnect().getMetadata().getName(), is("A"));
        assertThat(result.kafkaConnectors().get(0).getMetadata().getName(), is("cA"));
    }

    @Test
    public void testKafkaConnectEntryValidatingPluginCannotModifyTheResource() {
        KafkaConnect base = kafkaConnect("original");
        GatekeeperPluginFactory.initializeForTests(List.of(new ConnectResourceMutatingValidator()));

        KafkaConnectAndKafkaConnectors result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaConnectEntry(null, base, List.of())
                .toCompletableFuture().join();

        assertThat(result.kafkaConnect().getMetadata().getName(), is("original"));
        assertThat(base.getMetadata().getName(), is("original"));
    }

    @Test
    public void testKafkaConnectEntryStopsAtFirstValidationFailure() {
        GatekeeperPluginFactory.initializeForTests(List.of(new ConnectValidator("fail", true), new ConnectValidator("ok", false)));

        CompletableFuture<KafkaConnectAndKafkaConnectors> result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaConnectEntry(null, kafkaConnect(""), List.of())
                .toCompletableFuture();

        assertThrows(CompletionException.class, result::join);
        assertThat(order, contains("fail"));
    }

    @Test
    public void testKafkaConnectExitMutatingPluginCanModifyStatus() {
        GatekeeperPluginFactory.initializeForTests(List.of(new ConnectMutator("A")));

        KafkaConnectStatus status = new KafkaConnectStatus();
        status.setObservedGeneration(1L);

        KafkaConnectStatus result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaConnectExit(null, kafkaConnect("my-connect"), List.of(), status)
                .toCompletableFuture().join();

        assertThat(result.getObservedGeneration(), is(1874L));
    }

    @Test
    public void testKafkaConnectExitValidatingPluginCannotModifyStatus() {
        GatekeeperPluginFactory.initializeForTests(List.of(new ConnectValidator("A", false)));

        KafkaConnectStatus status = new KafkaConnectStatus();
        status.setObservedGeneration(1L);

        KafkaConnectStatus result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaConnectExit(null, kafkaConnect("my-connect"), List.of(), status)
                .toCompletableFuture().join();

        assertThat(result.getObservedGeneration(), is(1L));
    }

    @Test
    public void testKafkaConnectorExitMutatingPluginCanModifyStatus() {
        GatekeeperPluginFactory.initializeForTests(List.of(new ConnectMutator("A")));

        KafkaConnectorStatus status = new KafkaConnectorStatus();
        status.setObservedGeneration(1L);

        KafkaConnectorStatus result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaConnectorExit(null, kafkaConnect("my-connect"), kafkaConnector("c"), status)
                .toCompletableFuture().join();

        assertThat(result.getObservedGeneration(), is(1874L));
    }

    @Test
    public void testKafkaConnectDeletionInvokesMutatingAndValidatingPluginsInOrder() {
        GatekeeperPluginFactory.initializeForTests(List.of(new ConnectMutator("A"), new ConnectValidator("B", false)));

        ClusterOperatorGatekeeperPluginInvoker
                .kafkaConnectDeletion(null, "my-namespace", "my-connect")
                .toCompletableFuture().join();

        assertThat(order, contains("A", "B"));
    }

    @Test
    public void testKafkaConnectDeletionStopsAtFirstFailure() {
        GatekeeperPluginFactory.initializeForTests(List.of(new ConnectValidator("fail", true), new ConnectMutator("ok")));

        CompletableFuture<Void> result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaConnectDeletion(null, "my-namespace", "my-connect")
                .toCompletableFuture();

        assertThrows(CompletionException.class, result::join);
        assertThat(order, contains("fail"));
    }

    ///////////////////////////////////////////////////
    // KafkaMirrorMaker2
    ///////////////////////////////////////////////////

    @Test
    public void testKafkaMirrorMaker2EntryChainsPluginsAndMutatesResource() {
        GatekeeperPluginFactory.initializeForTests(List.of(new MirrorMaker2Mutator("A"), new MirrorMaker2Validator("B", false)));

        KafkaMirrorMaker2 result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaMirrorMaker2Entry(null, kafkaMirrorMaker2(""))
                .toCompletableFuture().join();

        assertThat(order, contains("A", "B"));
        assertThat(result.getMetadata().getName(), is("A"));
    }

    @Test
    public void testKafkaMirrorMaker2ExitMutatingPluginCanModifyStatus() {
        GatekeeperPluginFactory.initializeForTests(List.of(new MirrorMaker2Mutator("A")));

        KafkaMirrorMaker2Status status = new KafkaMirrorMaker2Status();
        status.setObservedGeneration(1L);

        KafkaMirrorMaker2Status result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaMirrorMaker2Exit(null, kafkaMirrorMaker2("my-mm2"), status)
                .toCompletableFuture().join();

        assertThat(result.getObservedGeneration(), is(1874L));
    }

    @Test
    public void testKafkaMirrorMaker2ExitValidatingPluginCannotModifyStatus() {
        GatekeeperPluginFactory.initializeForTests(List.of(new MirrorMaker2Validator("A", false)));

        KafkaMirrorMaker2Status status = new KafkaMirrorMaker2Status();
        status.setObservedGeneration(1L);

        KafkaMirrorMaker2Status result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaMirrorMaker2Exit(null, kafkaMirrorMaker2("my-mm2"), status)
                .toCompletableFuture().join();

        assertThat(result.getObservedGeneration(), is(1L));
    }

    @Test
    public void testKafkaMirrorMaker2DeletionInvokesMutatingAndValidatingPluginsInOrder() {
        GatekeeperPluginFactory.initializeForTests(List.of(new MirrorMaker2Mutator("A"), new MirrorMaker2Validator("B", false)));

        ClusterOperatorGatekeeperPluginInvoker
                .kafkaMirrorMaker2Deletion(null, "my-namespace", "my-mm2")
                .toCompletableFuture().join();

        assertThat(order, contains("A", "B"));
    }

    ///////////////////////////////////////////////////
    // KafkaBridge
    ///////////////////////////////////////////////////

    @Test
    public void testKafkaBridgeEntryChainsPluginsAndMutatesResource() {
        GatekeeperPluginFactory.initializeForTests(List.of(new BridgeMutator("A"), new BridgeValidator("B", false)));

        KafkaBridge result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaBridgeEntry(null, kafkaBridge(""))
                .toCompletableFuture().join();

        assertThat(order, contains("A", "B"));
        assertThat(result.getMetadata().getName(), is("A"));
    }

    @Test
    public void testKafkaBridgeExitMutatingPluginCanModifyStatus() {
        GatekeeperPluginFactory.initializeForTests(List.of(new BridgeMutator("A")));

        KafkaBridgeStatus status = new KafkaBridgeStatus();
        status.setObservedGeneration(1L);

        KafkaBridgeStatus result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaBridgeExit(null, kafkaBridge("my-bridge"), status)
                .toCompletableFuture().join();

        assertThat(result.getObservedGeneration(), is(1874L));
    }

    @Test
    public void testKafkaBridgeExitValidatingPluginCannotModifyStatus() {
        GatekeeperPluginFactory.initializeForTests(List.of(new BridgeValidator("A", false)));

        KafkaBridgeStatus status = new KafkaBridgeStatus();
        status.setObservedGeneration(1L);

        KafkaBridgeStatus result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaBridgeExit(null, kafkaBridge("my-bridge"), status)
                .toCompletableFuture().join();

        assertThat(result.getObservedGeneration(), is(1L));
    }

    @Test
    public void testKafkaBridgeDeletionInvokesMutatingAndValidatingPluginsInOrder() {
        GatekeeperPluginFactory.initializeForTests(List.of(new BridgeMutator("A"), new BridgeValidator("B", false)));

        ClusterOperatorGatekeeperPluginInvoker
                .kafkaBridgeDeletion(null, "my-namespace", "my-bridge")
                .toCompletableFuture().join();

        assertThat(order, contains("A", "B"));
    }

    ///////////////////////////////////////////////////
    // KafkaRebalance
    ///////////////////////////////////////////////////

    @Test
    public void testKafkaRebalanceEntryChainsPluginsAndMutatesResource() {
        GatekeeperPluginFactory.initializeForTests(List.of(new RebalanceMutator("A"), new RebalanceValidator("B", false)));

        KafkaRebalance result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaRebalanceEntry(null, kafkaRebalance(""))
                .toCompletableFuture().join();

        assertThat(order, contains("A", "B"));
        assertThat(result.getMetadata().getName(), is("A"));
    }

    @Test
    public void testKafkaRebalanceExitMutatingPluginCanModifyStatus() {
        GatekeeperPluginFactory.initializeForTests(List.of(new RebalanceMutator("A")));

        KafkaRebalanceStatus status = new KafkaRebalanceStatus();
        status.setObservedGeneration(1L);

        KafkaRebalanceStatus result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaRebalanceExit(null, kafkaRebalance("my-rebalance"), status)
                .toCompletableFuture().join();

        assertThat(result.getObservedGeneration(), is(1874L));
    }

    @Test
    public void testKafkaRebalanceExitValidatingPluginCannotModifyStatus() {
        GatekeeperPluginFactory.initializeForTests(List.of(new RebalanceValidator("A", false)));

        KafkaRebalanceStatus status = new KafkaRebalanceStatus();
        status.setObservedGeneration(1L);

        KafkaRebalanceStatus result = ClusterOperatorGatekeeperPluginInvoker
                .kafkaRebalanceExit(null, kafkaRebalance("my-rebalance"), status)
                .toCompletableFuture().join();

        assertThat(result.getObservedGeneration(), is(1L));
    }

    @Test
    public void testKafkaRebalanceDeletionInvokesMutatingAndValidatingPluginsInOrder() {
        GatekeeperPluginFactory.initializeForTests(List.of(new RebalanceMutator("A"), new RebalanceValidator("B", false)));

        ClusterOperatorGatekeeperPluginInvoker
                .kafkaRebalanceDeletion(null, "my-namespace", "my-rebalance")
                .toCompletableFuture().join();

        assertThat(order, contains("A", "B"));
    }

    ///////////////////////////////////////////////////
    // Helper methods for creating the resources
    ///////////////////////////////////////////////////

    private static Kafka kafka(String name) {
        return new KafkaBuilder().withNewMetadata().withName(name).endMetadata().withNewSpec().endSpec().build();
    }

    private static KafkaConnect kafkaConnect(String name) {
        return new KafkaConnectBuilder().withNewMetadata().withName(name).endMetadata().withNewSpec().endSpec().build();
    }

    private static KafkaConnector kafkaConnector(String name) {
        return new KafkaConnectorBuilder().withNewMetadata().withName(name).endMetadata().withNewSpec().endSpec().build();
    }

    private static KafkaMirrorMaker2 kafkaMirrorMaker2(String name) {
        return new KafkaMirrorMaker2Builder().withNewMetadata().withName(name).endMetadata().withNewSpec().endSpec().build();
    }

    private static KafkaBridge kafkaBridge(String name) {
        return new KafkaBridgeBuilder().withNewMetadata().withName(name).endMetadata().withNewSpec().endSpec().build();
    }

    private static KafkaRebalance kafkaRebalance(String name) {
        return new KafkaRebalanceBuilder().withNewMetadata().withName(name).endMetadata().withNewSpec().endSpec().build();
    }

    private static String append(String current, String tag) {
        return (current == null ? "" : current) + tag;
    }

    ///////////////////////////////////////////////////
    // Kafka plugins
    ///////////////////////////////////////////////////

    private final class KafkaMutator implements GatekeeperKafkaMutatingPlugin {
        private final String tag;

        private KafkaMutator(String tag) {
            this.tag = tag;
        }

        @Override
        public CompletionStage<KafkaAndKafkaNodePools> kafkaEntry(GatekeeperKafkaEntryContext context, Kafka kafka, List<KafkaNodePool> kafkaNodePools) {
            order.add(tag);
            Kafka mutated = new KafkaBuilder(kafka)
                    .editOrNewMetadata()
                        .withName(append(kafka.getMetadata().getName(), tag))
                    .endMetadata()
                    .build();
            return CompletableFuture.completedFuture(new KafkaAndKafkaNodePools(mutated, kafkaNodePools));
        }

        @Override
        public CompletionStage<KafkaStatus> kafkaExit(GatekeeperKafkaExitContext context, Kafka kafka, List<KafkaNodePool> kafkaNodePools, KafkaStatus newKafkaStatus) {
            order.add(tag);
            return CompletableFuture.completedFuture(newKafkaStatus);
        }
    }

    private final class KafkaValidator implements GatekeeperKafkaValidatingPlugin {
        private final String tag;
        private final boolean fail;

        private KafkaValidator(String tag, boolean fail) {
            this.tag = tag;
            this.fail = fail;
        }

        @Override
        public CompletionStage<Void> kafkaEntry(GatekeeperKafkaEntryContext context, Kafka kafka, List<KafkaNodePool> kafkaNodePools) {
            order.add(tag);
            return fail
                    ? CompletableFuture.failedFuture(new RuntimeException("rejected by " + tag))
                    : CompletableFuture.completedFuture(null);
        }
    }

    private static final class KafkaResourceMutatingValidator implements GatekeeperKafkaValidatingPlugin {
        @Override
        public CompletionStage<Void> kafkaEntry(GatekeeperKafkaEntryContext context, Kafka kafka, List<KafkaNodePool> kafkaNodePools) {
            kafka.getMetadata().setName("HACKED");
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class StatusMutator implements GatekeeperKafkaMutatingPlugin {
        @Override
        public CompletionStage<KafkaStatus> kafkaExit(GatekeeperKafkaExitContext context, Kafka kafka, List<KafkaNodePool> kafkaNodePools, KafkaStatus newKafkaStatus) {
            newKafkaStatus.setObservedGeneration(1874L);
            return CompletableFuture.completedFuture(newKafkaStatus);
        }
    }

    private static final class StatusMutatingValidator implements GatekeeperKafkaValidatingPlugin {
        @Override
        public CompletionStage<Void> kafkaExit(GatekeeperKafkaExitContext context, Kafka kafka, List<KafkaNodePool> kafkaNodePools, KafkaStatus newKafkaStatus) {
            newKafkaStatus.setObservedGeneration(1919L);
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class NodePoolStatusMutator implements GatekeeperKafkaMutatingPlugin {
        @Override
        public CompletionStage<KafkaNodePoolStatus> kafkaNodePoolExit(GatekeeperKafkaExitContext context, Kafka kafka, KafkaNodePool kafkaNodePool, KafkaNodePoolStatus newKafkaNodePoolStatus) {
            newKafkaNodePoolStatus.setObservedGeneration(1874L);
            return CompletableFuture.completedFuture(newKafkaNodePoolStatus);
        }
    }

    private final class KafkaDeletionPlugin implements GatekeeperKafkaMutatingPlugin {
        private final String tag;
        private final boolean fail;
        private String namespace;
        private String name;

        private KafkaDeletionPlugin(String tag, boolean fail) {
            this.tag = tag;
            this.fail = fail;
        }

        @Override
        public CompletionStage<Void> kafkaDeletion(GatekeeperKafkaDeletionContext context, String namespace, String name) {
            order.add(tag);
            this.namespace = namespace;
            this.name = name;
            return fail
                    ? CompletableFuture.failedFuture(new RuntimeException("rejected by " + tag))
                    : CompletableFuture.completedFuture(null);
        }
    }

    ///////////////////////////////////////////////////
    // KafkaConnect plugins
    ///////////////////////////////////////////////////

    private final class ConnectMutator implements GatekeeperKafkaConnectMutatingPlugin {
        private final String tag;

        private ConnectMutator(String tag) {
            this.tag = tag;
        }

        @Override
        public CompletionStage<KafkaConnectAndKafkaConnectors> kafkaConnectEntry(GatekeeperKafkaConnectEntryContext context, KafkaConnect kafkaConnect, List<KafkaConnector> kafkaConnectors) {
            order.add(tag);
            KafkaConnect mutated = new KafkaConnectBuilder(kafkaConnect)
                    .editOrNewMetadata()
                        .withName(append(kafkaConnect.getMetadata().getName(), tag))
                    .endMetadata()
                    .build();
            List<KafkaConnector> mutatedConnectors = kafkaConnectors.stream()
                    .map(connector -> new KafkaConnectorBuilder(connector)
                            .editOrNewMetadata()
                                .withName(append(connector.getMetadata().getName(), tag))
                            .endMetadata()
                            .build())
                    .toList();
            return CompletableFuture.completedFuture(new KafkaConnectAndKafkaConnectors(mutated, mutatedConnectors));
        }

        @Override
        public CompletionStage<KafkaConnectStatus> kafkaConnectExit(GatekeeperKafkaConnectExitContext context, KafkaConnect kafkaConnect, List<KafkaConnector> kafkaConnectors, KafkaConnectStatus newKafkaConnectStatus) {
            newKafkaConnectStatus.setObservedGeneration(1874L);
            return CompletableFuture.completedFuture(newKafkaConnectStatus);
        }

        @Override
        public CompletionStage<KafkaConnectorStatus> kafkaConnectorExit(GatekeeperKafkaConnectExitContext context, KafkaConnect kafkaConnect, KafkaConnector kafkaConnector, KafkaConnectorStatus newKafkaConnectorStatus) {
            newKafkaConnectorStatus.setObservedGeneration(1874L);
            return CompletableFuture.completedFuture(newKafkaConnectorStatus);
        }

        @Override
        public CompletionStage<Void> kafkaConnectDeletion(GatekeeperKafkaConnectDeletionContext context, String namespace, String name) {
            order.add(tag);
            return CompletableFuture.completedFuture(null);
        }
    }

    private final class ConnectValidator implements GatekeeperKafkaConnectValidatingPlugin {
        private final String tag;
        private final boolean fail;

        private ConnectValidator(String tag, boolean fail) {
            this.tag = tag;
            this.fail = fail;
        }

        @Override
        public CompletionStage<Void> kafkaConnectEntry(GatekeeperKafkaConnectEntryContext context, KafkaConnect kafkaConnect, List<KafkaConnector> kafkaConnectors) {
            order.add(tag);
            return fail
                    ? CompletableFuture.failedFuture(new RuntimeException("rejected by " + tag))
                    : CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> kafkaConnectExit(GatekeeperKafkaConnectExitContext context, KafkaConnect kafkaConnect, List<KafkaConnector> kafkaConnectors, KafkaConnectStatus newKafkaConnectStatus) {
            newKafkaConnectStatus.setObservedGeneration(1919L);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> kafkaConnectDeletion(GatekeeperKafkaConnectDeletionContext context, String namespace, String name) {
            order.add(tag);
            return fail
                    ? CompletableFuture.failedFuture(new RuntimeException("rejected by " + tag))
                    : CompletableFuture.completedFuture(null);
        }
    }

    private static final class ConnectResourceMutatingValidator implements GatekeeperKafkaConnectValidatingPlugin {
        @Override
        public CompletionStage<Void> kafkaConnectEntry(GatekeeperKafkaConnectEntryContext context, KafkaConnect kafkaConnect, List<KafkaConnector> kafkaConnectors) {
            kafkaConnect.getMetadata().setName("HACKED");
            return CompletableFuture.completedFuture(null);
        }
    }

    ///////////////////////////////////////////////////
    // KafkaMirrorMaker2 plugins
    ///////////////////////////////////////////////////

    private final class MirrorMaker2Mutator implements GatekeeperKafkaMirrorMaker2MutatingPlugin {
        private final String tag;

        private MirrorMaker2Mutator(String tag) {
            this.tag = tag;
        }

        @Override
        public CompletionStage<KafkaMirrorMaker2> kafkaMirrorMaker2Entry(GatekeeperKafkaMirrorMaker2EntryContext context, KafkaMirrorMaker2 kafkaMirrorMaker2) {
            order.add(tag);
            KafkaMirrorMaker2 mutated = new KafkaMirrorMaker2Builder(kafkaMirrorMaker2)
                    .editOrNewMetadata()
                        .withName(append(kafkaMirrorMaker2.getMetadata().getName(), tag))
                    .endMetadata()
                    .build();
            return CompletableFuture.completedFuture(mutated);
        }

        @Override
        public CompletionStage<KafkaMirrorMaker2Status> kafkaMirrorMaker2Exit(GatekeeperKafkaMirrorMaker2ExitContext context, KafkaMirrorMaker2 kafkaMirrorMaker2, KafkaMirrorMaker2Status newKafkaMirrorMaker2Status) {
            newKafkaMirrorMaker2Status.setObservedGeneration(1874L);
            return CompletableFuture.completedFuture(newKafkaMirrorMaker2Status);
        }

        @Override
        public CompletionStage<Void> kafkaMirrorMaker2Deletion(GatekeeperKafkaMirrorMaker2DeletionContext context, String namespace, String name) {
            order.add(tag);
            return CompletableFuture.completedFuture(null);
        }
    }

    private final class MirrorMaker2Validator implements GatekeeperKafkaMirrorMaker2ValidatingPlugin {
        private final String tag;
        private final boolean fail;

        private MirrorMaker2Validator(String tag, boolean fail) {
            this.tag = tag;
            this.fail = fail;
        }

        @Override
        public CompletionStage<Void> kafkaMirrorMaker2Entry(GatekeeperKafkaMirrorMaker2EntryContext context, KafkaMirrorMaker2 kafkaMirrorMaker2) {
            order.add(tag);
            return fail
                    ? CompletableFuture.failedFuture(new RuntimeException("rejected by " + tag))
                    : CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> kafkaMirrorMaker2Exit(GatekeeperKafkaMirrorMaker2ExitContext context, KafkaMirrorMaker2 kafkaMirrorMaker2, KafkaMirrorMaker2Status newKafkaMirrorMaker2Status) {
            newKafkaMirrorMaker2Status.setObservedGeneration(1919L);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> kafkaMirrorMaker2Deletion(GatekeeperKafkaMirrorMaker2DeletionContext context, String namespace, String name) {
            order.add(tag);
            return CompletableFuture.completedFuture(null);
        }
    }

    ///////////////////////////////////////////////////
    // KafkaBridge plugins
    ///////////////////////////////////////////////////

    private final class BridgeMutator implements GatekeeperKafkaBridgeMutatingPlugin {
        private final String tag;

        private BridgeMutator(String tag) {
            this.tag = tag;
        }

        @Override
        public CompletionStage<KafkaBridge> kafkaBridgeEntry(GatekeeperKafkaBridgeEntryContext context, KafkaBridge kafkaBridge) {
            order.add(tag);
            KafkaBridge mutated = new KafkaBridgeBuilder(kafkaBridge)
                    .editOrNewMetadata()
                        .withName(append(kafkaBridge.getMetadata().getName(), tag))
                    .endMetadata()
                    .build();
            return CompletableFuture.completedFuture(mutated);
        }

        @Override
        public CompletionStage<KafkaBridgeStatus> kafkaBridgeExit(GatekeeperKafkaBridgeExitContext context, KafkaBridge kafkaBridge, KafkaBridgeStatus newKafkaBridgeStatus) {
            newKafkaBridgeStatus.setObservedGeneration(1874L);
            return CompletableFuture.completedFuture(newKafkaBridgeStatus);
        }

        @Override
        public CompletionStage<Void> kafkaBridgeDeletion(GatekeeperKafkaBridgeDeletionContext context, String namespace, String name) {
            order.add(tag);
            return CompletableFuture.completedFuture(null);
        }
    }

    private final class BridgeValidator implements GatekeeperKafkaBridgeValidatingPlugin {
        private final String tag;
        private final boolean fail;

        private BridgeValidator(String tag, boolean fail) {
            this.tag = tag;
            this.fail = fail;
        }

        @Override
        public CompletionStage<Void> kafkaBridgeEntry(GatekeeperKafkaBridgeEntryContext context, KafkaBridge kafkaBridge) {
            order.add(tag);
            return fail
                    ? CompletableFuture.failedFuture(new RuntimeException("rejected by " + tag))
                    : CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> kafkaBridgeExit(GatekeeperKafkaBridgeExitContext context, KafkaBridge kafkaBridge, KafkaBridgeStatus newKafkaBridgeStatus) {
            newKafkaBridgeStatus.setObservedGeneration(1919L);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> kafkaBridgeDeletion(GatekeeperKafkaBridgeDeletionContext context, String namespace, String name) {
            order.add(tag);
            return CompletableFuture.completedFuture(null);
        }
    }

    ///////////////////////////////////////////////////
    // KafkaRebalance plugins
    ///////////////////////////////////////////////////

    private final class RebalanceMutator implements GatekeeperKafkaRebalanceMutatingPlugin {
        private final String tag;

        private RebalanceMutator(String tag) {
            this.tag = tag;
        }

        @Override
        public CompletionStage<KafkaRebalance> kafkaRebalanceEntry(GatekeeperKafkaRebalanceEntryContext context, KafkaRebalance kafkaRebalance) {
            order.add(tag);
            KafkaRebalance mutated = new KafkaRebalanceBuilder(kafkaRebalance)
                    .editOrNewMetadata()
                        .withName(append(kafkaRebalance.getMetadata().getName(), tag))
                    .endMetadata()
                    .build();
            return CompletableFuture.completedFuture(mutated);
        }

        @Override
        public CompletionStage<KafkaRebalanceStatus> kafkaRebalanceExit(GatekeeperKafkaRebalanceExitContext context, KafkaRebalance kafkaRebalance, KafkaRebalanceStatus newKafkaRebalanceStatus) {
            newKafkaRebalanceStatus.setObservedGeneration(1874L);
            return CompletableFuture.completedFuture(newKafkaRebalanceStatus);
        }

        @Override
        public CompletionStage<Void> kafkaRebalanceDeletion(GatekeeperKafkaRebalanceDeletionContext context, String namespace, String name) {
            order.add(tag);
            return CompletableFuture.completedFuture(null);
        }
    }

    private final class RebalanceValidator implements GatekeeperKafkaRebalanceValidatingPlugin {
        private final String tag;
        private final boolean fail;

        private RebalanceValidator(String tag, boolean fail) {
            this.tag = tag;
            this.fail = fail;
        }

        @Override
        public CompletionStage<Void> kafkaRebalanceEntry(GatekeeperKafkaRebalanceEntryContext context, KafkaRebalance kafkaRebalance) {
            order.add(tag);
            return fail
                    ? CompletableFuture.failedFuture(new RuntimeException("rejected by " + tag))
                    : CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> kafkaRebalanceExit(GatekeeperKafkaRebalanceExitContext context, KafkaRebalance kafkaRebalance, KafkaRebalanceStatus newKafkaRebalanceStatus) {
            newKafkaRebalanceStatus.setObservedGeneration(1919L);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> kafkaRebalanceDeletion(GatekeeperKafkaRebalanceDeletionContext context, String namespace, String name) {
            order.add(tag);
            return CompletableFuture.completedFuture(null);
        }
    }
}
