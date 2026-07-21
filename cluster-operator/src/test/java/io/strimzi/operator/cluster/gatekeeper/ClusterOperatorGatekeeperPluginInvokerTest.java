/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.gatekeeper;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.KafkaStatusBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.operator.common.gatekeeper.GatekeeperPluginFactory;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaDeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaEntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaMutatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaValidatingPlugin;
import io.strimzi.plugin.gatekeeper.KafkaAndKafkaNodePools;
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
 * as the User and Topic Operator invokers, so these tests focus on the Kafka methods, which exercise the
 * Cluster-Operator-specific parts (the compound KafkaAndKafkaNodePools resource and the KafkaNodePool exit).
 */
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

    private static Kafka kafka(String name) {
        return new KafkaBuilder().withNewMetadata().withName(name).endMetadata().withNewSpec().endSpec().build();
    }

    private static String append(String current, String tag) {
        return (current == null ? "" : current) + tag;
    }

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
}
