/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.gatekeeper.plugin;

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
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatus;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserStatus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InjectedFailureGatekeeperPluginTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String NAME = "my-user";
    private static final KafkaUser USER = new KafkaUserBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(NAME)
            .endMetadata()
            .build();

    private final InjectedFailureGatekeeperPlugin plugin = new InjectedFailureGatekeeperPlugin();

    @Test
    public void testEntryFailsWhenAnnotated() {
        CompletableFuture<Void> result = plugin
                .kafkaUserEntry(null, kafkaUser(Map.of(InjectedFailureGatekeeperPlugin.ENTRY_FAILURE_ANNOTATION, "true")))
                .toCompletableFuture();

        CompletionException thrown = assertThrows(CompletionException.class, result::join);
        assertThat(thrown.getCause(), instanceOf(RuntimeException.class));
        assertThat(thrown.getCause().getMessage(), allOf(containsString("Injected Gatekeeper failure"), containsString("entry")));
    }

    @Test
    public void testEntryCompletesWhenNotAnnotated() {
        Void result = assertDoesNotThrow(() -> plugin.kafkaUserEntry(null, kafkaUser(Map.of())).toCompletableFuture().join());

        assertThat(result, is((Void) null));
    }

    @Test
    public void testEntryDoesNotFailWhenOnlyTheExitAnnotationIsSet() {
        // Only the entry annotation enables the entry failure
        assertDoesNotThrow(() -> plugin.kafkaUserEntry(null, kafkaUser(Map.of(InjectedFailureGatekeeperPlugin.EXIT_FAILURE_ANNOTATION, "true"))).toCompletableFuture().join());
    }

    @Test
    public void testExitFailsWhenAnnotated() {
        CompletableFuture<Void> result = plugin
                .kafkaUserExit(null, kafkaUser(Map.of(InjectedFailureGatekeeperPlugin.EXIT_FAILURE_ANNOTATION, "true")), null)
                .toCompletableFuture();

        CompletionException thrown = assertThrows(CompletionException.class, result::join);
        assertThat(thrown.getCause().getMessage(), allOf(containsString("Injected Gatekeeper failure"), containsString("exit")));
    }

    @Test
    public void testExitCompletesWhenNotAnnotated() {
        assertDoesNotThrow(() -> plugin.kafkaUserExit(null, kafkaUser(Map.of()), null).toCompletableFuture().join());
    }

    @Test
    public void testExitDoesNotFailWhenOnlyTheEntryAnnotationIsSet() {
        // Only the exit annotation enables the exit failure
        assertDoesNotThrow(() -> plugin.kafkaUserExit(null, kafkaUser(Map.of(InjectedFailureGatekeeperPlugin.ENTRY_FAILURE_ANNOTATION, "true")), null).toCompletableFuture().join());
    }

    @Test
    public void testDeletionAlwaysFails() {
        CompletableFuture<Void> result = plugin
                .kafkaUserDeletion(null, NAMESPACE, NAME)
                .toCompletableFuture();

        CompletionException thrown = assertThrows(CompletionException.class, result::join);
        assertThat(thrown.getCause().getMessage(), allOf(containsString("Injected Gatekeeper failure"), containsString("deletion")));
    }

    @Test
    public void testAllOperandEntryAndExitHooksPassThroughWithoutAnnotations() {
        // Without the failure annotations, every operand's entry and exit hooks must complete normally
        Kafka kafka = new KafkaBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        KafkaNodePool pool = new KafkaNodePoolBuilder().withNewMetadata().withNamespace(NAMESPACE).withName("pool").endMetadata().build();
        assertDoesNotThrow(() -> plugin.kafkaEntry(null, kafka, List.of(pool)).toCompletableFuture().join());
        assertDoesNotThrow(() -> plugin.kafkaExit(null, kafka, List.of(pool), new KafkaStatus()).toCompletableFuture().join());
        assertDoesNotThrow(() -> plugin.kafkaNodePoolExit(null, kafka, pool, new KafkaNodePoolStatus()).toCompletableFuture().join());

        KafkaConnect connect = new KafkaConnectBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        KafkaConnector connector = new KafkaConnectorBuilder().withNewMetadata().withNamespace(NAMESPACE).withName("connector").endMetadata().build();
        assertDoesNotThrow(() -> plugin.kafkaConnectEntry(null, connect, List.of(connector)).toCompletableFuture().join());
        assertDoesNotThrow(() -> plugin.kafkaConnectExit(null, connect, List.of(connector), new KafkaConnectStatus()).toCompletableFuture().join());
        assertDoesNotThrow(() -> plugin.kafkaConnectorExit(null, connect, connector, new KafkaConnectorStatus()).toCompletableFuture().join());

        KafkaMirrorMaker2 mirrorMaker2 = new KafkaMirrorMaker2Builder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        assertDoesNotThrow(() -> plugin.kafkaMirrorMaker2Entry(null, mirrorMaker2).toCompletableFuture().join());
        assertDoesNotThrow(() -> plugin.kafkaMirrorMaker2Exit(null, mirrorMaker2, new KafkaMirrorMaker2Status()).toCompletableFuture().join());

        KafkaBridge bridge = new KafkaBridgeBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        assertDoesNotThrow(() -> plugin.kafkaBridgeEntry(null, bridge).toCompletableFuture().join());
        assertDoesNotThrow(() -> plugin.kafkaBridgeExit(null, bridge, new KafkaBridgeStatus()).toCompletableFuture().join());

        KafkaRebalance rebalance = new KafkaRebalanceBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        assertDoesNotThrow(() -> plugin.kafkaRebalanceEntry(null, rebalance).toCompletableFuture().join());
        assertDoesNotThrow(() -> plugin.kafkaRebalanceExit(null, rebalance, new KafkaRebalanceStatus()).toCompletableFuture().join());

        KafkaTopic topic = new KafkaTopicBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        assertDoesNotThrow(() -> plugin.kafkaTopicEntry(null, topic).toCompletableFuture().join());
        assertDoesNotThrow(() -> plugin.kafkaTopicExit(null, topic, new KafkaTopicStatus()).toCompletableFuture().join());

        KafkaUser user = new KafkaUserBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(NAME).endMetadata().build();
        assertDoesNotThrow(() -> plugin.kafkaUserEntry(null, user).toCompletableFuture().join());
        assertDoesNotThrow(() -> plugin.kafkaUserExit(null, user, new KafkaUserStatus()).toCompletableFuture().join());
    }

    @Test
    public void testAllOperandDeletionHooksAlwaysFail() {
        assertThrows(CompletionException.class, () -> plugin.kafkaDeletion(null, NAMESPACE, NAME).toCompletableFuture().join());
        assertThrows(CompletionException.class, () -> plugin.kafkaConnectDeletion(null, NAMESPACE, NAME).toCompletableFuture().join());
        assertThrows(CompletionException.class, () -> plugin.kafkaMirrorMaker2Deletion(null, NAMESPACE, NAME).toCompletableFuture().join());
        assertThrows(CompletionException.class, () -> plugin.kafkaBridgeDeletion(null, NAMESPACE, NAME).toCompletableFuture().join());
        assertThrows(CompletionException.class, () -> plugin.kafkaRebalanceDeletion(null, NAMESPACE, NAME).toCompletableFuture().join());
        assertThrows(CompletionException.class, () -> plugin.kafkaTopicDeletion(null, NAMESPACE, NAME).toCompletableFuture().join());
        assertThrows(CompletionException.class, () -> plugin.kafkaUserDeletion(null, NAMESPACE, NAME).toCompletableFuture().join());
    }

    //////////////////////////////////////////////////
    // Helper methods
    //////////////////////////////////////////////////

    private static KafkaUser kafkaUser(Map<String, String> annotations) {
        return new KafkaUserBuilder(USER)
                .editMetadata()
                    .withAnnotations(annotations)
                .endMetadata()
                .build();
    }
}
