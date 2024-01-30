/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaMetadataState;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import org.junit.Test;

import java.util.Map;

import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.KRaft;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.KRaftDualWriting;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.KRaftMigration;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.KRaftPostMigration;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.PreKRaft;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.ZooKeeper;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaMetadataStateManagerTest {

    private static final String CLUSTER_NAMESPACE = "my-namespace";

    private static final String CLUSTER_NAME = "kafka-test-cluster";

    private static final Reconciliation RECONCILIATION = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, CLUSTER_NAMESPACE, CLUSTER_NAME);

    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled"))
                .withNamespace(CLUSTER_NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName("plain")
                        .withPort(9092)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(false)
                    .build())
                .endKafka()
            .endSpec()
            .build();

    /**
     *  Computes the next state to which the previous state has transitioned
     *
     * @param kafka The Kafka instance.
     * @param status Status of the Kafka custom resource where warnings about any issues with metadata state will be added
     * @param isKRaftFeatureGateEnabled if the UseKRaft feature gate is enabled on the operator
     *
     * @return the state to which the previous state has transitioned
     */
    private KafkaMetadataState checkTransition(Kafka kafka, KafkaStatus status, boolean isKRaftFeatureGateEnabled) {
        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(RECONCILIATION, kafka, isKRaftFeatureGateEnabled);
        return kafkaMetadataStateManager.computeNextMetadataState(status);
    }

    @Test
    public void testMoveFromZookeeperToKRaftMigrationWhenMigrationAnnotationEnabled() {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState("ZooKeeper")
                .endStatus()
                .build();

        assertEquals(checkTransition(kafka, kafka.getStatus(), true), KRaftMigration);
    }

    @Test
    public void testMoveFromZookeeperToKRaftMigrationFailsWhenMigrationEnabledButKRaftDisabled() {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState("ZooKeeper")
                .endStatus()
                .build();

        var exception = assertThrows(IllegalArgumentException.class, () ->
                checkTransition(kafka, kafka.getStatus(), false));

        assertEquals("Failed to reconcile a KRaft enabled cluster or migration to KRaft because useKRaft feature gate is disabled",
                exception.getMessage());
    }

    @Test
    public void testMoveFromKRaftMigrationToKRaftDualWriteWhenMigrationEnabled() {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState("KRaftDualWriting")
                .endStatus()
                .build();

        assertEquals(checkTransition(kafka, kafka.getStatus(), true), KRaftPostMigration);
    }

    @Test
    public void testMoveFromKRaftPostMigrationToPreKRaftWhenEnabledAnnotationApplied() {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState("KRaftPostMigration")
                .endStatus()
                .build();

        assertEquals(checkTransition(kafka, kafka.getStatus(), true), PreKRaft);
    }

    @Test
    public void testMoveFromKRaftPostMigrationToKraftDualWritingWhenRollbackAnnotationApplied() {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "rollback")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState("KRaftPostMigration")
                .endStatus()
                .build();

        assertEquals(checkTransition(kafka, kafka.getStatus(), true), KRaftDualWriting);
    }

    @Test
    public void testMoveFromPreKRafttoKRaftMigrationWhenEnabledAnnotationApplied() {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState("PreKRaft")
                .endStatus()
                .build();

        assertEquals(checkTransition(kafka, kafka.getStatus(), true), KRaft);
    }

    @Test
    public void testMoveFromKRaftMigrationToZookeeperWhenDisabledAnnotationApplied() {

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "disabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState("KRaftMigration")
                .endStatus()
                .build();

        assertEquals(checkTransition(kafka, kafka.getStatus(), true), ZooKeeper);
    }
}
