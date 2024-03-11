/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.KRaft;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.KRaftDualWriting;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.KRaftMigration;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.KRaftPostMigration;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.PreKRaft;
import static io.strimzi.api.kafka.model.kafka.KafkaMetadataState.ZooKeeper;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Tests the state transitions which happens in the KafkaMetadataStateManager class.
 */
public class KafkaMetadataStateManagerTest {

    private static final String CLUSTER_NAMESPACE = "my-namespace";

    private static final String CLUSTER_NAME = "kafka-test-cluster";

    private static final int REPLICAS = 3;

    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled"))
                .withNamespace(CLUSTER_NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withReplicas(REPLICAS)
                .endKafka()
                .withNewZookeeper()
                    .withReplicas(REPLICAS)
                .endZookeeper()
            .endSpec()
            .build();

    @Test
    public void testFromZookeeperToKRaftMigration() {
        // test with no metadata state set
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration")
                .endMetadata()
                .build();

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        assertEquals(kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus()), KRaftMigration);

        // test with ZooKeeper metadata state set
        kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(ZooKeeper)
                .endStatus()
                .build();

        kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        assertEquals(kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus()), KRaftMigration);
    }

    @Test
    public void testFromZookeeperToKRaftMigrationFailsKRaftDisabled() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(ZooKeeper)
                .endStatus()
                .build();

        var exception = assertThrows(IllegalArgumentException.class, () ->
                new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, false));

        assertEquals("Failed to reconcile a KRaft enabled cluster or migration to KRaft because useKRaft feature gate is disabled",
                exception.getMessage());
    }

    @Test
    public void testFromKRaftMigrationToKRaftDualWriting() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftMigration)
                .endStatus()
                .build();

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        // check staying in KRaftMigration, migration is not done yet
        assertEquals(kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus()), KRaftMigration);
        // set migration done and check move to KRaftDualWriting
        kafkaMetadataStateManager.setMigrationDone(true);
        assertEquals(kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus()), KRaftDualWriting);
    }

    @Test
    public void testFromKRaftDualWritingToKRaftPostMigration() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftDualWriting)
                .endStatus()
                .build();

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        assertEquals(kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus()), KRaftPostMigration);
    }

    @Test
    public void testFromKRaftPostMigrationToPreKRaft() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftPostMigration)
                .endStatus()
                .build();

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        assertEquals(kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus()), PreKRaft);
    }

    @Test
    public void testFromPreKRaftToKRaft() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(PreKRaft)
                .endStatus()
                .build();

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        assertEquals(kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus()), KRaft);
    }

    @Test
    public void testFromKRaftPostMigrationToKraftDualWriting() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "rollback")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftPostMigration)
                .endStatus()
                .build();

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        assertEquals(kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus()), KRaftDualWriting);
    }

    @Test
    public void testFromKRaftDualWritingToZookeeper() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "disabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftDualWriting)
                .endStatus()
                .build();

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        assertEquals(kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus()), ZooKeeper);
    }

    @Test
    public void testWarningInZooKeeper() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(ZooKeeper)
                .endStatus()
                .build();

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus());
        assertTrue(kafka.getStatus().getConditions().stream().anyMatch(condition -> "KafkaMetadataStateWarning".equals(condition.getReason())));
        assertEquals(kafka.getStatus().getConditions().get(0).getMessage(),
                "The strimzi.io/kraft annotation can't be set to 'enabled' because the cluster is ZooKeeper-based. " +
                        "If you want to migrate it to be KRaft-based apply the 'migration' value instead.");
        assertEquals(kafka.getStatus().getKafkaMetadataState(), ZooKeeper);

        kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "rollback")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(ZooKeeper)
                .endStatus()
                .build();

        kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus());
        assertTrue(kafka.getStatus().getConditions().stream().anyMatch(condition -> "KafkaMetadataStateWarning".equals(condition.getReason())));
        assertEquals(kafka.getStatus().getConditions().get(0).getMessage(),
                "The strimzi.io/kraft annotation can't be set to 'rollback' because the cluster is already ZooKeeper-based. " +
                        "There is no migration ongoing to rollback. If you want to migrate it to be KRaft-based apply the 'migration' value instead.");
        assertEquals(kafka.getStatus().getKafkaMetadataState(), ZooKeeper);
    }

    @Test
    public void testWarningInKRaftMigration() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftMigration)
                .endStatus()
                .build();

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus());
        assertTrue(kafka.getStatus().getConditions().stream().anyMatch(condition -> "KafkaMetadataStateWarning".equals(condition.getReason())));
        assertEquals(kafka.getStatus().getConditions().get(0).getMessage(),
                "The strimzi.io/kraft annotation can't be set to 'enabled' during a migration process. " +
                        "It has to be used in post migration to finalize it and move definitely to KRaft.");

        kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "rollback")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftMigration)
                .endStatus()
                .build();

        kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus());
        assertTrue(kafka.getStatus().getConditions().stream().anyMatch(condition -> "KafkaMetadataStateWarning".equals(condition.getReason())));
        assertEquals(kafka.getStatus().getConditions().get(0).getMessage(),
                "The strimzi.io/kraft annotation can't be set to 'rollback' during a migration process. " +
                        "It can be used in post migration to start rollback process.");
    }

    @Test
    public void testWarningInKRaftDualWriting() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftDualWriting)
                .endStatus()
                .build();

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus());
        assertTrue(kafka.getStatus().getConditions().stream().anyMatch(condition -> "KafkaMetadataStateWarning".equals(condition.getReason())));
        assertEquals(kafka.getStatus().getConditions().get(0).getMessage(),
                "The strimzi.io/kraft annotation can't be set to 'enabled' during a migration process. " +
                        "It has to be used in post migration to finalize it and move definitely to KRaft.");

        kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "rollback")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftDualWriting)
                .endStatus()
                .build();

        kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus());
        assertTrue(kafka.getStatus().getConditions().stream().anyMatch(condition -> "KafkaMetadataStateWarning".equals(condition.getReason())));
        assertEquals(kafka.getStatus().getConditions().get(0).getMessage(),
                "The strimzi.io/kraft annotation can't be set to 'rollback' during dual writing. " +
                        "It can be used in post migration to start rollback process.");
    }

    @Test
    public void testWarningInKRaftPostMigration() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "disabled")
                .endMetadata()
                .withNewStatus()
                    .withKafkaMetadataState(KRaftPostMigration)
                .endStatus()
                .build();

        KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
        kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus());
        assertTrue(kafka.getStatus().getConditions().stream().anyMatch(condition -> "KafkaMetadataStateWarning".equals(condition.getReason())));
        assertEquals(kafka.getStatus().getConditions().get(0).getMessage(),
                "The strimzi.io/kraft annotation can't be set to 'migration' or 'disabled' in the post-migration. " +
                        "You can use 'rollback' value to come back to ZooKeeper. Use the 'enabled' value to finalize migration instead.");
    }

    @Test
    public void testWarningInPreKRaft() {
        List<String> wrongAnnotations = List.of("rollback", "disabled");
        for (String annotation : wrongAnnotations) {
            Kafka kafka = new KafkaBuilder(KAFKA)
                    .editMetadata()
                        .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, annotation)
                    .endMetadata()
                    .withNewStatus()
                        .withKafkaMetadataState(PreKRaft)
                    .endStatus()
                    .build();

            KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
            kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus());
            assertTrue(kafka.getStatus().getConditions().stream().anyMatch(condition -> "KafkaMetadataStateWarning".equals(condition.getReason())));
            assertEquals(kafka.getStatus().getConditions().get(0).getMessage(),
                    "The strimzi.io/kraft annotation can't be set to 'migration', 'disabled' or 'rollback' in the pre-kraft. " +
                            "Use the 'enabled' value to finalize migration and removing ZooKeeper.");
            assertEquals(kafka.getStatus().getKafkaMetadataState(), PreKRaft);
        }
    }

    @Test
    public void testWarningInKRaft() {
        List<String> wrongAnnotations = List.of("rollback", "disabled", "migration");
        for (String annotation : wrongAnnotations) {
            Kafka kafka = new KafkaBuilder(KAFKA)
                    .editMetadata()
                        .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, annotation)
                    .endMetadata()
                    .withNewStatus()
                        .withKafkaMetadataState(KRaft)
                    .endStatus()
                    .build();

            KafkaMetadataStateManager kafkaMetadataStateManager = new KafkaMetadataStateManager(Reconciliation.DUMMY_RECONCILIATION, kafka, true);
            kafkaMetadataStateManager.computeNextMetadataState(kafka.getStatus());
            assertTrue(kafka.getStatus().getConditions().stream().anyMatch(condition -> "KafkaMetadataStateWarning".equals(condition.getReason())));
            assertEquals(kafka.getStatus().getConditions().get(0).getMessage(),
                    "The strimzi.io/kraft annotation can't be set to 'migration', 'rollback' or 'disabled' because the cluster is already KRaft.");
            assertEquals(kafka.getStatus().getKafkaMetadataState(), KRaft);
        }
    }
}
