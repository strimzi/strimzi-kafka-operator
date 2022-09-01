/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.api.kafka.model.balancing.KafkaRebalanceState;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The purpose of this test is to check that custom resource predicates correctly
 * identify CRDs in specific states.
 */
class CustomResourceConditionsTest {

    Predicate<Kafka> kafkaIsReady = CustomResourceConditions.isReady();

    Predicate<KafkaRebalance> customConditionPresent = CustomResourceConditions.isLatestGenerationAndAnyConditionMatches("ProposalReady", "True");

    @Test
    public void testIsReady_EmptyCrdIsNotReady() {
        Kafka emptyKafka = new KafkaBuilder().build();
        assertFalse(kafkaIsReady.test(emptyKafka));
    }

    @Test
    public void testIsReady_ReadyKafka() {
        Kafka build = new KafkaBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .addNewCondition().withType("Ready").withStatus("True").endCondition()
                .endStatus().build();
        assertTrue(kafkaIsReady.test(build));
    }

    @Test
    public void testIsReady_ConditionMissing() {
        Kafka build = new KafkaBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .endStatus()
                .build();
        assertFalse(kafkaIsReady.test(build));
    }

    @Test
    public void testIsReady_ConditionNotTrue() {
        Kafka build = new KafkaBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .addNewCondition().withType("Ready").withStatus("False").endCondition()
                .endStatus()
                .build();
        assertFalse(kafkaIsReady.test(build));
    }

    @Test
    public void testIsReady_NullStatus() {
        Kafka build = new KafkaBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .build();
        assertFalse(kafkaIsReady.test(build));
    }

    @Test
    public void testIsReady_ObservedGenerationNotEqualToMetadataGeneration() {
        Kafka build = new KafkaBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(1L)
                .addNewCondition().withType("Ready").withStatus("True").endCondition()
                .endStatus()
                .build();
        assertFalse(kafkaIsReady.test(build));
    }


    @Test
    public void testIsLatestGenerationAndAnyConditionMatches_EmptyCrd() {
        KafkaRebalance build = new KafkaRebalanceBuilder().build();
        assertFalse(customConditionPresent.test(build));
    }


    @Test
    public void testIsLatestGenerationAndAnyConditionMatches_ReadyProposal() {
        KafkaRebalance build = new KafkaRebalanceBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .addNewCondition().withType("ProposalReady").withStatus("True").endCondition()
                .endStatus()
                .build();
        assertTrue(customConditionPresent.test(build));
    }

    @Test
    public void testIsLatestGenerationAndAnyConditionMatches_ConditionMissing() {
        KafkaRebalance build = new KafkaRebalanceBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .endStatus()
                .build();
        assertFalse(customConditionPresent.test(build));
    }

    @Test
    public void testIsLatestGenerationAndAnyConditionMatches_ConditionNotTrue() {
        KafkaRebalance build = new KafkaRebalanceBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .addNewCondition().withType("ProposalReady").withStatus("False").endCondition()
                .endStatus()
                .build();
        assertFalse(customConditionPresent.test(build));
    }

    @Test
    public void testIsLatestGenerationAndAnyConditionMatches_NullStatus() {
        KafkaRebalance build = new KafkaRebalanceBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .build();
        assertFalse(customConditionPresent.test(build));
    }

    @Test
    public void testIsLatestGenerationAndAnyConditionMatches_ObservedGenerationNotEqualToMetadataGeneration() {
        KafkaRebalance build = new KafkaRebalanceBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(1L)
                .addNewCondition().withType("ProposalReady").withStatus("True").endCondition()
                .endStatus()
                .build();
        assertFalse(customConditionPresent.test(build));
    }

    @Test
    public void testKafkaIsReady() {
        Kafka build = new KafkaBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .addNewCondition().withType("Ready").withStatus("True").endCondition()
                .endStatus().build();
        assertTrue(Kafka.isReady().test(build));
    }

    @Test
    public void testKafkaConnectorIsReady() {
        KafkaConnector build = new KafkaConnectorBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .addNewCondition().withType("Ready").withStatus("True").endCondition()
                .endStatus().build();
        assertTrue(KafkaConnector.isReady().test(build));
    }

    @Test
    public void testKafkaConnectIsReady() {
        KafkaConnect build = new KafkaConnectBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .addNewCondition().withType("Ready").withStatus("True").endCondition()
                .endStatus().build();
        assertTrue(KafkaConnect.isReady().test(build));
    }

    @Test
    public void testKafkaTopicIsReady() {
        KafkaTopic build = new KafkaTopicBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .addNewCondition().withType("Ready").withStatus("True").endCondition()
                .endStatus().build();
        assertTrue(KafkaTopic.isReady().test(build));
    }

    @Test
    public void testKafkaBridgeIsReady() {
        KafkaBridge build = new KafkaBridgeBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .addNewCondition().withType("Ready").withStatus("True").endCondition()
                .endStatus().build();
        assertTrue(KafkaBridge.isReady().test(build));
    }

    @Test
    public void testKafkaMirrorMaker2IsReady() {
        KafkaMirrorMaker2 build = new KafkaMirrorMaker2Builder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .addNewCondition().withType("Ready").withStatus("True").endCondition()
                .endStatus().build();
        assertTrue(KafkaMirrorMaker2.isReady().test(build));
    }

    @Test
    public void testKafkaUserIsReady() {
        KafkaUser build = new KafkaUserBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .addNewCondition().withType("Ready").withStatus("True").endCondition()
                .endStatus().build();
        assertTrue(KafkaUser.isReady().test(build));
    }

    @Test
    public void testKafkaRebalanceIsState() {
        for (KafkaRebalanceState crdState : KafkaRebalanceState.values()) {
            KafkaRebalance build = new KafkaRebalanceBuilder()
                    .editMetadata().withGeneration(2L).endMetadata()
                    .withNewStatus()
                    .withObservedGeneration(2L)
                    .addNewCondition().withType(crdState.name()).withStatus("True").endCondition()
                    .endStatus().build();
            for (KafkaRebalanceState testState : KafkaRebalanceState.values()) {
                assertEquals(KafkaRebalance.isInState(testState).test(build), testState == crdState);
            }
        }
    }
}