/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The purpose of this test is to check that custom resource predicates correctly
 * identify CRDs in specific states
 */
class CustomResourceConditionsTest {

    Predicate<Kafka> isReady = CustomResourceConditions.isReady();

    Predicate<KafkaRebalance> isProposalReady = CustomResourceConditions.isKafkaRebalanceProposalReady();

    @Test
    public void testIsReady_EmptyKafkaNotReady() {
        Kafka build = new KafkaBuilder().build();
        assertFalse(isReady.test(build));
    }

    @Test
    public void testIsReady_ReadyKafka() {
        Kafka build = new KafkaBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .addNewCondition().withType("Ready").withStatus("True").endCondition()
                .endStatus()
                .build();
        assertTrue(isReady.test(build));
    }

    @Test
    public void testIsReady_ConditionMissing() {
        Kafka build = new KafkaBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .endStatus()
                .build();
        assertFalse(isReady.test(build));
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
        assertFalse(isReady.test(build));
    }

    @Test
    public void testIsReady_NullStatus() {
        Kafka build = new KafkaBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .build();
        assertFalse(isReady.test(build));
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
        assertFalse(isReady.test(build));
    }


    @Test
    public void testIsProposalReady_EmptyKafkaRebalanceNotProposalReady() {
        KafkaRebalance build = new KafkaRebalanceBuilder().build();
        assertFalse(isProposalReady.test(build));
    }


    @Test
    public void testIsProposalReady_ReadyProposal() {
        KafkaRebalance build = new KafkaRebalanceBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .addNewCondition().withType("ProposalReady").withStatus("True").endCondition()
                .endStatus()
                .build();
        assertTrue(isProposalReady.test(build));
    }

    @Test
    public void testIsProposalReady_ConditionMissing() {
        KafkaRebalance build = new KafkaRebalanceBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .endStatus()
                .build();
        assertFalse(isProposalReady.test(build));
    }

    @Test
    public void testIsProposalReady_ConditionNotTrue() {
        KafkaRebalance build = new KafkaRebalanceBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(2L)
                .addNewCondition().withType("ProposalReady").withStatus("False").endCondition()
                .endStatus()
                .build();
        assertFalse(isProposalReady.test(build));
    }

    @Test
    public void testIsProposalReady_NullStatus() {
        KafkaRebalance build = new KafkaRebalanceBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .build();
        assertFalse(isProposalReady.test(build));
    }

    @Test
    public void testIsProposalReady_ObservedGenerationNotEqualToMetadataGeneration() {
        KafkaRebalance build = new KafkaRebalanceBuilder()
                .editMetadata().withGeneration(2L).endMetadata()
                .withNewStatus()
                .withObservedGeneration(1L)
                .addNewCondition().withType("ProposalReady").withStatus("True").endCondition()
                .endStatus()
                .build();
        assertFalse(isProposalReady.test(build));
    }

}