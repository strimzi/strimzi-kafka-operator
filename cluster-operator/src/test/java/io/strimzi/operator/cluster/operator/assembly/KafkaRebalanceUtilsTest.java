/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatusBuilder;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaRebalanceUtilsTest {

    @Test
    public void testValidSingleState() {
        KafkaRebalanceStatus kafkaRebalanceStatus = new KafkaRebalanceStatusBuilder()
                .withConditions(
                        new ConditionBuilder()
                                .withType(KafkaRebalanceState.Rebalancing.toString())
                                .withStatus("True")
                                .build())
                .build();

        KafkaRebalanceState state = KafkaRebalanceUtils.rebalanceState(kafkaRebalanceStatus);
        assertThat(state, is(KafkaRebalanceState.Rebalancing));
    }

    @Test
    public void testMultipleState() {
        KafkaRebalanceStatus kafkaRebalanceStatus = new KafkaRebalanceStatusBuilder()
                .withConditions(
                        new ConditionBuilder()
                                .withType(KafkaRebalanceState.ProposalReady.toString())
                                .withStatus("True")
                                .build(),
                        new ConditionBuilder()
                                .withType(KafkaRebalanceState.Rebalancing.toString())
                                .withStatus("True")
                                .build())
                .build();

        Throwable ex = assertThrows(RuntimeException.class, () -> KafkaRebalanceUtils.rebalanceState(kafkaRebalanceStatus));
        assertThat(ex.getMessage(), is("Multiple KafkaRebalance State Conditions were present in the KafkaRebalance status"));
    }

    @Test
    public void testNoConditionWithState() {
        KafkaRebalanceStatus kafkaRebalanceStatus = new KafkaRebalanceStatusBuilder()
                .withConditions(
                        new ConditionBuilder()
                                .withType("Some other type")
                                .withStatus("True")
                                .build())
                .build();

        KafkaRebalanceState state = KafkaRebalanceUtils.rebalanceState(kafkaRebalanceStatus);
        assertThat(state, is(nullValue()));
    }

    @Test
    public void testNoConditions() {
        KafkaRebalanceStatus kafkaRebalanceStatus = new KafkaRebalanceStatusBuilder().build();

        KafkaRebalanceState state = KafkaRebalanceUtils.rebalanceState(kafkaRebalanceStatus);
        assertThat(state, is(nullValue()));
    }

    @Test
    public void testNullStatus() {
        KafkaRebalanceState state = KafkaRebalanceUtils.rebalanceState(null);
        assertThat(state, is(nullValue()));
    }
}
