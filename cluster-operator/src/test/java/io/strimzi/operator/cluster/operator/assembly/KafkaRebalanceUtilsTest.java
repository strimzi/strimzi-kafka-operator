/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.KafkaStatusBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceMode;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatus;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatusBrokersBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatusBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatusBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
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

    @Test
    public void testAutoRebalanceNoStatusNoAddedNodes() {
        KafkaStatus kafkaStatus = new KafkaStatusBuilder().build();
        KafkaRebalanceUtils.updateKafkaAutoRebalanceStatus(kafkaStatus, null, Set.of());
        assertThat(kafkaStatus.getAutoRebalance(), is(notNullValue()));
    }

    @Test
    public void testAutoRebalanceNoStatusNewAddedNodes() {
        KafkaStatus kafkaStatus = new KafkaStatusBuilder().build();
        KafkaRebalanceUtils.updateKafkaAutoRebalanceStatus(kafkaStatus, null, Set.of(3, 4));
        assertThat(kafkaStatus.getAutoRebalance(), is(notNullValue()));
        assertThat(kafkaStatus.getAutoRebalance().getModes(), is(notNullValue()));
        assertThat(kafkaStatus.getAutoRebalance().getModes().size(), is(1));
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getMode(), is(KafkaAutoRebalanceMode.ADD_BROKERS));
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getBrokers().stream().collect(Collectors.toSet()).equals(Set.of(3, 4)), is(true));
    }

    @Test
    public void testAutoRebalanceStatusNoModesNoAddedNodes() {
        KafkaStatus kafkaStatus = new KafkaStatusBuilder().build();
        KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus = new KafkaAutoRebalanceStatusBuilder().build();
        KafkaRebalanceUtils.updateKafkaAutoRebalanceStatus(kafkaStatus, kafkaAutoRebalanceStatus, Set.of());
        assertThat(kafkaStatus.getAutoRebalance(), is(notNullValue()));
        assertThat(kafkaStatus.getAutoRebalance().getModes(), is(nullValue()));
    }

    @Test
    public void testAutoRebalanceStatusNoModesNewAddedNodes() {
        KafkaStatus kafkaStatus = new KafkaStatusBuilder().build();
        KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus = new KafkaAutoRebalanceStatusBuilder().build();
        KafkaRebalanceUtils.updateKafkaAutoRebalanceStatus(kafkaStatus, kafkaAutoRebalanceStatus, Set.of(3, 4));
        assertThat(kafkaStatus.getAutoRebalance(), is(notNullValue()));
        assertThat(kafkaStatus.getAutoRebalance().getModes(), is(notNullValue()));
        assertThat(kafkaStatus.getAutoRebalance().getModes().size(), is(1));
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getMode(), is(KafkaAutoRebalanceMode.ADD_BROKERS));
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getBrokers().stream().collect(Collectors.toSet()).equals(Set.of(3, 4)), is(true));
    }

    @Test
    public void testAutoRebalanceStatusNoAddedNodes() {
        KafkaStatus kafkaStatus = new KafkaStatusBuilder().build();
        KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus = new KafkaAutoRebalanceStatusBuilder()
                .withModes(
                        new KafkaAutoRebalanceStatusBrokersBuilder()
                                .withMode(KafkaAutoRebalanceMode.ADD_BROKERS)
                                .withBrokers(List.of(3, 4))
                                .build()
                )
                .build();
        KafkaRebalanceUtils.updateKafkaAutoRebalanceStatus(kafkaStatus, kafkaAutoRebalanceStatus, Set.of());
        assertThat(kafkaStatus.getAutoRebalance(), is(notNullValue()));
        assertThat(kafkaStatus.getAutoRebalance().getModes(), is(notNullValue()));
        assertThat(kafkaStatus.getAutoRebalance().getModes().size(), is(1));
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getMode(), is(KafkaAutoRebalanceMode.ADD_BROKERS));
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getBrokers().stream().collect(Collectors.toSet()).equals(Set.of(3, 4)), is(true));
    }

    @Test
    public void testAutoRebalanceStatusNewAddedNodes() {
        KafkaStatus kafkaStatus = new KafkaStatusBuilder().build();
        KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus = new KafkaAutoRebalanceStatusBuilder()
                .withModes(
                        new KafkaAutoRebalanceStatusBrokersBuilder()
                                .withMode(KafkaAutoRebalanceMode.ADD_BROKERS)
                                .withBrokers(List.of(3, 4))
                                .build()
                )
                .build();
        KafkaRebalanceUtils.updateKafkaAutoRebalanceStatus(kafkaStatus, kafkaAutoRebalanceStatus, Set.of(5, 6));
        assertThat(kafkaStatus.getAutoRebalance(), is(notNullValue()));
        assertThat(kafkaStatus.getAutoRebalance().getModes(), is(notNullValue()));
        assertThat(kafkaStatus.getAutoRebalance().getModes().size(), is(1));
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getMode(), is(KafkaAutoRebalanceMode.ADD_BROKERS));
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getBrokers().stream().collect(Collectors.toSet()).equals(Set.of(3, 4, 5, 6)), is(true));
    }
}
