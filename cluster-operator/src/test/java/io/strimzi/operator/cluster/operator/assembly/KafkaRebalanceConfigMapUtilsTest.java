/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.ExecutorStatus;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlExecutorState;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceAssemblyOperator.BROKER_LOAD_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.BYTE_MOVEMENT_COMPLETED;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.EXECUTOR_STATE_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.TIME_COMPLETED;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.updateRebalanceConfigMapWithProgressFields;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceStatusTest.buildOptimizationProposal;
import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.ExecutorStatusTest.createExecutorStatusJson;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaRebalanceConfigMapUtilsTest {

    private static final String BROKER_LOAD = buildOptimizationProposal().toString();
    private static final Map<String, String> BROKER_LOAD_MAP = Map.of(BROKER_LOAD_KEY, buildOptimizationProposal().toString());

    private static ConfigMap createKafkaRebalanceConfigMap(Map<String, String> brokerLoad) {
        return new ConfigMapBuilder()
                .addToData(brokerLoad)
                .build();
    }

    @Test
    public void testProgressFieldsForProposalReadyState() {
        ExecutorStatus es = new ExecutorStatus(createExecutorStatusJson(
                CruiseControlExecutorState.NO_TASK_IN_PROGRESS, null, null, null));
        ConfigMap cm = createKafkaRebalanceConfigMap(BROKER_LOAD_MAP);
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.ProposalReady, es, cm);

        Map<String, String> m =  cm.getData();
        assertThat(m.containsKey(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY), is(false));
        assertThat(m.get(COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY), is("0"));
        assertThat(m.containsKey(EXECUTOR_STATE_KEY), is(false));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));
    }

    @Test
    public void testProgressFieldsForProposalRebalancingState() {
        Instant taskStartTime = Instant.now().minusSeconds(2).truncatedTo(ChronoUnit.SECONDS);
        ExecutorStatus es0 = new ExecutorStatus(createExecutorStatusJson(
                CruiseControlExecutorState.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS, "250", "10000",
                String.format("No reason provided (Client: 172.17.0.1, Date: %s)", taskStartTime.toString())));
        ConfigMap cm = createKafkaRebalanceConfigMap(BROKER_LOAD_MAP);
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.Rebalancing, es0, cm);

        /*
         * Total Data to Move:          10,000 MB
         * Data Moved:                  250 MB
         * Time Since Task Start:       2 seconds
         * Data Rate:                   125 MB/s
         * Remaining Data:              9,750 MB
         * Estimated Time to Complete:  78 seconds
         */
        Map<String, String> m =  cm.getData();
        assertThat(m.get(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY), is("1"));
        assertThat(m.get(COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY), is("2"));
        assertThat(m.get(EXECUTOR_STATE_KEY), is(es0.getJson().toString()));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));
    }

    @Test
    public void testProgressFieldsForProposalStopped() throws Exception {
        Instant taskStartTime = Instant.now().minusSeconds(1).truncatedTo(ChronoUnit.SECONDS);
        ExecutorStatus es = new ExecutorStatus(createExecutorStatusJson(
                CruiseControlExecutorState.NO_TASK_IN_PROGRESS, "250", "10000",
                String.format("No reason provided (Client: 172.17.0.1, Date: %s)", taskStartTime.toString())));
        ConfigMap cm = createKafkaRebalanceConfigMap(Map.of(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY, "5",
                COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY, "100",
                EXECUTOR_STATE_KEY, es.toString(),
                BROKER_LOAD_KEY, BROKER_LOAD));
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.Stopped, es, cm);

        Map<String, String> m =  cm.getData();
        assertThat(m.containsKey(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY), is(false));
        assertThat(m.get(COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY), is("100"));
        assertThat(m.get(EXECUTOR_STATE_KEY), is(es.toString()));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));

        cm = createKafkaRebalanceConfigMap(Map.of(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY, "5",
                BROKER_LOAD_KEY, BROKER_LOAD));
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.Stopped, es, cm);

        m =  cm.getData();
        assertThat(m.containsKey(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY), is(false));
        assertThat(m.containsKey(COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY), is(false));
        assertThat(m.containsKey(EXECUTOR_STATE_KEY), is(false));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));
    }

    @Test
    public void testProgressFieldsForProposalNotReady() {
        ExecutorStatus es = new ExecutorStatus(createExecutorStatusJson(
                CruiseControlExecutorState.NO_TASK_IN_PROGRESS, "250", "10000", null));
        ConfigMap cm = createKafkaRebalanceConfigMap(Map.of(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY, "5",
                COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY, "100",
                EXECUTOR_STATE_KEY, es.toString(),
                BROKER_LOAD_KEY, BROKER_LOAD));
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.NotReady,  es, cm);

        Map<String, String> m =  cm.getData();
        assertThat(m.containsKey(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY), is(false));
        assertThat(m.get(COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY), is("100"));
        assertThat(m.get(EXECUTOR_STATE_KEY), is(es.toString()));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));

        cm = createKafkaRebalanceConfigMap(Map.of(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY, "5",
                BROKER_LOAD_KEY, BROKER_LOAD));
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.NotReady, es, cm);

        m =  cm.getData();
        assertThat(m.containsKey(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY), is(false));
        assertThat(m.containsKey(COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY), is(false));
        assertThat(m.containsKey(EXECUTOR_STATE_KEY), is(false));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));
    }

    @Test
    public void testProgressFieldsForReadyState() {
        ExecutorStatus es = new ExecutorStatus(createExecutorStatusJson(
                CruiseControlExecutorState.NO_TASK_IN_PROGRESS, null, null, null));
        ConfigMap cm = createKafkaRebalanceConfigMap(BROKER_LOAD_MAP);
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.Ready, es, cm);

        Map<String, String> m =  cm.getData();
        assertThat(m.get(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY), is(TIME_COMPLETED));
        assertThat(m.get(COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY), is(BYTE_MOVEMENT_COMPLETED));
        assertThat(m.containsKey(EXECUTOR_STATE_KEY), is(false));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));
    }

    @Test
    public void testProgressFieldsForNewAndPendingProposalStates() {
        ExecutorStatus es = new ExecutorStatus(createExecutorStatusJson(
                CruiseControlExecutorState.NO_TASK_IN_PROGRESS, null, null, null));
        ConfigMap cm = createKafkaRebalanceConfigMap(BROKER_LOAD_MAP);
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.New, es, cm);

        Map<String, String> m =  cm.getData();
        assertThat(m.containsKey(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY), is(false));
        assertThat(m.containsKey(COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY), is(false));
        assertThat(m.containsKey(EXECUTOR_STATE_KEY), is(false));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));

        es = new ExecutorStatus(createExecutorStatusJson(
                CruiseControlExecutorState.NO_TASK_IN_PROGRESS, null, null, null));
        cm = createKafkaRebalanceConfigMap(BROKER_LOAD_MAP);
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.PendingProposal, es, cm);

        m =  cm.getData();
        assertThat(m.containsKey(ESTIMATED_TIME_TO_COMPLETION_IN_MINUTES_KEY), is(false));
        assertThat(m.containsKey(COMPLETED_BYTE_MOVEMENT_PERCENTAGE_KEY), is(false));
        assertThat(m.containsKey(EXECUTOR_STATE_KEY), is(false));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));
    }
}
