/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;

import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceAssemblyOperator.BROKER_LOAD_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.BYTE_MOVEMENT_COMPLETED;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.COMPLETED_BYTE_MOVEMENT_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.ESTIMATED_TIME_TO_COMPLETION_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.EXECUTOR_STATE_KEY;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.TIME_COMPLETED;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceConfigMapUtils.updateRebalanceConfigMapWithProgressFields;
import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceStatusTest.buildOptimizationProposal;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaRebalanceConfigMapUtilsTest {

    private static final String BROKER_LOAD = buildOptimizationProposal().toString();
    private static final Map<String, String> BROKER_LOAD_MAP = Map.of(BROKER_LOAD_KEY, buildOptimizationProposal().toString());

    private static ObjectNode createExecutorState(Map<String, String> executorState) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        if (executorState != null) {
            for (Map.Entry<String, String> entry : executorState.entrySet()) {
                objectNode.put(entry.getKey(), entry.getValue());
            }
        }
        return objectNode;
    }

    private static ConfigMap createKafkaRebalanceConfigMap(Map<String, String> brokerLoad) {
        return new ConfigMapBuilder()
                .addToData(brokerLoad)
                .build();
    }

    private static String triggeredTaskTime(int secondsAgo) {
        return Instant.ofEpochSecond(Instant.now().getEpochSecond() - secondsAgo).toString();
    }

    @Test
    public void testProgressFieldsForProposalReadyState() throws Exception {
        JsonNode es = createExecutorState(null);
        ConfigMap cm = createKafkaRebalanceConfigMap(BROKER_LOAD_MAP);
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.ProposalReady, null, es, cm);

        Map<String, String> m =  cm.getData();
        assertThat(m.containsKey(ESTIMATED_TIME_TO_COMPLETION_KEY), is(false));
        assertThat(m.get(COMPLETED_BYTE_MOVEMENT_KEY), is("0"));
        assertThat(m.containsKey(EXECUTOR_STATE_KEY), is(false));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));
    }

    @Test
    public void testProgressFieldsForProposalRebalancingState() throws Exception {
        ZonedDateTime taskStartTime = ZonedDateTime.now().minusSeconds(2);
        JsonNode es0 = createExecutorState(Map.of("finishedDataMovement",  "250",
                "totalDataToMove", "10000"));
        ConfigMap cm = createKafkaRebalanceConfigMap(BROKER_LOAD_MAP);
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.Rebalancing, taskStartTime, es0, cm);

        Map<String, String> m =  cm.getData();
        assertThat(m.get(ESTIMATED_TIME_TO_COMPLETION_KEY), is("1"));
        assertThat(m.get(COMPLETED_BYTE_MOVEMENT_KEY), is("2"));
        assertThat(m.get(EXECUTOR_STATE_KEY), is(es0.toString()));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));
    }

    @Test
    public void testProgressFieldsForProposalStopped() throws Exception {
        ZonedDateTime taskStartTime = ZonedDateTime.now().minusSeconds(1);
        ObjectNode es = createExecutorState(Map.of("finishedDataMovement",  "250",
                "totalDataToMove", "10000"));
        ConfigMap cm = createKafkaRebalanceConfigMap(Map.of(ESTIMATED_TIME_TO_COMPLETION_KEY, "5",
                COMPLETED_BYTE_MOVEMENT_KEY, "100",
                EXECUTOR_STATE_KEY, es.toString(),
                BROKER_LOAD_KEY, BROKER_LOAD));
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.Stopped, taskStartTime, es, cm);

        Map<String, String> m =  cm.getData();
        assertThat(m.containsKey(ESTIMATED_TIME_TO_COMPLETION_KEY), is(false));
        assertThat(m.get(COMPLETED_BYTE_MOVEMENT_KEY), is("100"));
        assertThat(m.get(EXECUTOR_STATE_KEY), is(es.toString()));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));

        cm = createKafkaRebalanceConfigMap(Map.of(ESTIMATED_TIME_TO_COMPLETION_KEY, "5",
                BROKER_LOAD_KEY, BROKER_LOAD));
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.Stopped, null, es, cm);

        m =  cm.getData();
        assertThat(m.containsKey(ESTIMATED_TIME_TO_COMPLETION_KEY), is(false));
        assertThat(m.containsKey(COMPLETED_BYTE_MOVEMENT_KEY), is(false));
        assertThat(m.containsKey(EXECUTOR_STATE_KEY), is(false));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));
    }

    @Test
    public void testProgressFieldsForProposalNotReady() throws Exception {
        ObjectNode es = createExecutorState(Map.of("finishedDataMovement",  "250",
                "totalDataToMove", "10000"));
        ConfigMap cm = createKafkaRebalanceConfigMap(Map.of(ESTIMATED_TIME_TO_COMPLETION_KEY, "5",
                COMPLETED_BYTE_MOVEMENT_KEY, "100",
                EXECUTOR_STATE_KEY, es.toString(),
                BROKER_LOAD_KEY, BROKER_LOAD));
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.NotReady, null, es, cm);

        Map<String, String> m =  cm.getData();
        assertThat(m.containsKey(ESTIMATED_TIME_TO_COMPLETION_KEY), is(false));
        assertThat(m.get(COMPLETED_BYTE_MOVEMENT_KEY), is("100"));
        assertThat(m.get(EXECUTOR_STATE_KEY), is(es.toString()));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));

        cm = createKafkaRebalanceConfigMap(Map.of(ESTIMATED_TIME_TO_COMPLETION_KEY, "5",
                BROKER_LOAD_KEY, BROKER_LOAD));
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.NotReady, null, es, cm);

        m =  cm.getData();
        assertThat(m.containsKey(ESTIMATED_TIME_TO_COMPLETION_KEY), is(false));
        assertThat(m.containsKey(COMPLETED_BYTE_MOVEMENT_KEY), is(false));
        assertThat(m.containsKey(EXECUTOR_STATE_KEY), is(false));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));
    }

    @Test
    public void testProgressFieldsForReadyState() throws Exception {
        JsonNode es = createExecutorState(null);
        ConfigMap cm = createKafkaRebalanceConfigMap(BROKER_LOAD_MAP);
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.Ready, null, es, cm);

        Map<String, String> m =  cm.getData();
        assertThat(m.get(ESTIMATED_TIME_TO_COMPLETION_KEY), is(TIME_COMPLETED));
        assertThat(m.get(COMPLETED_BYTE_MOVEMENT_KEY), is(BYTE_MOVEMENT_COMPLETED));
        assertThat(m.containsKey(EXECUTOR_STATE_KEY), is(false));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));
    }

    @Test
    public void testProgressFieldsForUnsupportedStates() throws Exception {
        JsonNode es = createExecutorState(null);
        ConfigMap cm = createKafkaRebalanceConfigMap(BROKER_LOAD_MAP);
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.New, null, es, cm);

        Map<String, String> m =  cm.getData();
        assertThat(m.containsKey(ESTIMATED_TIME_TO_COMPLETION_KEY), is(false));
        assertThat(m.containsKey(COMPLETED_BYTE_MOVEMENT_KEY), is(false));
        assertThat(m.containsKey(EXECUTOR_STATE_KEY), is(false));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));

        es = createExecutorState(null);
        cm =  createKafkaRebalanceConfigMap(BROKER_LOAD_MAP);
        updateRebalanceConfigMapWithProgressFields(KafkaRebalanceState.PendingProposal, null, es, cm);

        m =  cm.getData();
        assertThat(m.containsKey(ESTIMATED_TIME_TO_COMPLETION_KEY), is(false));
        assertThat(m.containsKey(COMPLETED_BYTE_MOVEMENT_KEY), is(false));
        assertThat(m.containsKey(EXECUTOR_STATE_KEY), is(false));
        assertThat(m.get(BROKER_LOAD_KEY), is(BROKER_LOAD));
    }
}
