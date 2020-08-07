/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlLoadParameters;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlRebalanceKeys;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class KafkaRebalanceStatusTest {

    private static final int BROKER_ONE_KEY = 1;


    public static JsonObject buildOptimizationProposal() {

        JsonObject proposal = new JsonObject();

        JsonObject summary = new JsonObject();

        JsonObject brokersBeforeObject = new JsonObject();
        JsonArray brokerLoadBeforeArray = new JsonArray();
        JsonObject brokerOneBefore = new JsonObject();
        brokerOneBefore.put(CruiseControlRebalanceKeys.BROKER_ID.getKey(), BROKER_ONE_KEY);
        brokerOneBefore.put(CruiseControlLoadParameters.CPU_PERCENTAGE.getCruiseControlKey(), 10.0);
        brokerOneBefore.put(CruiseControlLoadParameters.REPLICAS.getCruiseControlKey(), 10);
        brokerLoadBeforeArray.add(brokerOneBefore);
        brokersBeforeObject.put(CruiseControlRebalanceKeys.BROKERS.getKey(), brokerLoadBeforeArray);

        JsonObject brokersAfterObject = new JsonObject();
        JsonArray brokerLoadAfterArray = new JsonArray();
        JsonObject brokerOneAfter = new JsonObject();
        brokerOneAfter.put(CruiseControlRebalanceKeys.BROKER_ID.getKey(), BROKER_ONE_KEY);
        brokerOneAfter.put(CruiseControlLoadParameters.CPU_PERCENTAGE.getCruiseControlKey(), 20.0);
        brokerOneAfter.put(CruiseControlLoadParameters.REPLICAS.getCruiseControlKey(), 5);
        brokerLoadAfterArray.add(brokerOneAfter);
        brokersAfterObject.put(CruiseControlRebalanceKeys.BROKERS.getKey(), brokerLoadAfterArray);

        proposal.put(CruiseControlRebalanceKeys.SUMMARY.getKey(), summary);
        proposal.put(CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey(), brokersBeforeObject);
        proposal.put(CruiseControlRebalanceKeys.LOAD_AFTER_OPTIMIZATION.getKey(), brokersAfterObject);

        return proposal;

    }

    @Test
    public void testLoadParamExtract() {

        JsonObject proposal = buildOptimizationProposal();

        JsonArray loadBeforeArray = proposal.getJsonObject(CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey())
                .getJsonArray(CruiseControlRebalanceKeys.BROKERS.getKey());

        Map<Integer, Map<String, Object>> output = KafkaRebalanceAssemblyOperator.extractLoadParameters(loadBeforeArray);

        assertThat(output, hasKey(BROKER_ONE_KEY));
        assertThat(output.get(BROKER_ONE_KEY), hasEntry(CruiseControlLoadParameters.CPU_PERCENTAGE.getKafkaRebalanceStatusKey(), 10.0));
        assertThat(output.get(BROKER_ONE_KEY), hasEntry(CruiseControlLoadParameters.REPLICAS.getKafkaRebalanceStatusKey(), 10));

    }

    @Test
    public void testCreateLoadMap() {

        JsonObject proposal = buildOptimizationProposal();

        JsonArray loadBeforeArray = proposal.getJsonObject(CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey())
                .getJsonArray(CruiseControlRebalanceKeys.BROKERS.getKey());
        JsonArray loadAfterArray = proposal.getJsonObject(CruiseControlRebalanceKeys.LOAD_AFTER_OPTIMIZATION.getKey())
                .getJsonArray(CruiseControlRebalanceKeys.BROKERS.getKey());

        Map<Integer, Map<String, Object>> output = KafkaRebalanceAssemblyOperator.parseLoadStats(
                loadBeforeArray, loadAfterArray);

        assertThat(output, hasKey(BROKER_ONE_KEY));
        assertThat(output.get(BROKER_ONE_KEY), hasKey(CruiseControlLoadParameters.CPU_PERCENTAGE.getKafkaRebalanceStatusKey()));
        double[] cpu = (double[]) output.get(BROKER_ONE_KEY).get(CruiseControlLoadParameters.CPU_PERCENTAGE.getKafkaRebalanceStatusKey());
        assertThat(cpu[0], is(10.0));
        assertThat(cpu[1], is(20.0));
        assertThat(cpu[2], is(10.0));

        assertThat(output.get(BROKER_ONE_KEY), hasKey(CruiseControlLoadParameters.REPLICAS.getKafkaRebalanceStatusKey()));
        int[] replicas = (int[]) output.get(BROKER_ONE_KEY).get(CruiseControlLoadParameters.REPLICAS.getKafkaRebalanceStatusKey());
        assertThat(replicas[0], is(10));
        assertThat(replicas[1], is(5));
        assertThat(replicas[2], is(-5));

    }

    @Test
    public void testProcessProposal() {

        JsonObject proposal = buildOptimizationProposal();

        Map<String, Object> output = KafkaRebalanceAssemblyOperator.processOptimizationProposal(proposal);

        assertTrue(output.containsKey(CruiseControlRebalanceKeys.SUMMARY.getKey()));
        assertTrue(output.containsKey(KafkaRebalanceAssemblyOperator.BROKER_LOAD_KEY));

        Map<Integer, Map<String, Object>> brokerMap = (Map<Integer, Map<String, Object>>) output.get(KafkaRebalanceAssemblyOperator.BROKER_LOAD_KEY);

        assertThat(brokerMap, hasKey(BROKER_ONE_KEY));
        assertThat(brokerMap.get(BROKER_ONE_KEY), hasKey(CruiseControlLoadParameters.CPU_PERCENTAGE.getKafkaRebalanceStatusKey()));
        double[] cpu = (double[]) brokerMap.get(BROKER_ONE_KEY).get(CruiseControlLoadParameters.CPU_PERCENTAGE.getKafkaRebalanceStatusKey());
        assertThat(cpu[0], is(10.0));
        assertThat(cpu[1], is(20.0));
        assertThat(cpu[2], is(10.0));

        assertThat(brokerMap.get(BROKER_ONE_KEY), hasKey(CruiseControlLoadParameters.REPLICAS.getKafkaRebalanceStatusKey()));
        int[] replicas = (int[]) brokerMap.get(BROKER_ONE_KEY).get(CruiseControlLoadParameters.REPLICAS.getKafkaRebalanceStatusKey());
        assertThat(replicas[0], is(10));
        assertThat(replicas[1], is(5));
        assertThat(replicas[2], is(-5));
    }
}
