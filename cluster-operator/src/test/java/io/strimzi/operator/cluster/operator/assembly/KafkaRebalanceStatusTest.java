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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class KafkaRebalanceStatusTest {


    public static JsonObject buildOptimizationProposal() {

        JsonObject proposal = new JsonObject();

        JsonObject summary = new JsonObject();

        JsonObject brokersBeforeObject = new JsonObject();
        JsonArray brokerLoadBeforeArray = new JsonArray();
        JsonObject brokerOneBefore = new JsonObject();
        brokerOneBefore.put(CruiseControlRebalanceKeys.BROKER_ID.getKey(), 1);
        brokerOneBefore.put(CruiseControlLoadParameters.CPU_PERCENTAGE.getCruiseControlKey(), 10.0);
        brokerOneBefore.put(CruiseControlLoadParameters.REPLICAS.getCruiseControlKey(), 10);
        brokerLoadBeforeArray.add(brokerOneBefore);
        brokersBeforeObject.put(CruiseControlRebalanceKeys.BROKERS.getKey(), brokerLoadBeforeArray);

        JsonObject brokersAfterObject = new JsonObject();
        JsonArray brokerLoadAfterArray = new JsonArray();
        JsonObject brokerOneAfter = new JsonObject();
        brokerOneAfter.put(CruiseControlRebalanceKeys.BROKER_ID.getKey(), 1);
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

        assertTrue(output.containsKey(1));
        assertTrue(output.get(1).containsKey(CruiseControlLoadParameters.CPU_PERCENTAGE.getStrimziKey()));
        assertTrue(output.get(1).containsKey(CruiseControlLoadParameters.REPLICAS.getStrimziKey()));

    }

    @Test
    public void testCreateLoadMap() {

        JsonObject proposal = buildOptimizationProposal();

        JsonArray loadBeforeArray = proposal.getJsonObject(CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey())
                .getJsonArray(CruiseControlRebalanceKeys.BROKERS.getKey());
        JsonArray loadAfterArray = proposal.getJsonObject(CruiseControlRebalanceKeys.LOAD_AFTER_OPTIMIZATION.getKey())
                .getJsonArray(CruiseControlRebalanceKeys.BROKERS.getKey());

        Map<Integer, Map<String, Object>> output = KafkaRebalanceAssemblyOperator.createBeforeAfterLoadMap(
                loadBeforeArray, loadAfterArray);

        assertTrue(output.containsKey(1));
        double[] cpu = (double[]) output.get(1).get(CruiseControlLoadParameters.CPU_PERCENTAGE.getStrimziKey());
        assertEquals(cpu[0], 10.0);
        assertEquals(cpu[1], 20.0);
        assertEquals(cpu[2], 10.0);
        int[] replicas = (int[]) output.get(1).get(CruiseControlLoadParameters.REPLICAS.getStrimziKey());
        assertEquals(replicas[0], 10);
        assertEquals(replicas[1], 5);
        assertEquals(-5, replicas[2]);

    }

    @Test
    public void testProcessProposal() {

        JsonObject proposal = buildOptimizationProposal();

        Map<String, Object> output = KafkaRebalanceAssemblyOperator.processOptimizationProposal(proposal);

        assertTrue(output.containsKey(CruiseControlRebalanceKeys.SUMMARY.getKey()));
        assertTrue(output.containsKey(KafkaRebalanceAssemblyOperator.BROKER_LOAD_KEY));

        Map<Integer, Map<String, Object>> brokerMap = (Map<Integer, Map<String, Object>>) output.get(KafkaRebalanceAssemblyOperator.BROKER_LOAD_KEY);

        double[] cpu = (double[]) brokerMap.get(1).get(CruiseControlLoadParameters.CPU_PERCENTAGE.getStrimziKey());
        assertEquals(cpu[0], 10.0);
        assertEquals(cpu[1], 20.0);
        assertEquals(cpu[2], 10.0);
        int[] replicas = (int[]) brokerMap.get(1).get(CruiseControlLoadParameters.REPLICAS.getStrimziKey());
        assertEquals(replicas[0], 10);
        assertEquals(replicas[1], 5);
        assertEquals(-5, replicas[2]);
    }
}
