/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceSpec;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceSpecBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlLoadParameters;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlRebalanceKeys;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRebalanceStatusTest {

    private static final int BROKER_ONE_KEY = 1;
    private static final String RESOURCE_NAME = "my-rebalance";
    private static final String CLUSTER_NAMESPACE = "cruise-control-namespace";
    private static final String CLUSTER_NAME = "kafka-cruise-control-test-cluster";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


    private KafkaRebalance createKafkaRebalance(String namespace, String clusterName, String resourceName,
                                                KafkaRebalanceSpec kafkaRebalanceSpec) {
        return new KafkaRebalanceBuilder()
                .withNewMetadata()
                .withNamespace(namespace)
                .withName(resourceName)
                .withLabels(clusterName != null ? Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME) : null)
                .endMetadata()
                .withSpec(kafkaRebalanceSpec)
                .build();
    }

    public static JsonNode buildOptimizationProposal() {
        ObjectNode proposal = OBJECT_MAPPER.createObjectNode();

        ObjectNode summary = OBJECT_MAPPER.createObjectNode();

        ObjectNode brokersBeforeObject = OBJECT_MAPPER.createObjectNode();
        ArrayNode brokerLoadBeforeArray = OBJECT_MAPPER.createArrayNode();

        ObjectNode brokerOneBefore = OBJECT_MAPPER.createObjectNode();
        brokerOneBefore.put(CruiseControlRebalanceKeys.BROKER_ID.getKey(), BROKER_ONE_KEY);
        brokerOneBefore.put(CruiseControlLoadParameters.CPU_PERCENTAGE.getCruiseControlKey(), 10.0);
        brokerOneBefore.put(CruiseControlLoadParameters.REPLICAS.getCruiseControlKey(), 10);

        brokerLoadBeforeArray.add(brokerOneBefore);
        brokersBeforeObject.set(CruiseControlRebalanceKeys.BROKERS.getKey(), brokerLoadBeforeArray);

        ObjectNode brokersAfterObject = OBJECT_MAPPER.createObjectNode();
        ArrayNode brokerLoadAfterArray = OBJECT_MAPPER.createArrayNode();

        ObjectNode brokerOneAfter = OBJECT_MAPPER.createObjectNode();
        brokerOneAfter.put(CruiseControlRebalanceKeys.BROKER_ID.getKey(), BROKER_ONE_KEY);
        brokerOneAfter.put(CruiseControlLoadParameters.CPU_PERCENTAGE.getCruiseControlKey(), 20.0);
        brokerOneAfter.put(CruiseControlLoadParameters.REPLICAS.getCruiseControlKey(), 5);

        brokerLoadAfterArray.add(brokerOneAfter);
        brokersAfterObject.set(CruiseControlRebalanceKeys.BROKERS.getKey(), brokerLoadAfterArray);

        proposal.set(CruiseControlRebalanceKeys.SUMMARY.getKey(), summary);
        proposal.set(CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey(), brokersBeforeObject);
        proposal.set(CruiseControlRebalanceKeys.LOAD_AFTER_OPTIMIZATION.getKey(), brokersAfterObject);

        return proposal;

    }

    @Test
    public void testLoadParamExtract() {

        JsonNode proposal = buildOptimizationProposal();

        ArrayNode loadBeforeArray = (ArrayNode) proposal.get(CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey())
                .get(CruiseControlRebalanceKeys.BROKERS.getKey());

        Map<Integer, Map<String, Object>> output = KafkaRebalanceAssemblyOperator.extractLoadParameters(loadBeforeArray);

        assertThat(output, hasKey(BROKER_ONE_KEY));
        assertThat(output.get(BROKER_ONE_KEY), hasEntry(CruiseControlLoadParameters.CPU_PERCENTAGE.getKafkaRebalanceStatusKey(), 10.0));
        assertThat(output.get(BROKER_ONE_KEY), hasEntry(CruiseControlLoadParameters.REPLICAS.getKafkaRebalanceStatusKey(), 10));

    }

    @Test
    public void testCreateLoadMap() {

        JsonNode proposal = buildOptimizationProposal();

        ArrayNode loadBeforeArray = (ArrayNode) proposal.get(CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey())
                .get(CruiseControlRebalanceKeys.BROKERS.getKey());
        ArrayNode loadAfterArray = (ArrayNode) proposal.get(CruiseControlRebalanceKeys.LOAD_AFTER_OPTIMIZATION.getKey())
                .get(CruiseControlRebalanceKeys.BROKERS.getKey());

        JsonNode output = KafkaRebalanceAssemblyOperator.parseLoadStats(
                loadBeforeArray, loadAfterArray);

        assertTrue(output.has("1"));
        assertTrue(output.get("1").has(CruiseControlLoadParameters.REPLICAS.getKafkaRebalanceStatusKey()));

        JsonNode replicas = output.get("1").get("replicas");

        assertThat(replicas.get("before").asInt(), is(10));
        assertThat(replicas.get("after").asInt(), is(5));
        assertThat(replicas.get("diff").asInt(), is(-5));

        assertTrue(output.get("1").has(CruiseControlLoadParameters.CPU_PERCENTAGE.getKafkaRebalanceStatusKey()));

        JsonNode cpus = output.get("1").get("cpuPercentage");

        assertThat(cpus.get("before").asDouble(), is(10.));
        assertThat(cpus.get("after").asDouble(), is(20.0));
        assertThat(cpus.get("diff").asDouble(), is(10.0));

    }

    @Test
    public void testProcessProposal() {

        JsonNode proposal = buildOptimizationProposal();

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        KafkaRebalanceAssemblyOperator.MapAndStatus<ConfigMap, Map<String, Object>> output = KafkaRebalanceAssemblyOperator.processOptimizationProposal(kr, proposal);

        Map<String, String> brokerMap = output.getLoadMap().getData();

        try {

            Map<String, LinkedHashMap<String, String>> brokerLoadMap = OBJECT_MAPPER.readValue(brokerMap.get(KafkaRebalanceAssemblyOperator.BROKER_LOAD_KEY), LinkedHashMap.class);

            assertThat(brokerMap, hasKey(KafkaRebalanceAssemblyOperator.BROKER_LOAD_KEY));

            LinkedHashMap<String, LinkedHashMap<String, Object>> m = (LinkedHashMap) brokerLoadMap.get("1");

            assertThat(m, hasKey(CruiseControlLoadParameters.CPU_PERCENTAGE.getKafkaRebalanceStatusKey()));

            assertThat((Double) m.get("cpuPercentage").get("before"), is(10.0));
            assertThat((Double) m.get("cpuPercentage").get("after"), is(20.0));
            assertThat((Double) m.get("cpuPercentage").get("diff"), is(10.0));

            assertThat(m, hasKey(CruiseControlLoadParameters.REPLICAS.getKafkaRebalanceStatusKey()));

            assertThat((Integer) m.get("replicas").get("before"), is(10));
            assertThat((Integer) m.get("replicas").get("after"), is(5));
            assertThat((Integer) m.get("replicas").get("diff"), is(-5));

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
