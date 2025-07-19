/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityOverride;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityOverrideBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpecBuilder;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlGoals;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.strimzi.operator.cluster.model.cruisecontrol.CruiseControlConfiguration.filterResourceGoalsWithoutCapacityConfig;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlGoals.CPU_CAPACITY_GOAL;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlGoals.CPU_USAGE_DISTRIBUTION_GOAL;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlGoals.LEADER_BYTES_IN_DISTRIBUTION_GOAL;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlGoals.NETWORK_INBOUND_CAPACITY_GOAL;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlGoals.NETWORK_INBOUND_USAGE_DISTRIBUTION_GOAL;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlGoals.NETWORK_OUTBOUND_CAPACITY_GOAL;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlGoals.NETWORK_OUTBOUND_USAGE_DISTRIBUTION_GOAL;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlGoals.POTENTIAL_NETWORK_OUTAGE_GOAL;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class CruiseControlConfigurationTest {
    private static final String DEFAULT_CPU_CAPACITY = "1000m";
    private static final String DEFAULT_NETWORK_CAPACITY = "10000KiB/s";
    private static final Set<NodeRef> NODES = Set.of(
            new NodeRef("my-cluster-brokers-0", 0, "brokers", false, true),
            new NodeRef("my-cluster-brokers-1", 1, "brokers", false, true),
            new NodeRef("my-cluster-brokers-2", 2, "brokers", false, true)
    );
    private static final Map<String, Storage> STORAGE = Map.of("brokers", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build());
    private static final List<String> GOAL_LIST = List.of(
            NETWORK_INBOUND_CAPACITY_GOAL.toString(),
            NETWORK_OUTBOUND_CAPACITY_GOAL.toString(),
            CPU_CAPACITY_GOAL.toString(),
            POTENTIAL_NETWORK_OUTAGE_GOAL.toString(),
            NETWORK_INBOUND_USAGE_DISTRIBUTION_GOAL.toString(),
            NETWORK_OUTBOUND_USAGE_DISTRIBUTION_GOAL.toString(),
            CPU_USAGE_DISTRIBUTION_GOAL.toString(),
            LEADER_BYTES_IN_DISTRIBUTION_GOAL.toString());

    private static CruiseControlSpec createCcSpec(BrokerCapacity brokerCapacity) {
        return new CruiseControlSpecBuilder()
                .withBrokerCapacity(brokerCapacity)
                .build();
    }

    private static BrokerCapacity createBrokerCapacity(String cpuCapacity,
                                                       String inboundNetworkCapacity,
                                                       String outboundNetworkCapacity) {
        return new BrokerCapacityBuilder()
                    .withCpu(cpuCapacity)
                    .withInboundNetwork(inboundNetworkCapacity)
                    .withOutboundNetwork(outboundNetworkCapacity)
                .build();
    }

    private static BrokerCapacityOverride createBrokerCapacityOverride(List<Integer> brokerIds,
                                                                       String cpuCapacity,
                                                                       String inboundNetworkCapacity,
                                                                       String outboundNetworkCapacity) {
        return new BrokerCapacityOverrideBuilder()
                .addAllToBrokers(brokerIds)
                .withCpu(cpuCapacity)
                .withInboundNetwork(inboundNetworkCapacity)
                .withOutboundNetwork(outboundNetworkCapacity)
                .build();
    }

    private static ResourceRequirements createCpuResourceRequirement(String request, String limit) {
        return new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity(request))
                .addToLimits("cpu", new Quantity(limit))
                .build();
    }

    private void assertGoalsPresent(List<String> actual, CruiseControlGoals... expected) {
        for (CruiseControlGoals goal : expected) {
            assertThat("Expected goal to be present: " + goal, actual.contains(goal.toString()), is(true));
        }
    }

    private void assertGoalsAbsent(List<String> actual, CruiseControlGoals... expected) {
        for (CruiseControlGoals goal : expected) {
            assertThat("Expected goal to be absent: " + goal, actual.contains(goal.toString()), is(false));
        }
    }

    @Test
    public void shouldNotFilterGoalsWhenAllCapacitiesAreConfigured() {
        BrokerCapacity bc = createBrokerCapacity(DEFAULT_CPU_CAPACITY, DEFAULT_NETWORK_CAPACITY, DEFAULT_NETWORK_CAPACITY);
        Map<String, ResourceRequirements> kafkaBrokerResources = Map.of("brokers", new ResourceRequirementsBuilder().build());
        CapacityConfiguration capacityConfiguration = new CapacityConfiguration(null, createCcSpec(bc), NODES, STORAGE, kafkaBrokerResources);

        List<String> filteredGoals = filterResourceGoalsWithoutCapacityConfig(GOAL_LIST, capacityConfiguration);
        assertThat(filteredGoals, containsInAnyOrder(GOAL_LIST.toArray()));
    }

    @Test
    public void shouldFilterInboundNetworkGoalsWhenDefaultCapacityIsMissing() {
        BrokerCapacity bc = createBrokerCapacity(DEFAULT_CPU_CAPACITY, null, DEFAULT_NETWORK_CAPACITY);
        Map<String, ResourceRequirements> kafkaBrokerResources = Map.of("brokers", createCpuResourceRequirement(DEFAULT_CPU_CAPACITY, DEFAULT_CPU_CAPACITY));
        CapacityConfiguration cc = new CapacityConfiguration(null, createCcSpec(bc), NODES, STORAGE, kafkaBrokerResources);

        List<String> filteredGoals = filterResourceGoalsWithoutCapacityConfig(GOAL_LIST, cc);
        assertGoalsAbsent(filteredGoals, NETWORK_INBOUND_USAGE_DISTRIBUTION_GOAL, NETWORK_INBOUND_CAPACITY_GOAL, LEADER_BYTES_IN_DISTRIBUTION_GOAL);
    }

    @Test
    public void shouldFilterLeaderBytesInGoalWhenOverridesAreHeterogeneous() {
        BrokerCapacity bc = createBrokerCapacity(DEFAULT_CPU_CAPACITY, DEFAULT_NETWORK_CAPACITY, DEFAULT_NETWORK_CAPACITY);
        BrokerCapacityOverride bco0 = createBrokerCapacityOverride(List.of(0), DEFAULT_CPU_CAPACITY, DEFAULT_NETWORK_CAPACITY, DEFAULT_NETWORK_CAPACITY);
        BrokerCapacityOverride bco1 = createBrokerCapacityOverride(List.of(1), DEFAULT_CPU_CAPACITY, "5000KiB/s", DEFAULT_NETWORK_CAPACITY);
        bc.setOverrides(List.of(bco0, bco1));
        Map<String, ResourceRequirements> kafkaBrokerResources = Map.of("brokers", createCpuResourceRequirement(DEFAULT_CPU_CAPACITY, DEFAULT_CPU_CAPACITY));
        CapacityConfiguration cc = new CapacityConfiguration(null, createCcSpec(bc), NODES, STORAGE, kafkaBrokerResources);

        List<String> filteredGoals = filterResourceGoalsWithoutCapacityConfig(GOAL_LIST, cc);
        assertGoalsAbsent(filteredGoals, LEADER_BYTES_IN_DISTRIBUTION_GOAL);
    }

    @Test
    public void shouldNotFilterInboundNetworkGoalsWhenOverridesAreHomogeneous() {
        BrokerCapacity bc = createBrokerCapacity(DEFAULT_CPU_CAPACITY, DEFAULT_NETWORK_CAPACITY, DEFAULT_NETWORK_CAPACITY);
        BrokerCapacityOverride bco = createBrokerCapacityOverride(List.of(0, 1, 2), DEFAULT_CPU_CAPACITY, DEFAULT_NETWORK_CAPACITY, DEFAULT_NETWORK_CAPACITY);
        bc.setOverrides(List.of(bco));
        Map<String, ResourceRequirements> kafkaBrokerResources = Map.of("brokers", createCpuResourceRequirement(DEFAULT_CPU_CAPACITY, DEFAULT_CPU_CAPACITY));
        CapacityConfiguration cc = new CapacityConfiguration(null, createCcSpec(bc), NODES, STORAGE, kafkaBrokerResources);

        List<String> filteredGoals = filterResourceGoalsWithoutCapacityConfig(GOAL_LIST, cc);
        assertGoalsPresent(filteredGoals, LEADER_BYTES_IN_DISTRIBUTION_GOAL);
    }

    @Test
    public void shouldFilterOutboundNetworkGoalsWhenDefaultCapacityIsMissing() {
        BrokerCapacity bc = createBrokerCapacity(DEFAULT_CPU_CAPACITY, DEFAULT_NETWORK_CAPACITY, null);
        Map<String, ResourceRequirements> kafkaBrokerResources = Map.of("brokers", createCpuResourceRequirement(DEFAULT_CPU_CAPACITY, DEFAULT_CPU_CAPACITY));
        CapacityConfiguration cc = new CapacityConfiguration(null, createCcSpec(bc), NODES, STORAGE, kafkaBrokerResources);

        List<String> filteredGoals = filterResourceGoalsWithoutCapacityConfig(GOAL_LIST, cc);
        assertGoalsAbsent(filteredGoals, NETWORK_OUTBOUND_USAGE_DISTRIBUTION_GOAL, NETWORK_OUTBOUND_CAPACITY_GOAL, POTENTIAL_NETWORK_OUTAGE_GOAL);
    }

    @Test
    public void shouldNotFilterOutboundNetworkGoalsWhenOverridesAreDefined() {
        BrokerCapacity bc = createBrokerCapacity(DEFAULT_CPU_CAPACITY, DEFAULT_NETWORK_CAPACITY, DEFAULT_NETWORK_CAPACITY);
        BrokerCapacityOverride bco = createBrokerCapacityOverride(List.of(0, 1, 2), DEFAULT_CPU_CAPACITY, DEFAULT_NETWORK_CAPACITY, DEFAULT_NETWORK_CAPACITY);
        bc.setOverrides(List.of(bco));
        Map<String, ResourceRequirements> kafkaBrokerResources = Map.of("brokers", createCpuResourceRequirement(DEFAULT_CPU_CAPACITY, DEFAULT_CPU_CAPACITY));
        CapacityConfiguration cc = new CapacityConfiguration(null, createCcSpec(bc), NODES, STORAGE, kafkaBrokerResources);

        List<String> filteredGoals = filterResourceGoalsWithoutCapacityConfig(GOAL_LIST, cc);
        assertGoalsPresent(filteredGoals, NETWORK_OUTBOUND_USAGE_DISTRIBUTION_GOAL, NETWORK_OUTBOUND_CAPACITY_GOAL, POTENTIAL_NETWORK_OUTAGE_GOAL);
    }
}
