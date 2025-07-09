/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.kafka.KafkaSpec;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityOverride;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Uses information in a `Kafka` and `KafkaNodePool` custom resources to generate a capacity configuration file to
 * be used for Cruise Control's Broker Capacity File Resolver.
 *
 * For example, it takes a `Kafka` custom resource like the following:
 *
 * kind: Kafka
 * metadata:
 *   name: my-cluster
 *   annotations:
 *     strimzi.io/node-pools: enabled
 *     strimzi.io/kraft: enabled
 * spec:
 *   kafka:
 *     ...
 *   cruiseControl:
 *     brokerCapacity:
 *       cpu: "1"
 *       inboundNetwork: 10000KB/s
 *       outboundNetwork: 10000KB/s
 *       overrides:
 *         - brokers: [0]
 *           cpu: "2.345"
 *           outboundNetwork: 40000KB/s
 *         - brokers: [1, 2]
 *           cpu: 4000m
 *           inboundNetwork: 60000KB/s
 *           outboundNetwork: 20000KB/s
 *
 * and `KafkaNodePool` custom resources like the following:
 *
 * kind: KafkaNodePool
 * metadata:
 *   name: controller
 *   labels:
 *     strimzi.io/cluster: my-cluster
 * spec:
 *   replicas: 3
 *   roles:
 *     - controller
 *   storage:
 *     type: jbod
 *     volumes:
 *       - id: 0
 *         type: ephemeral
 *         kraftMetadata: shared
 * ---
 *
 * apiVersion: kafka.strimzi.io/v1beta2
 * kind: KafkaNodePool
 * metadata:
 *   name: broker
 *   labels:
 *     strimzi.io/cluster: my-cluster
 * spec:
 *   replicas: 3
 *   roles:
 *     - broker
 *   storage:
 *     type: jbod
 *     volumes:
 *       - id: 0
 *         type: persistent-claim
 *         size: 100Gi
 *         deleteClaim: false
 *       - id: 1
 *         type: persistent-claim
 *         size: 200Gi
 *         deleteClaim: false
 * ---
 *
 * Using this information, this class generates Cruise Control BrokerCapacityFileResolver config file like the following:
 *
 * {
 *   "brokerCapacities": [
 *     {
 *       "brokerId": "0",
 *       "capacity": {
 *         "DISK": {
 *           "/var/lib/kafka0/kafka-log0": "100000",
 *           "/var/lib/kafka1/kafka-log0": "200000"
 *         },
 *         "CPU": { "num.cores": "2.345" },
 *         "NW_IN": "10000",
 *         "NW_OUT": "40000"
 *       },
 *       "doc": "Capacity for Broker 0"
 *     },
 *     {
 *       "brokerId": "1",
 *       "capacity": {
 *         "DISK": {
 *           "/var/lib/kafka0/kafka-log1": "100000",
 *           "/var/lib/kafka1/kafka-log1": "200000"
 *         },
 *         "CPU": { "num.cores": "4" },
 *         "NW_IN": "60000",
 *         "NW_OUT": "20000"
 *       },
 *       "doc": "Capacity for Broker 1"
 *     },
 *     {
 *       "brokerId": "2",
 *       "capacity": {
 *         "DISK": {
 *           "/var/lib/kafka0/kafka-log2": "100000",
 *           "/var/lib/kafka1/kafka-log2": "200000"
 *         },
 *         "CPU": { "num.cores": "4" },
 *         "NW_IN": "60000",
 *         "NW_OUT": "20000"
 *       },
 *       "doc": "Capacity for Broker 2"
 *     }
 *   ]
 * }
 */
public class CapacityConfiguration {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CapacityConfiguration.class.getName());

    private final TreeMap<Integer, CapacityEntry> capacityEntries;

    /**
     * Constructor
     *
     * @param reconciliation        Reconciliation marker.
     * @param spec                  Spec of the Kafka custom resource.
     * @param kafkaBrokerNodes      List of the broker nodes which are part of the Kafka cluster.
     * @param kafkaStorage          A map with storage configuration used by the Kafka cluster and its node pools.
     * @param kafkaBrokerResources  A map with resource configuration used by the Kafka cluster and its broker pools.
     */
    public CapacityConfiguration(
            Reconciliation reconciliation,
            KafkaSpec spec,
            Set<NodeRef> kafkaBrokerNodes,
            Map<String, Storage> kafkaStorage,
            Map<String, ResourceRequirements> kafkaBrokerResources
    ) {
        this.capacityEntries = generateCapacityEntries(reconciliation, spec.getCruiseControl(), kafkaBrokerNodes,
                kafkaStorage, kafkaBrokerResources);
    }

    private static Map<Integer, BrokerCapacityOverride> processBrokerCapacityOverrides(Reconciliation reconciliation,
                                                                                       Set<NodeRef> kafkaBrokerNodes,
                                                                                       BrokerCapacity brokerCapacity) {
        Map<Integer, BrokerCapacityOverride> overrideMap = new HashMap<>();
        List<BrokerCapacityOverride> overrides = null;
        if (brokerCapacity != null) {
            overrides = brokerCapacity.getOverrides();
        }
        if (overrides != null) {
            if (overrides.isEmpty()) {
                LOGGER.warnCr(reconciliation, "Ignoring empty overrides list");
            } else {
                for (BrokerCapacityOverride override : overrides) {
                    List<Integer> ids = override.getBrokers();
                    for (int id : ids) {
                        if (overrideMap.containsKey(id)) {
                            LOGGER.warnCr(reconciliation, "Duplicate broker id {} found in overrides, using first occurrence.", id);
                        } else if (kafkaBrokerNodes.stream().noneMatch(node -> node.nodeId() == id)) {
                            LOGGER.warnCr(reconciliation, "Ignoring broker capacity override for unknown node ID {}", id);
                        } else {
                            overrideMap.put(id, override);
                        }
                    }
                }
            }
        }
        return overrideMap;
    }

    private static TreeMap<Integer, CapacityEntry> generateCapacityEntries(Reconciliation reconciliation,
                                         CruiseControlSpec spec,
                                         Set<NodeRef> kafkaBrokerNodes,
                                         Map<String, Storage> kafkaStorage,
                                         Map<String, ResourceRequirements> kafkaBrokerResources) {
        TreeMap<Integer, CapacityEntry> capacityEntries = new TreeMap<>();
        BrokerCapacity generalBrokerCapacity = spec.getBrokerCapacity();
        Map<Integer, BrokerCapacityOverride> brokerCapacityOverrideMap = processBrokerCapacityOverrides(reconciliation,
                kafkaBrokerNodes, generalBrokerCapacity);

        for (NodeRef node : kafkaBrokerNodes) {
            BrokerCapacityOverride brokerCapacityOverride = brokerCapacityOverrideMap.get(node.nodeId());

            DiskCapacity disk = new DiskCapacity(kafkaStorage.get(node.poolName()), node.nodeId());
            CpuCapacity cpu = new CpuCapacity(generalBrokerCapacity, brokerCapacityOverride, kafkaBrokerResources.get(node.poolName()));
            InboundNetworkCapacity inboundNetwork = new InboundNetworkCapacity(generalBrokerCapacity, brokerCapacityOverride);
            OutboundNetworkCapacity outboundNetwork = new OutboundNetworkCapacity(generalBrokerCapacity, brokerCapacityOverride);

            CapacityEntry capacityEntry = new CapacityEntry(node.nodeId(), disk, cpu, inboundNetwork, outboundNetwork);
            capacityEntries.put(node.nodeId(), capacityEntry);
        }
        return capacityEntries;
    }

    /**
     * Generate a capacity configuration for cluster.
     *
     * @return Cruise Control capacity configuration as a formatted JSON String.
     */
    public String toJson() {
        JsonArray capacityList = new JsonArray();
        for (CapacityEntry capacityEntry : capacityEntries.values()) {

            JsonObject capacityEntryJson = new JsonObject()
                    .put("brokerId", capacityEntry.id)
                    .put("capacity", new JsonObject()
                            .put("DISK", capacityEntry.disk.getJson())
                            .put("CPU", capacityEntry.cpu.getJson())
                            .put("NW_IN", capacityEntry.inboundNetwork.getJson())
                            .put("NW_OUT", capacityEntry.outboundNetwork.getJson()))
                    .put("doc", "Capacity for Broker " + capacityEntry.id);

            capacityList.add(capacityEntryJson);
        }

        return new JsonObject().put("brokerCapacities", capacityList).encodePrettily();
    }

    /**
     * Represents a Cruise Control capacity entry configuration for a Kafka broker.
     *
     * @param id  The broker ID.
     */
    private record CapacityEntry(
            int id,
            DiskCapacity disk,
            CpuCapacity cpu,
            InboundNetworkCapacity inboundNetwork,
            OutboundNetworkCapacity outboundNetwork
    ) { }
}
