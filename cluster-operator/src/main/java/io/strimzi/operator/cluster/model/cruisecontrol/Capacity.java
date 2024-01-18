/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.KafkaSpec;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.SingleVolumeStorage;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityOverride;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.Quantities;
import io.strimzi.operator.cluster.model.StorageUtils;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Uses information in a Kafka Custom Resource to generate a capacity configuration file to be used for
 * Cruise Control's Broker Capacity File Resolver.
 *
 *
 * For example, it takes a Kafka Custom Resource like the following:
 *
 * spec:
 *  kafka:
 *     replicas: 3
 *     storage:
 *       type: jbod
 *       volumes:
 *       - id: 0
 *         type: persistent-claim
 *         size: 100Gi
 *         deleteClaim: false
 *       - id: 1
 *         type: persistent-claim
 *         size: 200Gi
 *         deleteClaim: false
 *  cruiseControl:
 *    brokerCapacity:
 *     cpu: "1"
 *     inboundNetwork: 10000KB/s
 *     outboundNetwork: 10000KB/s
 *     overrides:
 *       - brokers: [0]
 *         cpu: "2.345"
 *         outboundNetwork: 40000KB/s
 *       - brokers: [1, 2]
 *         cpu: 4000m
 *         inboundNetwork: 60000KB/s
 *         outboundNetwork: 20000KB/s
 *
 * and uses the information to create Cruise Control BrokerCapacityFileResolver config file like the following:
 *
 * {
 *   "brokerCapacities":[
 *     {
 *       "brokerId": "-1",
 *       "capacity": {
 *         "DISK": {
 *             "/var/lib/kafka0/kafka-log-1": "100000",
 *             "/var/lib/kafka1/kafka-log-1": "200000"
 *          },
 *         "CPU": {"num.cores": "1"},
 *         "NW_IN": "10000",
 *         "NW_OUT": "10000"
 *       },
 *       "doc": "This is the default capacity. Capacity unit used for disk is in MB, cpu is in percentage, network throughput is in KB."
 *     },
 *     {
 *       "brokerId": "0",
 *       "capacity": {
 *         "DISK": {
 *             "/var/lib/kafka0/kafka-log0": "100000",
 *             "/var/lib/kafka1/kafka-log0": "200000"
 *          },
 *         "CPU": {"num.cores": "2.345"},
 *         "NW_IN": "10000",
 *         "NW_OUT": "40000"
 *       },
 *       "doc": "Capacity for Broker 0"
 *     },
 *     {
 *       "brokerId": "1",
 *       "capacity": {
 *         "DISK": {
 *             "/var/lib/kafka0/kafka-log1": "100000",
 *             "/var/lib/kafka1/kafka-log1": "200000"
 *           },
 *         "CPU": {"num.cores": "4"},
 *         "NW_IN": "60000",
 *         "NW_OUT": "20000"
 *       },
 *       "doc": "Capacity for Broker 1"
 *     },
 *       "brokerId": "2",
 *       "capacity": {
 *         "DISK": {
 *             "/var/lib/kafka0/kafka-log2": "100000",
 *             "/var/lib/kafka1/kafka-log2": "200000"
 *           },
 *         "CPU": {"num.cores": "4"},
 *         "NW_IN": "60000",
 *         "NW_OUT": "20000"
 *       },
 *       "doc": "Capacity for Broker 2"
 *     }
 *   ]
 * }
 */
public class Capacity {
    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(Capacity.class.getName());
    private final Reconciliation reconciliation;
    private final TreeMap<Integer, BrokerCapacity> capacityEntries;

    /**
     * Broker capacities key
     */
    public static final String CAPACITIES_KEY = "brokerCapacities";

    /**
     * Capacity key
     */
    public static final String CAPACITY_KEY = "capacity";

    /**
     * Disk key
     */
    public static final String DISK_KEY = "DISK";

    /**
     * CPU key
     */
    public static final String CPU_KEY = "CPU";

    /**
     * Inbound network key
     */
    public static final String INBOUND_NETWORK_KEY = "NW_IN";

    /**
     * Outbound network key
     */
    public static final String OUTBOUND_NETWORK_KEY = "NW_OUT";

    /**
     * Resource type
     */
    public static final String RESOURCE_TYPE = "cpu";

    private static final String KAFKA_MOUNT_PATH = "/var/lib/kafka";
    private static final String KAFKA_LOG_DIR = "kafka-log";
    private static final String BROKER_ID_KEY = "brokerId";
    private static final String DOC_KEY = "doc";

    private enum ResourceRequirementType {
        REQUEST,
        LIMIT;

        private Quantity getQuantity(ResourceRequirements resources) {
            Map<String, Quantity> resourceRequirement;
            switch (this) {
                case REQUEST:
                    resourceRequirement = resources.getRequests();
                    break;
                case LIMIT:
                    resourceRequirement = resources.getLimits();
                    break;
                default:
                    resourceRequirement = null;
            }
            if (resourceRequirement != null) {
                return resourceRequirement.get(RESOURCE_TYPE);
            }
            return null;
        }
    }

    /**
     * Constructor
     *
     * @param reconciliation        Reconciliation marker
     * @param spec                  Spec of the Kafka custom resource
     * @param kafkaBrokerNodes      List of the broker nodes which are part of the Kafka cluster
     * @param kafkaStorage          A map with storage configuration used by the Kafka cluster and its node pools
     * @param kafkaBrokerResources  A map with resource configuration used by the Kafka cluster and its broker pools
     */
    public Capacity(
            Reconciliation reconciliation,
            KafkaSpec spec,
            Set<NodeRef> kafkaBrokerNodes,
            Map<String, Storage> kafkaStorage,
            Map<String, ResourceRequirements> kafkaBrokerResources
    ) {
        this.reconciliation = reconciliation;
        this.capacityEntries = new TreeMap<>();

        processCapacityEntries(spec.getCruiseControl(), kafkaBrokerNodes, kafkaStorage, kafkaBrokerResources);
    }

    private static Integer getResourceRequirement(ResourceRequirements resources, ResourceRequirementType requirementType) {
        if (resources != null) {
            Quantity quantity = requirementType.getQuantity(resources);
            if (quantity != null) {
                return Quantities.parseCpuAsMilliCpus(quantity.toString());
            }
        }
        return null;
    }

    private static CpuCapacity getCpuBasedOnRequirements(ResourceRequirements resourceRequirements) {
        Integer request = getResourceRequirement(resourceRequirements, ResourceRequirementType.REQUEST);
        Integer limit = getResourceRequirement(resourceRequirements, ResourceRequirementType.LIMIT);

        if (request != null) {
            return new CpuCapacity(CpuCapacity.milliCpuToCpu(request));
        } else if (limit != null) {
            return new CpuCapacity(CpuCapacity.milliCpuToCpu(limit));
        } else {
            return new CpuCapacity(BrokerCapacity.DEFAULT_CPU_CORE_CAPACITY);
        }
    }

    /**
     * The brokerCapacity overrides per broker take top precedence, then general brokerCapacity configuration, and then the Kafka resource requests, then the Kafka resource limits.
     * For example:
     *   (1) brokerCapacity overrides
     *   (2) brokerCapacity
     *   (3) Kafka resource requests
     *   (4) Kafka resource limits
     * When none of Cruise Control CPU capacity configurations mentioned above are configured, CPU capacity will be set to `1`.
     *
     * @param override             brokerCapacity overrides (per broker)
     * @param bc                   brokerCapacity (for all brokers)
     * @param resourceRequirements Kafka resource requests and limits (for all brokers)
     * @return A {@link CpuCapacity} object containing the specified capacity for a broker
     */
    private CpuCapacity processCpu(BrokerCapacityOverride override, io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity bc, ResourceRequirements resourceRequirements) {
        if (override != null && override.getCpu() != null) {
            return new CpuCapacity(override.getCpu());
        } else if (bc != null && bc.getCpu() != null) {
            return new CpuCapacity(bc.getCpu());
        }
        return getCpuBasedOnRequirements(resourceRequirements);
    }

    private static DiskCapacity processDisk(Storage storage, int brokerId) {
        if (storage instanceof JbodStorage) {
            return generateJbodDiskCapacity(storage, brokerId);
        } else {
            return generateDiskCapacity(storage);
        }
    }

    private static String processInboundNetwork(io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity bc, BrokerCapacityOverride override) {
        if (override != null && override.getInboundNetwork() != null) {
            return getThroughputInKiB(override.getInboundNetwork());
        } else if (bc != null && bc.getInboundNetwork() != null) {
            return getThroughputInKiB(bc.getInboundNetwork());
        } else {
            return BrokerCapacity.DEFAULT_INBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND;
        }
    }

    private static String processOutboundNetwork(io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity bc, BrokerCapacityOverride override) {
        if (override != null && override.getOutboundNetwork() != null) {
            return getThroughputInKiB(override.getOutboundNetwork());
        } else if (bc != null && bc.getOutboundNetwork() != null) {
            return getThroughputInKiB(bc.getOutboundNetwork());
        } else {
            return BrokerCapacity.DEFAULT_OUTBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND;
        }
    }

    /**
     * Generate JBOD disk capacity configuration for a broker using the supplied storage configuration
     *
     * @param storage Storage configuration for Kafka cluster
     * @param brokerId Id of the broker
     * @return Disk capacity configuration value for broker brokerId
     */
    private static DiskCapacity generateJbodDiskCapacity(Storage storage, int brokerId) {
        DiskCapacity disks = new DiskCapacity();
        String size = "";

        for (SingleVolumeStorage volume : ((JbodStorage) storage).getVolumes()) {
            String name = VolumeUtils.createVolumePrefix(volume.getId(), true);
            String path = KAFKA_MOUNT_PATH + "/" + name + "/" + KAFKA_LOG_DIR + brokerId;

            if (volume instanceof PersistentClaimStorage) {
                size = ((PersistentClaimStorage) volume).getSize();
            } else if (volume instanceof EphemeralStorage) {
                size = ((EphemeralStorage) volume).getSizeLimit();
            }
            disks.add(path, String.valueOf(getSizeInMiB(size)));
        }
        return disks;
    }

    /**
     * Generate total disk capacity using the supplied storage configuration
     *
     * @param storage Storage configuration for Kafka cluster
     * @return Disk capacity per broker
     */
    private static DiskCapacity generateDiskCapacity(Storage storage) {
        if (storage instanceof PersistentClaimStorage) {
            return DiskCapacity.of(getSizeInMiB(((PersistentClaimStorage) storage).getSize()));
        } else if (storage instanceof EphemeralStorage) {
            if (((EphemeralStorage) storage).getSizeLimit() != null) {
                return DiskCapacity.of(getSizeInMiB(((EphemeralStorage) storage).getSizeLimit()));
            } else {
                return DiskCapacity.of(BrokerCapacity.DEFAULT_DISK_CAPACITY_IN_MIB);
            }
        } else {
            throw new IllegalStateException("The declared storage '" + storage.getType() + "' is not supported");
        }
    }

    /*
     * Parse a K8S-style representation of a disk size, such as {@code 100Gi},
     * into the equivalent number of mebibytes represented as a String.
     *
     * @param size The String representation of the volume size.
     * @return The equivalent number of mebibytes.
     */
    private static String getSizeInMiB(String size) {
        if (size == null) {
            return BrokerCapacity.DEFAULT_DISK_CAPACITY_IN_MIB;
        }
        return String.valueOf(StorageUtils.convertTo(size, "Mi"));
    }

    /**
     * Parse Strimzi representation of throughput, such as {@code 10000KB/s},
     * into the equivalent number of kibibytes represented as a String.
     *
     * @param throughput The String representation of the throughput.
     * @return The equivalent number of kibibytes.
     */
    public static String getThroughputInKiB(String throughput) {
        String size = throughput.substring(0, throughput.indexOf("B"));
        return String.valueOf(StorageUtils.convertTo(size, "Ki"));
    }

    private void processCapacityEntries(CruiseControlSpec spec, Set<NodeRef> kafkaBrokerNodes, Map<String, Storage> kafkaStorage, Map<String, ResourceRequirements> kafkaBrokerResources) {
        io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity brokerCapacity = spec.getBrokerCapacity();

        String inboundNetwork = processInboundNetwork(brokerCapacity, null);
        String outboundNetwork = processOutboundNetwork(brokerCapacity, null);

        // We create a capacity for each broker node
        for (NodeRef node : kafkaBrokerNodes) {
            DiskCapacity disk = processDisk(kafkaStorage.get(node.poolName()), node.nodeId());
            CpuCapacity cpu = processCpu(null, brokerCapacity, kafkaBrokerResources.get(node.poolName()));

            BrokerCapacity broker = new BrokerCapacity(node.nodeId(), cpu, disk, inboundNetwork, outboundNetwork);
            capacityEntries.put(node.nodeId(), broker);

            if (brokerCapacity != null) {
                // For checking for duplicate brokerIds
                Set<Integer> overrideIds = new HashSet<>();
                List<BrokerCapacityOverride> overrides = brokerCapacity.getOverrides();
                // Override broker entries
                if (overrides != null) {
                    if (overrides.isEmpty()) {
                        LOGGER.warnCr(reconciliation, "Ignoring empty overrides list");
                    } else {
                        for (BrokerCapacityOverride override : overrides) {
                            List<Integer> ids = override.getBrokers();
                            inboundNetwork = processInboundNetwork(brokerCapacity, override);
                            outboundNetwork = processOutboundNetwork(brokerCapacity, override);
                            for (int id : ids) {
                                if (id == BrokerCapacity.DEFAULT_BROKER_ID) {
                                    LOGGER.warnCr(reconciliation, "Ignoring broker capacity override with illegal broker id -1.");
                                } else {
                                    if (capacityEntries.containsKey(id)) {
                                        if (overrideIds.add(id)) {
                                            BrokerCapacity brokerCapacityEntry = capacityEntries.get(id);
                                            brokerCapacityEntry.setCpu(processCpu(override, brokerCapacity, kafkaBrokerResources.get(Integer.toString(id))));
                                            brokerCapacityEntry.setInboundNetwork(inboundNetwork);
                                            brokerCapacityEntry.setOutboundNetwork(outboundNetwork);
                                        } else {
                                            LOGGER.warnCr(reconciliation, "Duplicate broker id {} found in overrides, using first occurrence.", id);
                                        }
                                    } else {
                                        LOGGER.warnCr(reconciliation, "Ignoring broker capacity override for unknown node ID {}", id);
                                        overrideIds.add(id);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    /**
     * Generate broker capacity entry for capacity configuration.
     *
     * @param brokerCapacity Broker capacity object
     * @return Broker entry as a JsonObject
     */
    private JsonObject generateBrokerCapacity(BrokerCapacity brokerCapacity) {
        return new JsonObject()
            .put(BROKER_ID_KEY, brokerCapacity.getId())
            .put(CAPACITY_KEY, new JsonObject()
                .put(DISK_KEY, brokerCapacity.getDisk().getJson())
                .put(CPU_KEY, brokerCapacity.getCpu().getJson())
                .put(INBOUND_NETWORK_KEY, brokerCapacity.getInboundNetwork())
                .put(OUTBOUND_NETWORK_KEY, brokerCapacity.getOutboundNetwork())
            )
            .put(DOC_KEY, brokerCapacity.getDoc());
    }

    /**
     * Generate a capacity configuration for cluster
     *
     * @return Cruise Control capacity configuration as a JsonObject
     */
    public JsonObject generateCapacityConfig() {
        JsonArray brokerList = new JsonArray();
        for (BrokerCapacity brokerCapacity : capacityEntries.values()) {
            JsonObject brokerEntry = generateBrokerCapacity(brokerCapacity);
            brokerList.add(brokerEntry);
        }

        JsonObject config = new JsonObject();
        config.put("brokerCapacities", brokerList);

        return config;
    }

    @Override
    public String toString() {
        return generateCapacityConfig().encodePrettily();
    }

    /**
     * @return  Capacity entries
     */
    public TreeMap<Integer, BrokerCapacity> getCapacityEntries() {
        return capacityEntries;
    }
}
