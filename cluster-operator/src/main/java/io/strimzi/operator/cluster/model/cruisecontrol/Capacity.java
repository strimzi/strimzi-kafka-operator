/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.balancing.BrokerCapacityOverride;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.StorageUtils;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.strimzi.operator.cluster.operator.resource.Quantities;
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
     * Resource type
     */
    public static final String RESOURCE_TYPE = "cpu";

    private static final String KAFKA_MOUNT_PATH = "/var/lib/kafka";
    private static final String KAFKA_LOG_DIR = "kafka-log";
    private static final String BROKER_ID_KEY = "brokerId";
    private static final String INBOUND_NETWORK_KEY = "NW_IN";
    private static final String OUTBOUND_NETWORK_KEY = "NW_OUT";
    private static final String DOC_KEY = "doc";

    private final Storage storage;

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
     * @param reconciliation    Reconciliation marker
     * @param spec              Spec of the Kafka custom resource
     * @param storage           Used storage configuration
     */
    public Capacity(Reconciliation reconciliation, KafkaSpec spec, Storage storage) {
        this.reconciliation = reconciliation;
        this.capacityEntries = new TreeMap<>();
        this.storage = storage;

        processCapacityEntries(spec);
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

    private static String getCpuBasedOnRequirements(ResourceRequirements resourceRequirements) {
        Integer request = getResourceRequirement(resourceRequirements, ResourceRequirementType.REQUEST);
        Integer limit = getResourceRequirement(resourceRequirements, ResourceRequirementType.LIMIT);

        if (limit != null) {
            if (request == null || limit.intValue() == request.intValue()) {
                return CpuCapacity.milliCpuToCpu(limit);
            }
        }
        return null;
    }

    private CpuCapacity processCpu(io.strimzi.api.kafka.model.balancing.BrokerCapacity bc, BrokerCapacityOverride override, String cpuBasedOnRequirements) {
        if (cpuBasedOnRequirements != null) {
            if ((override != null && override.getCpu() != null) || (bc != null && bc.getCpu() != null)) {
                LOGGER.warnCr(reconciliation, "Ignoring CPU capacity override settings since they are automatically set to resource limits");
            }
            return new CpuCapacity(cpuBasedOnRequirements);
        } else if (override != null && override.getCpu() != null) {
            return new CpuCapacity(override.getCpu());
        } else if (bc != null && bc.getCpu() != null) {
            return new CpuCapacity(bc.getCpu());
        } else {
            return new CpuCapacity(BrokerCapacity.DEFAULT_CPU_CORE_CAPACITY);
        }
    }

    private static DiskCapacity processDisk(Storage storage, int brokerId) {
        if (storage instanceof JbodStorage) {
            return generateJbodDiskCapacity(storage, brokerId);
        } else {
            return generateDiskCapacity(storage);
        }
    }

    private static String processInboundNetwork(io.strimzi.api.kafka.model.balancing.BrokerCapacity bc, BrokerCapacityOverride override) {
        if (override != null && override.getInboundNetwork() != null) {
            return getThroughputInKiB(override.getInboundNetwork());
        } else if (bc != null && bc.getInboundNetwork() != null) {
            return getThroughputInKiB(bc.getInboundNetwork());
        } else {
            return BrokerCapacity.DEFAULT_INBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND;
        }
    }

    private static String processOutboundNetwork(io.strimzi.api.kafka.model.balancing.BrokerCapacity bc, BrokerCapacityOverride override) {
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

    private void processCapacityEntries(KafkaSpec spec) {
        io.strimzi.api.kafka.model.balancing.BrokerCapacity brokerCapacity = spec.getCruiseControl().getBrokerCapacity();
        String cpuBasedOnRequirements = getCpuBasedOnRequirements(spec.getKafka().getResources());
        int replicas = spec.getKafka().getReplicas();

        CpuCapacity cpu = processCpu(brokerCapacity, null, cpuBasedOnRequirements);
        DiskCapacity disk = processDisk(storage, BrokerCapacity.DEFAULT_BROKER_ID);
        String inboundNetwork = processInboundNetwork(brokerCapacity, null);
        String outboundNetwork = processOutboundNetwork(brokerCapacity, null);

        // Default broker entry
        BrokerCapacity defaultBrokerCapacity = new BrokerCapacity(BrokerCapacity.DEFAULT_BROKER_ID, cpu, disk, inboundNetwork, outboundNetwork);
        capacityEntries.put(BrokerCapacity.DEFAULT_BROKER_ID, defaultBrokerCapacity);

        if (storage instanceof JbodStorage) {
            // A capacity configuration for a cluster with a JBOD configuration
            // requires a distinct broker capacity entry for every broker because the
            // Kafka volume paths are not homogeneous across brokers and include
            // the broker pod index in their names.
            List<String> podList = KafkaCluster.generatePodList(reconciliation.name(), replicas);
            for (int podIndex = 0; podIndex < podList.size(); podIndex++) {
                int id = ModelUtils.idOfPod(podList.get(podIndex));
                disk = processDisk(storage, id);
                BrokerCapacity broker = new BrokerCapacity(id, cpu, disk, inboundNetwork, outboundNetwork);
                capacityEntries.put(id, broker);
            }
        }

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
                        cpu = processCpu(brokerCapacity, override, cpuBasedOnRequirements);
                        inboundNetwork = processInboundNetwork(brokerCapacity, override);
                        outboundNetwork = processOutboundNetwork(brokerCapacity, override);
                        for (int id : ids) {
                            if (id == BrokerCapacity.DEFAULT_BROKER_ID) {
                                LOGGER.warnCr(reconciliation, "Ignoring broker capacity override with illegal broker id -1.");
                            } else {
                                if (capacityEntries.containsKey(id)) {
                                    if (overrideIds.add(id)) {
                                        BrokerCapacity brokerCapacityEntry = capacityEntries.get(id);
                                        brokerCapacityEntry.setCpu(cpu);
                                        brokerCapacityEntry.setInboundNetwork(inboundNetwork);
                                        brokerCapacityEntry.setOutboundNetwork(outboundNetwork);
                                    } else {
                                        LOGGER.warnCr(reconciliation, "Duplicate broker id %d found in overrides, using first occurrence.", id);
                                    }
                                } else {
                                    BrokerCapacity brokerCapacityEntry = new BrokerCapacity(id, cpu, disk, inboundNetwork, outboundNetwork);
                                    capacityEntries.put(id, brokerCapacityEntry);
                                    overrideIds.add(id);
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
