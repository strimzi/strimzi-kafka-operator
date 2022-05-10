/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.balancing.BrokerCapacity;
import io.strimzi.api.kafka.model.balancing.BrokerCapacityOverride;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.StorageUtils;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;

/**
 * Uses information in a Kafka Custom Resource to generate a capacity configuration file to be used for
 * Cruise Control's Broker Capacity File Resolver.
 *
 *
 * For example, takes a Kafka Custom Resource like the following:
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
 *     inboundNetwork: 10000KB/s
 *     outboundNetwork: 10000KB/s
 *     overrides:
 *       - brokers: [0]
 *         outboundNetwork: 40000KB/s
 *       - brokers: [1, 2]
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
 *         "CPU": "100",
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
 *         "CPU": "100",
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
 *         "CPU": "100",
 *         "NW_IN": "60000",
 *         "NW_OUT": "20000"
 *       },
 *       "doc": "Capacity for Broker 1"
 *     },
 *       "brokerId": "2",
 *       "capacity": {
 *         "DISK": {
 *             "/var/lib/kafka0/kafka-log1": "100000",
 *             "/var/lib/kafka1/kafka-log1": "200000"
 *           },
 *         "CPU": "100",
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

    private TreeMap<Integer, Broker> capacityEntries;

    private int replicas;
    private Storage storage;

    public Capacity(KafkaSpec spec, Storage storage) {
        BrokerCapacity bc = spec.getCruiseControl().getBrokerCapacity();

        this.capacityEntries = new TreeMap<Integer, Broker>();
        this.replicas = spec.getKafka().getReplicas();
        this.storage = storage;

        processCapacityEntries(bc, storage);
    }

    public static String processCpu() {
        return Broker.DEFAULT_CPU_UTILIZATION_CAPACITY;
    }

    public static String processDisk(Storage storage, int brokerId) {
        if (storage instanceof  JbodStorage) {
            return generateJbodDiskCapacity(storage, brokerId).encodePrettily();
        } else {
            return generateDiskCapacity(storage);
        }
    }

    public static String processInboundNetwork(BrokerCapacity bc, BrokerCapacityOverride override) {
        if (override != null && override.getInboundNetwork() != null) {
            return getThroughputInKiB(override.getInboundNetwork());
        } else if (bc != null && bc.getInboundNetwork() != null) {
            return getThroughputInKiB(bc.getInboundNetwork());
        } else {
            return Broker.DEFAULT_INBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND;
        }
    }

    public static String processOutboundNetwork(BrokerCapacity bc, BrokerCapacityOverride override) {
        if (override != null && override.getOutboundNetwork() != null) {
            return getThroughputInKiB(override.getOutboundNetwork());
        } else if (bc != null && bc.getOutboundNetwork() != null) {
            return getThroughputInKiB(bc.getOutboundNetwork());
        } else {
            return Broker.DEFAULT_OUTBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND;
        }
    }

    /**
     * Generate JBOD disk capacity configuration for a broker using the supplied storage configuration
     *
     * @param storage Storage configuration for Kafka cluster
     * @param idx Index of the broker
     * @return Disk capacity configuration value as a JsonObject for broker idx
     */
    private static JsonObject generateJbodDiskCapacity(Storage storage, int idx) {
        JsonObject disks = new JsonObject();
        String size = "";

        for (SingleVolumeStorage volume : ((JbodStorage) storage).getVolumes()) {
            String name = VolumeUtils.createVolumePrefix(volume.getId(), true);
            String path = AbstractModel.KAFKA_MOUNT_PATH + "/" + name + "/" + AbstractModel.KAFKA_LOG_DIR + idx;

            if (volume instanceof PersistentClaimStorage) {
                size = ((PersistentClaimStorage) volume).getSize();
            } else if (volume instanceof EphemeralStorage) {
                size = ((EphemeralStorage) volume).getSizeLimit();
            }
            disks.put(path, String.valueOf(getSizeInMiB(size)));
        }
        return disks;
    }

    /**
     * Generate total disk capacity using the supplied storage configuration
     *
     * @param storage Storage configuration for Kafka cluster
     * @return Disk capacity per broker as a Double
     */
    public static String generateDiskCapacity(Storage storage) {
        if (storage instanceof PersistentClaimStorage) {
            return getSizeInMiB(((PersistentClaimStorage) storage).getSize());
        } else if (storage instanceof EphemeralStorage) {
            if (((EphemeralStorage) storage).getSizeLimit() != null) {
                return getSizeInMiB(((EphemeralStorage) storage).getSizeLimit());
            } else {
                return Broker.DEFAULT_DISK_CAPACITY_IN_MIB;
            }
        } else {
            throw new IllegalStateException("The declared storage '" + storage.getType() + "' is not supported");
        }
    }

    /*
     * Parse a K8S-style representation of a disk size, such as {@code 100Gi},
     * into the equivalent number of mebibytes represented as a Double.
     *
     * @param size The String representation of the volume size.
     * @return The equivalent number of mebibytes.
     */
    public static String getSizeInMiB(String size) {
        if (size == null) {
            return Broker.DEFAULT_DISK_CAPACITY_IN_MIB;
        }
        return String.valueOf(StorageUtils.convertTo(size, "Mi"));
    }

    /*
     * Parse Strimzi representation of throughput, such as {@code 10000KB/s},
     * into the equivalent number of kibibytes represented as a Double.
     *
     * @param throughput The String representation of the throughput.
     * @return The equivalent number of kibibytes.
     */
    public static String getThroughputInKiB(String throughput) {
        String size = throughput.substring(0, throughput.indexOf("B"));
        return String.valueOf(StorageUtils.convertTo(size, "Ki"));
    }

    private void processCapacityEntries(BrokerCapacity bc, Storage s) {
        String cpu = processCpu();
        String disk = processDisk(storage, Broker.DEFAULT_BROKER_ID);
        String inboundNetwork = processInboundNetwork(bc, null);
        String outboundNetwork = processOutboundNetwork(bc, null);

        // Default broker entry
        Broker defaultBroker = new Broker(Broker.DEFAULT_BROKER_ID, cpu, disk, inboundNetwork, outboundNetwork);
        capacityEntries.put(Broker.DEFAULT_BROKER_ID, defaultBroker);

        if (storage instanceof JbodStorage) {
            // A capacity configuration for a cluster with a JBOD configuration
            // requires a distinct broker capacity entry for every broker because the
            // Kafka volume paths are not homogeneous across brokers and include
            // the broker pod index in their names.
            for (int id = 0; id < replicas; id++) {
                disk = processDisk(storage, id);
                Broker broker = new Broker(id, cpu, disk, inboundNetwork, outboundNetwork);
                capacityEntries.put(id, broker);
            }
        }

        if (bc != null) {
            // For checking for duplicate brokerIds
            HashSet<Integer> overrideIds = new HashSet<>();
            List<BrokerCapacityOverride> overrides = bc.getOverrides();
            // Override broker entries
            if (overrides != null) {
                if (overrides.isEmpty()) {
                    LOGGER.warnOp("Ignoring empty overrides list");
                } else {
                    for (BrokerCapacityOverride override : overrides) {
                        List<Integer> ids = override.getBrokers();
                        inboundNetwork = processInboundNetwork(bc, override);
                        outboundNetwork = processOutboundNetwork(bc, override);
                        for (Integer id : ids) {
                            if (id == Broker.DEFAULT_BROKER_ID) {
                                LOGGER.warnOp("Ignoring broker capacity override with illegal broker id -1.");
                            } else {
                                disk = processDisk(storage, id);
                                if (capacityEntries.containsKey(id)) {
                                    if (overrideIds.contains(id)) {
                                        LOGGER.warnOp("Duplicate broker id %d found in overrides, using last occurrence.", id);
                                    } else {
                                        overrideIds.add(id);
                                    }
                                    Broker broker = capacityEntries.get(id);
                                    broker.setInboundNetworkKiBPerSecond(inboundNetwork);
                                    broker.setOutboundNetworkKiBPerSecond(outboundNetwork);
                                } else {
                                    Broker broker = new Broker(id, cpu, disk, inboundNetwork, outboundNetwork);
                                    capacityEntries.put(id, broker);
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
     * @param broker Broker capacity object
     * @return Broker entry as a JsonObject
     */
    private JsonObject generateBrokerCapacity(Broker broker) {
        JsonObject brokerCapacity = new JsonObject()
            .put("brokerId", broker.getId())
            .put("capacity", new JsonObject()
                .put("DISK", broker.getDisk())
                .put("CPU", broker.getCpu())
                .put("NW_IN", broker.getInboundNetworkKiBPerSecond())
                .put("NW_OUT", broker.getOutboundNetworkKiBPerSecond())
            )
            .put("doc", broker.getDoc());
        return brokerCapacity;
    }

    /**
     * Generate a capacity configuration for cluster
     *
     * @return Cruise Control capacity configuration as a String
     */
    public String generateCapacityConfig() {
        JsonArray brokerList = new JsonArray();
        for (Broker broker : capacityEntries.values()) {
            JsonObject brokerEntry = generateBrokerCapacity(broker);
            brokerList.add(brokerEntry);
        }

        JsonObject config = new JsonObject();
        config.put("brokerCapacities", brokerList);

        return config.encodePrettily();
    }

    public TreeMap<Integer, Broker> getCapacityEntries() {
        return capacityEntries;
    }
}
