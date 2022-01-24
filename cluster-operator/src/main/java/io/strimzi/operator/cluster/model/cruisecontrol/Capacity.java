/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.balancing.BrokerCapacity;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

import static io.strimzi.operator.cluster.model.StorageUtils.parseMemory;

public class Capacity {
    // CC allows specifying a generic "default" broker entry in the capacity configuration to apply to all brokers without a specific broker entry.
    // CC designates the id of this default broker entry as "-1".
    // When using a non-JBOD disk configuration, we use a "default" broker entry to configure all brokers
    private static final int DEFAULT_BROKER_ID = -1;
    private static final String DEFAULT_BROKER_DOC = "This is the default capacity. Capacity unit used for disk is in MB, cpu is in percentage, network throughput is in KB.";

    private static final double DEFAULT_BROKER_DISK_MIB_CAPACITY = 100_000;  // in MiB
    private static final int DEFAULT_BROKER_CPU_UTILIZATION_CAPACITY = 100;  // as a percentage (0-100)
    private static final double DEFAULT_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY = 10_000;  // in KiB/s
    private static final double DEFAULT_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY = 10_000;  // in KiB/s

    private Double diskMiB;
    private Integer cpuUtilization;
    private Double inboundNetworkKiBPerSecond;
    private Double outboundNetworkKiBPerSecond;

    private int replicas;
    private Storage storage;

    public Capacity(KafkaSpec spec, Storage storage) {
        BrokerCapacity bc = spec.getCruiseControl().getBrokerCapacity();

        this.replicas = spec.getKafka().getReplicas();
        this.storage = storage;

        this.diskMiB = bc != null && bc.getDisk() != null ? getSizeInMiB(bc.getDisk()) : generateDiskCapacity(storage);
        this.cpuUtilization = bc != null && bc.getCpuUtilization() != null ? bc.getCpuUtilization() : DEFAULT_BROKER_CPU_UTILIZATION_CAPACITY;
        this.inboundNetworkKiBPerSecond = bc != null && bc.getInboundNetwork() != null ? getThroughputInKiB(bc.getInboundNetwork()) : DEFAULT_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY;
        this.outboundNetworkKiBPerSecond = bc != null && bc.getOutboundNetwork() != null ? getThroughputInKiB(bc.getOutboundNetwork()) : DEFAULT_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY;
    }

    /**
     * Generate broker capacity entry for capacity configuration.
     *
     * @param brokerId Id of broker
     * @param diskCapacity Disk capacity configuration
     * @param doc Documentation for broker entry
     * @return Broker entry as a JsonObject
     */
    private JsonObject generateBrokerCapacity(int brokerId, JsonObject diskCapacity, String doc) {
        JsonObject brokerCapacity = new JsonObject()
            .put("brokerId", brokerId)
            .put("capacity", new JsonObject()
                .put("DISK", diskCapacity.getValue("DISK"))
                .put("CPU", Integer.toString(cpuUtilization))
                .put("NW_IN", Double.toString(inboundNetworkKiBPerSecond))
                .put("NW_OUT", Double.toString(outboundNetworkKiBPerSecond))
            )
            .put("doc", doc);
        return brokerCapacity;
    }

    /**
     * Generate JBOD disk capacity configuration for a broker using the supplied storage configuration
     *
     * @param storage Storage configuration for Kafka cluster
     * @param idx Index of the broker
     * @return Disk capacity configuration value as a JsonObject for broker idx
     */
    private JsonObject generateJbodDiskCapacity(Storage storage, int idx) {
        JsonObject json = new JsonObject();
        String name = "";
        String path = "";
        String size = "";

        for (SingleVolumeStorage volume : ((JbodStorage) storage).getVolumes()) {
            name = VolumeUtils.createVolumePrefix(volume.getId(), true);
            path = AbstractModel.KAFKA_MOUNT_PATH + "/" + name + "/" + AbstractModel.KAFKA_LOG_DIR + idx;

            if (volume instanceof PersistentClaimStorage) {
                size =  ((PersistentClaimStorage) volume).getSize();
            } else if (volume instanceof EphemeralStorage) {
                size = ((EphemeralStorage) volume).getSizeLimit();
            }
            json.put(path, String.valueOf(Capacity.getSizeInMiB(size)));
        }
        return json;
    }

    /**
     * Generate total disk capacity using the supplied storage configuration
     *
     * @param storage Storage configuration for Kafka cluster
     * @return Disk capacity per broker as a Double
     */
    public static Double generateDiskCapacity(Storage storage) {
        if (storage instanceof PersistentClaimStorage) {
            return getSizeInMiB(((PersistentClaimStorage) storage).getSize());
        } else if (storage instanceof EphemeralStorage) {
            if (((EphemeralStorage) storage).getSizeLimit() != null) {
                return getSizeInMiB(((EphemeralStorage) storage).getSizeLimit());
            } else {
                return DEFAULT_BROKER_DISK_MIB_CAPACITY;
            }
        } else if (storage instanceof JbodStorage) {
            // The value generated here for JBOD storage is used for tracking the total
            // disk capacity per broker. This will NOT be used for the final disk capacity
            // configuration since JBOD storage requires a special disk configuration.
            List<SingleVolumeStorage> volumeList = ((JbodStorage) storage).getVolumes();
            double size = 0;
            for (SingleVolumeStorage volume : volumeList) {
                size += generateDiskCapacity(volume);
            }
            return size;
        } else {
            throw new IllegalStateException("The declared storage '" + storage.getType() + "' is not supported");
        }
    }

    /**
     * Generate a capacity configuration for cluster
     *
     * @return Cruise Control capacity configuration as a String
     */
    public String generateCapacityConfig() {
        JsonArray brokerList = new JsonArray();
        if (storage instanceof JbodStorage) {
            // A capacity configuration for a cluster with a JBOD configuration
            // requires a distinct broker capacity entry for every broker because the
            // Kafka volume paths are not homogeneous across brokers and include
            // the broker pod index in their names.
            for (int idx = 0; idx < replicas; idx++) {
                JsonObject diskConfig = new JsonObject().put("DISK", generateJbodDiskCapacity(storage, idx));
                JsonObject brokerEntry = generateBrokerCapacity(idx, diskConfig, "Capacity for Broker " + idx);
                brokerList.add(brokerEntry);
            }
        } else {
            // A capacity configuration for a cluster without a JBOD configuration
            // can rely on a generic broker entry for all brokers
            JsonObject diskConfig = new JsonObject().put("DISK", String.valueOf(diskMiB));
            JsonObject defaultBrokerCapacity = generateBrokerCapacity(DEFAULT_BROKER_ID, diskConfig, DEFAULT_BROKER_DOC);
            brokerList.add(defaultBrokerCapacity);
        }
        JsonObject config = new JsonObject();
        config.put("brokerCapacities", brokerList);

        return config.encodePrettily();
    }

    /*
     * Parse a K8S-style representation of a disk size, such as {@code 100Gi},
     * into the equivalent number of mebibytes represented as a Double.
     *
     * @param size The String representation of the volume size.
     * @return The equivalent number of mebibytes.
     */
    public static Double getSizeInMiB(String size) {
        if (size == null) {
            return DEFAULT_BROKER_DISK_MIB_CAPACITY;
        }
        return parseMemory(size, "Mi");
    }

    /*
     * Parse Strimzi representation of throughput, such as {@code 10000KB/s},
     * into the equivalent number of kibibytes represented as a Double.
     *
     * @param throughput The String representation of the throughput.
     * @return The equivalent number of kibibytes.
     */
    public static Double getThroughputInKiB(String throughput) {
        String size = throughput.substring(0, throughput.indexOf("B"));
        return parseMemory(size, "Ki");
    }
}
