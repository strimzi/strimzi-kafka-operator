/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.SingleVolumeStorage;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.operator.cluster.model.StorageUtils;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Cruise Control disk capacity configuration for broker.
 */
public class DiskCapacity {
    protected static final String DEFAULT_DISK_CAPACITY_IN_MIB = "100000";

    private static final String KAFKA_MOUNT_PATH = "/var/lib/kafka";
    private static final String KAFKA_LOG_DIR = "kafka-log";
    private static final String SINGLE_DISK = "";

    private final Map<String, String> config;

    /**
     * Constructor
     *
     * Generate JBOD disk capacity configuration for a broker using the supplied storage configuration.
     *
     * @param storage Storage configuration for Kafka cluster.
     * @param brokerId Id of the broker.
     */
    public DiskCapacity(Storage storage, int brokerId) {
        if (storage instanceof JbodStorage) {
            config = generateJbodDiskConfig(storage, brokerId);
        } else {
            config = generateNonJbodDiskConfig(storage);
        }
    }

    /**
     * Parse a K8S-style representation of a disk size, such as {@code 100Gi},
     * into the equivalent number of mebibytes represented as a String.
     *
     * @param size The String representation of the volume size.
     * @return The equivalent number of mebibytes.
     */
    private static String getSizeInMiB(String size) {
        if (size == null) {
            return ResourceCapacityType.DISK.getDefaultResourceCapacity();
        }
        return String.valueOf(StorageUtils.convertTo(size, "Mi"));
    }

    /**
     * Generate JBOD disk capacity configuration for a broker using the supplied storage configuration.
     *
     * @param storage Storage configuration for Kafka cluster.
     * @param brokerId Id of the broker.
     *      
     * @return Disk capacity configuration value for broker brokerId.
     */
    private Map<String, String> generateJbodDiskConfig(Storage storage, int brokerId) {
        String size = "";
        Map<String, String> diskConfig = new HashMap<>();
        for (SingleVolumeStorage volume : ((JbodStorage) storage).getVolumes()) {
            String name = VolumeUtils.createVolumePrefix(volume.getId(), true);
            String path = KAFKA_MOUNT_PATH + "/" + name + "/" + KAFKA_LOG_DIR + brokerId;

            if (volume instanceof PersistentClaimStorage) {
                size = ((PersistentClaimStorage) volume).getSize();
            } else if (volume instanceof EphemeralStorage) {
                size = ((EphemeralStorage) volume).getSizeLimit();
            }
            diskConfig.put(path, String.valueOf(getSizeInMiB(size)));
        }
        return diskConfig;
    }

    /**
     * Generate total disk capacity using the supplied storage configuration.
     *
     * @param storage Storage configuration for Kafka cluster.
     *
     * @return Disk capacity configuration value for broker.
     */
    private static Map<String, String> generateNonJbodDiskConfig(Storage storage) {
        String size;
        if (storage instanceof PersistentClaimStorage) {
            size = getSizeInMiB(((PersistentClaimStorage) storage).getSize());
        } else if (storage instanceof EphemeralStorage) {
            if (((EphemeralStorage) storage).getSizeLimit() != null) {
                size = getSizeInMiB(((EphemeralStorage) storage).getSizeLimit());
            } else {
                size = DEFAULT_DISK_CAPACITY_IN_MIB;
            }
        } else if (storage == null) {
            throw new IllegalStateException("The storage declaration is missing");
        } else {
            throw new IllegalStateException("The declared storage '" + storage.getType() + "' is not supported");
        }
        return Map.of(SINGLE_DISK, size);
    }

    protected Object getJson() {
        if (config.size() == 1 && config.containsKey(SINGLE_DISK)) {
            return config.get(SINGLE_DISK);
        } else {
            JsonObject disks = new JsonObject();
            for (Map.Entry<String, String> e : config.entrySet()) {
                disks.put(e.getKey(), e.getValue());
            }
            return disks;
        }
    }

    @Override
    public String toString() {
        return this.getJson().toString();
    }
}