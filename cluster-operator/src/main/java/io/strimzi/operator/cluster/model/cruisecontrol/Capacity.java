/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.strimzi.api.kafka.model.balancing.CruiseControlBrokerCapacity;
import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;

import java.util.List;

import static io.strimzi.operator.cluster.model.StorageUtils.parseMemorybyFactor;

public class Capacity {
    public static final int DEFAULT_BROKER_DISK_CAPACITY   = 100000;  // in MB
    public static final int DEFAULT_BROKER_CPU_CAPACITY    = 100;     // as a percentage (0-100)
    public static final int DEFAULT_BROKER_NW_IN_CAPACITY  = 10000;   // in KB/s
    public static final int DEFAULT_BROKER_NW_OUT_CAPACITY = 10000;   // in KB/s

    private Integer disk;
    private Integer cpu;
    private Integer nwIn;
    private Integer nwOut;

    public Capacity(KafkaSpec spec) {
        CruiseControlBrokerCapacity bc = spec.getCruiseControl().getCapacity();

        if (bc != null) {
            this.disk = bc.getDisk() != null ? bc.getDisk() : generateDiskCapacity(spec.getKafka().getStorage());
            this.cpu = bc.getCpu() != null ? bc.getCpu() : DEFAULT_BROKER_CPU_CAPACITY;
            this.nwIn = bc.getNetworkIn() != null ? bc.getNetworkIn() : DEFAULT_BROKER_NW_IN_CAPACITY;
            this.nwOut = bc.getNetworkOut() != null ? bc.getNetworkOut() : DEFAULT_BROKER_NW_OUT_CAPACITY;
        } else {
            this.disk = generateDiskCapacity(spec.getKafka().getStorage());
            this.cpu = DEFAULT_BROKER_CPU_CAPACITY;
            this.nwIn = DEFAULT_BROKER_NW_IN_CAPACITY;
            this.nwOut = DEFAULT_BROKER_NW_OUT_CAPACITY;
        }
    }

    /**
     * Generate disk capacity configuration from the supplied storage configuration
     *
     * @param storage Storage configuration for Kafka cluster
     * @return Disk capacity configuration as a String
     */
    public static Integer generateDiskCapacity(Storage storage) {
        if (storage instanceof PersistentClaimStorage) {
            return getSizeInMb(((PersistentClaimStorage) storage).getSize());
        } else if (storage instanceof EphemeralStorage) {
            if (((EphemeralStorage) storage).getSizeLimit() != null) {
                return getSizeInMb(((EphemeralStorage) storage).getSizeLimit());
            } else {
                return DEFAULT_BROKER_DISK_CAPACITY;
            }
        } else if (storage instanceof JbodStorage) {
            List<SingleVolumeStorage> volumeList = ((JbodStorage) storage).getVolumes();
            int size = 0;
            for (SingleVolumeStorage volume : volumeList) {
                size += generateDiskCapacity(volume);
            }
            return size;
        } else {
            throw new IllegalStateException("The declared storage '" + storage.getType() + "' is not supported");
        }
    }

    /*
     * Parse a K8S-style representation of a disk size, such as {@code 100Gi},
     * into the equivalent number of megabytes represented as a Integer.
     *
     * @param size The String representation of the volume size.
     * @return The equivalent number of Megabytes.
     */
    public static Integer getSizeInMb(String size) {
        return Math.toIntExact(parseMemorybyFactor(size, size.charAt(size.length() - 1) == 'i' ? "Mi" : "M"));
    }

    public Integer getDisk() {
        return disk;
    }

    public void setDisk(Integer disk) {
        this.disk = disk;
    }

    public Integer getCpu() {
        return cpu;
    }

    public void setCpu(Integer cpu) {
        this.cpu = cpu;
    }

    public Integer getNwIn() {
        return nwIn;
    }

    public void setNwIn(Integer nwIn) {
        this.nwIn = nwIn;
    }

    public Integer getNwOut() {
        return nwOut;
    }

    public void setNwOut(Integer nwOut) {
        this.nwOut = nwOut;
    }
}
