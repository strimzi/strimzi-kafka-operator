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

import java.util.List;

import static io.strimzi.operator.cluster.model.StorageUtils.parseMemory;

public class Capacity {
    public static final long DEFAULT_BROKER_DISK_MIB_CAPACITY = 100_000;  // in MiB
    public static final int DEFAULT_BROKER_CPU_UTILIZATION_CAPACITY = 100;  // as a percentage (0-100)
    public static final int DEFAULT_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY = 10_000;  // in KiB/s
    public static final int DEFAULT_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY = 10_000;  // in KiB/s

    private Long diskMiB;
    private Integer cpuUtilization;
    private Integer inboundNetworkKiBPerSecond;
    private Integer outboutNetworkKibPerSecond;

    public Capacity(KafkaSpec spec) {
        BrokerCapacity bc = spec.getCruiseControl().getBrokerCapacity();

        this.diskMiB = bc != null && bc.getDiskMiB() != null ? bc.getDiskMiB() : generateDiskCapacity(spec.getKafka().getStorage());
        this.cpuUtilization = bc != null && bc.getCpuUtilization() != null ? bc.getCpuUtilization() : DEFAULT_BROKER_CPU_UTILIZATION_CAPACITY;
        this.inboundNetworkKiBPerSecond = bc != null && bc.getInboundNetworkKiBPerSecond() != null ? bc.getInboundNetworkKiBPerSecond() : DEFAULT_BROKER_INBOUND_NETWORK_KIB_PER_SECOND_CAPACITY;
        this.outboutNetworkKibPerSecond = bc != null && bc.getOutboundNetworkKiBPerSecond() != null ? bc.getOutboundNetworkKiBPerSecond() : DEFAULT_BROKER_OUTBOUND_NETWORK_KIB_PER_SECOND_CAPACITY;
    }

    /**
     * Generate diskMiB capacity configuration from the supplied storage configuration
     *
     * @param storage Storage configuration for Kafka cluster
     * @return Disk capacity configuration as a Long
     */
    public static Long generateDiskCapacity(Storage storage) {
        if (storage instanceof PersistentClaimStorage) {
            return getSizeInMiB(((PersistentClaimStorage) storage).getSize());
        } else if (storage instanceof EphemeralStorage) {
            if (((EphemeralStorage) storage).getSizeLimit() != null) {
                return getSizeInMiB(((EphemeralStorage) storage).getSizeLimit());
            } else {
                return DEFAULT_BROKER_DISK_MIB_CAPACITY;
            }
        } else if (storage instanceof JbodStorage) {
            List<SingleVolumeStorage> volumeList = ((JbodStorage) storage).getVolumes();
            long size = 0;
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
     * into the equivalent number of mebibytes represented as a Long.
     *
     * @param size The String representation of the volume size.
     * @return The equivalent number of mebibytes.
     */
    public static Long getSizeInMiB(String size) {
        return parseMemory(size, "Mi");
    }

    public Long getDiskMiB() {
        return diskMiB;
    }

    public void setDiskMiB(Long diskMiB) {
        this.diskMiB = diskMiB;
    }

    public Integer getCpuUtilization() {
        return cpuUtilization;
    }

    public void setCpuUtilization(Integer cpuUtilization) {
        this.cpuUtilization = cpuUtilization;
    }

    public Integer getInboundNetworkKiBPerSecond() {
        return inboundNetworkKiBPerSecond;
    }

    public void setInboundNetworkKiBPerSecond(Integer inboundNetworkKiBPerSecond) {
        this.inboundNetworkKiBPerSecond = inboundNetworkKiBPerSecond;
    }

    public Integer getOutboutNetworkKibPerSecond() {
        return outboutNetworkKibPerSecond;
    }

    public void setOutboutNetworkKibPerSecond(Integer outboutNetworkKibPerSecond) {
        this.outboutNetworkKibPerSecond = outboutNetworkKibPerSecond;
    }
}
