/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

public class Broker {
    // CC allows specifying a generic "default" broker entry in the capacity configuration to apply to all brokers without a specific broker entry.
    // CC designates the id of this default broker entry as "-1".
    public static final int DEFAULT_BROKER_ID = -1;
    public static final String DEFAULT_BROKER_DOC = "This is the default capacity. Capacity unit used for disk is in MB, cpu is in percentage, network throughput is in KB.";

    public static final String DEFAULT_CPU_UTILIZATION_CAPACITY = "100";  // as a percentage (0-100)
    public static final String DEFAULT_DISK_CAPACITY_IN_MIB = "100000";  // in MiB
    public static final String DEFAULT_INBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND = "10000";  // in KiB/s
    public static final String DEFAULT_OUTBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND = "10000";  // in KiB/s

    private Integer id;
    private String cpu;
    private String disk;
    private String inboundNetworkKiBPerSecond;
    private String outboundNetworkKiBPerSecond;
    private String doc;

    public Broker(int brokerId, String cpu, String disk, String inboundNetworkKiBPerSecond, String outboundNetworkKiBPerSecond) {
        this.id = brokerId;
        this.cpu = cpu;
        this.disk = disk;
        this.inboundNetworkKiBPerSecond = inboundNetworkKiBPerSecond;
        this.outboundNetworkKiBPerSecond = outboundNetworkKiBPerSecond;
        this.doc = brokerId == -1 ? DEFAULT_BROKER_DOC : "Capacity for Broker " + brokerId;
    }

    public Integer getId() {
        return id;
    }

    public String getCpu() {
        return cpu;
    }

    public String getDisk() {
        return disk;
    }

    public String getInboundNetworkKiBPerSecond() {
        return inboundNetworkKiBPerSecond;
    }

    public String getOutboundNetworkKiBPerSecond() {
        return outboundNetworkKiBPerSecond;
    }

    public String getDoc() {
        return doc;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setCpu(String cpu) {
        this.cpu = cpu;
    }

    public void setDisk(String disk) {
        this.disk = disk;
    }

    public void setInboundNetworkKiBPerSecond(String inboundNetworkKiBPerSecond) {
        this.inboundNetworkKiBPerSecond = inboundNetworkKiBPerSecond;
    }

    public void setOutboundNetworkKiBPerSecond(String outboundNetworkKiBPerSecond) {
        this.outboundNetworkKiBPerSecond = outboundNetworkKiBPerSecond;
    }
}
