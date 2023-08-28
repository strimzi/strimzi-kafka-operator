/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

/**
 * Configures the Kafka broker capacity
 */
public class BrokerCapacity {
    // CC allows specifying a generic "default" broker entry in the capacity configuration to apply to all brokers without a specific broker entry.
    // CC designates the id of this default broker entry as "-1".
    /**
     * Default broker ID
     */
    public static final int DEFAULT_BROKER_ID = -1;

    /**
     * Default outbound network capacity
     */
    public static final String DEFAULT_OUTBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND = "10000";

    private static final String DEFAULT_BROKER_DOC = "This is the default capacity. Capacity unit used for disk is in MiB, cpu is in number of cores, network throughput is in KiB.";
    /**
     * Default cpu core capacity
     */
    public static final String DEFAULT_CPU_CORE_CAPACITY = "1.0";
    protected static final String DEFAULT_DISK_CAPACITY_IN_MIB = "100000";
    protected static final String DEFAULT_INBOUND_NETWORK_CAPACITY_IN_KIB_PER_SECOND = "10000";

    private int id;
    private CpuCapacity cpu;
    private DiskCapacity disk;
    private String inboundNetwork;
    private String outboundNetwork;
    private final String doc;

    /**
     * Constructor
     *
     * @param brokerId          ID of the broker
     * @param cpu               CPU capacity
     * @param disk              Disk capacity
     * @param inboundNetwork    Inbound network capacity
     * @param outboundNetwork   Outbound network capacity
     */
    public BrokerCapacity(int brokerId, CpuCapacity cpu, DiskCapacity disk, String inboundNetwork, String outboundNetwork) {
        this.id = brokerId;
        this.cpu = cpu;
        this.disk = disk;
        this.inboundNetwork = inboundNetwork;
        this.outboundNetwork = outboundNetwork;
        this.doc = brokerId == -1 ? DEFAULT_BROKER_DOC : "Capacity for Broker " + brokerId;
    }

    /**
     * @return  Broker ID
     */
    protected Integer getId() {
        return id;
    }

    /**
     * @return  CPU capacity
     */
    public CpuCapacity getCpu() {
        return cpu;
    }

    /**
     * @return  Disk capacity
     */
    protected DiskCapacity getDisk() {
        return disk;
    }

    /**
     * @return  Inbound network capacity
     */
    public String getInboundNetwork() {
        return inboundNetwork;
    }

    /**
     * @return  Outbound network capacity
     */
    public String getOutboundNetwork() {
        return outboundNetwork;
    }

    protected String getDoc() {
        return doc;
    }

    protected void setCpu(CpuCapacity cpu) {
        this.cpu = cpu;
    }

    protected void setInboundNetwork(String inboundNetwork) {
        this.inboundNetwork = inboundNetwork;
    }

    protected void setOutboundNetwork(String outboundNetwork) {
        this.outboundNetwork = outboundNetwork;
    }
}
