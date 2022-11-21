/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import java.util.Map;

/**
 * Init Writer configuration
 */
public class InitWriterConfig {
    static final String INIT_FOLDER = "INIT_FOLDER";
    static final String RACK_TOPOLOGY_KEY = "RACK_TOPOLOGY_KEY";
    static final String NODE_NAME = "NODE_NAME";
    static final String EXTERNAL_ADDRESS = "EXTERNAL_ADDRESS";
    static final String EXTERNAL_ADDRESS_TYPE = "EXTERNAL_ADDRESS_TYPE";

    static final String DEFAULT_INIT_FOLDER = "/opt/kafka/init";

    private String nodeName;
    private String rackTopologyKey;
    private boolean externalAddress;
    private String addressType;
    private String initFolder;

    /**
     * Load configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return Rack Writer configuration instance
     */
    static InitWriterConfig fromMap(Map<String, String> map) {

        String nodeName = map.get(InitWriterConfig.NODE_NAME);
        if (nodeName == null || nodeName.equals("")) {
            throw new IllegalArgumentException(InitWriterConfig.NODE_NAME + " cannot be null or empty");
        }

        String rackTopologyKey = map.get(InitWriterConfig.RACK_TOPOLOGY_KEY);

        boolean externalAddress = map.containsKey(InitWriterConfig.EXTERNAL_ADDRESS);

        String initFolder = DEFAULT_INIT_FOLDER;
        String initFolderEnvVar = map.get(InitWriterConfig.INIT_FOLDER);
        if (initFolderEnvVar != null) {
            initFolder = initFolderEnvVar;
        }

        String externalAddressType = map.get(InitWriterConfig.EXTERNAL_ADDRESS_TYPE);

        return new InitWriterConfig(nodeName, rackTopologyKey, externalAddress, initFolder, externalAddressType);
    }

    InitWriterConfig(String nodeName, String rackTopologyKey, boolean externalAddress, String initFolder, String externalAddressType) {
        this.nodeName = nodeName;
        this.rackTopologyKey = rackTopologyKey;
        this.externalAddress = externalAddress;
        this.initFolder = initFolder;
        this.addressType = externalAddressType;
    }

    /**
     * @return Kubernetes cluster node name from which getting the rack related label
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * @return the Kubernetes cluster node label to use as topology key for rack definition
     */
    public String getRackTopologyKey() {
        return rackTopologyKey;
    }

    /**
     * @return folder where the rackid file is written
     */
    public String getInitFolder() {
        return initFolder;
    }

    /**
     * @return Return whether external address should be acquired
     */
    public boolean isExternalAddress() {
        return externalAddress;
    }

    /**
     * @return The address type which should be preferred in the selection
     */
    public String getAddressType() {
        return addressType;
    }

    @Override
    public String toString() {
        return "InitWriterConfig(" +
                "nodeName=" + nodeName +
                ",rackTopologyKey=" + rackTopologyKey +
                ",externalAddress=" + externalAddress +
                ",initFolder=" + initFolder +
                ",addressType=" + addressType +
                ")";
    }
}
