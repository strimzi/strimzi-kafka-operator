/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import java.util.Map;

/**
 * Rack Writer configuration
 */
public class RackWriterConfig {

    public static final String RACK_FOLDER = "RACK_FOLDER";
    public static final String RACK_TOPOLOGY_KEY = "RACK_TOPOLOGY_KEY";
    public static final String NODE_NAME = "NODE_NAME";

    public static final String DEFAULT_RACK_FOLDER = "/opt/kafka/rack";

    private String nodeName;
    private String rackTopologyKey;
    private String rackFolder;

    /**
     * Load configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return Rack Writer configuration instance
     */
    public static RackWriterConfig fromMap(Map<String, String> map) {

        String nodeName = map.get(RackWriterConfig.NODE_NAME);
        if (nodeName == null || nodeName.equals("")) {
            throw new IllegalArgumentException(RackWriterConfig.NODE_NAME + " cannot be null or empty");
        }

        String rackTopologyKey = map.get(RackWriterConfig.RACK_TOPOLOGY_KEY);
        if (rackTopologyKey == null || rackTopologyKey.equals("")) {
            throw new IllegalArgumentException(RackWriterConfig.RACK_TOPOLOGY_KEY + " cannot be null or empty");
        }

        String rackFolder = DEFAULT_RACK_FOLDER;
        String rackFolderEnvVar = map.get(RackWriterConfig.RACK_FOLDER);
        if (rackFolderEnvVar != null) {
            rackFolder = rackFolderEnvVar;
        }

        return new RackWriterConfig(nodeName, rackTopologyKey, rackFolder);
    }

    public RackWriterConfig(String nodeName, String rackTopologyKey, String rackFolder) {
        this.nodeName = nodeName;
        this.rackTopologyKey = rackTopologyKey;
        this.rackFolder = rackFolder;
    }

    /**
     * @return Kubernetes/OpenShift cluster node name from which getting the rack related label
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * @return the Kubernetes/OpenShift cluster node label to use as topology key for rack definition
     */
    public String getRackTopologyKey() {
        return rackTopologyKey;
    }

    /**
     * @return folder where the rackid file is written
     */
    public String getRackFolder() {
        return rackFolder;
    }

    @Override
    public String toString() {
        return "RackWriterConfig(" +
                "nodeName=" + nodeName +
                ",rackTopologyKey=" + rackTopologyKey +
                ",rackFolder=" + rackFolder +
                ")";
    }
}
