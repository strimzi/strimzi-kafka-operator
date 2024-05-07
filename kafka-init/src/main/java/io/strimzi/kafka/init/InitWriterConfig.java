/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.strimzi.operator.common.config.ConfigParameter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.strimzi.operator.common.config.ConfigParameterParser.BOOLEAN;
import static io.strimzi.operator.common.config.ConfigParameterParser.NON_EMPTY_STRING;
import static io.strimzi.operator.common.config.ConfigParameterParser.STRING;

/**
 * Init Writer configuration
 */
public class InitWriterConfig {
    private static final Map<String, ConfigParameter<?>> CONFIG_VALUES = new HashMap<>();
    /**
     * Folder where the rackid file is written
     */
    public static final ConfigParameter<String> INIT_FOLDER = new ConfigParameter<>("INIT_FOLDER", STRING, "/opt/kafka/init", CONFIG_VALUES);
    /**
     * Kubernetes cluster node name from which getting the rack related label
     */
    public static final ConfigParameter<String> NODE_NAME = new ConfigParameter<>("NODE_NAME", NON_EMPTY_STRING, CONFIG_VALUES);
    /**
     * Kubernetes cluster node label to use as topology key for rack definition
     */
    public static final ConfigParameter<String> RACK_TOPOLOGY_KEY = new ConfigParameter<>("RACK_TOPOLOGY_KEY", STRING, null, CONFIG_VALUES);
    /**
     * Whether external address should be acquired
     */
    public static final ConfigParameter<Boolean> EXTERNAL_ADDRESS = new ConfigParameter<>("EXTERNAL_ADDRESS", BOOLEAN, "false", CONFIG_VALUES);
    /**
     * The address type which should be preferred in the selection
     */
    public static final ConfigParameter<String> EXTERNAL_ADDRESS_TYPE = new ConfigParameter<>("EXTERNAL_ADDRESS_TYPE", STRING, null, CONFIG_VALUES);
    private final Map<String, Object> map;

    /**
     * @return Set of configuration key/names
     */
    public static Set<String> keyNames() {
        return Collections.unmodifiableSet(CONFIG_VALUES.keySet());
    }

    private InitWriterConfig(Map<String, Object> map) {
        this.map = map;
    }
    /**
     * Load configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return Rack Writer configuration instance
     */
    static InitWriterConfig fromMap(Map<String, String> map) {
        Map<String, String> envMap = new HashMap<>(map);
        envMap.keySet().retainAll(InitWriterConfig.keyNames());

        Map<String, Object> generatedMap = ConfigParameter.define(envMap, CONFIG_VALUES);

        return new InitWriterConfig(generatedMap);
    }

    /**
     * Gets the configuration value corresponding to the key
     * @param <T>      Type of value
     * @param value    Instance of Config Parameter class
     * @return         Configuration value w.r.t to the key
     */
    @SuppressWarnings("unchecked")
    public <T> T get(ConfigParameter<T> value) {
        return (T) this.map.get(value.key());
    }

    /**
     * @return Kubernetes cluster node name from which getting the rack related label
     */
    public String getNodeName() {
        return get(NODE_NAME);
    }

    /**
     * @return the Kubernetes cluster node label to use as topology key for rack definition
     */
    public String getRackTopologyKey() {
        return get(RACK_TOPOLOGY_KEY);
    }

    /**
     * @return folder where the rackid file is written
     */
    public String getInitFolder() {
        return get(INIT_FOLDER);
    }

    /**
     * @return Return whether external address should be acquired
     */
    public boolean isExternalAddress() {
        return get(EXTERNAL_ADDRESS);
    }

    /**
     * @return The address type which should be preferred in the selection
     */
    public String getAddressType() {
        return get(EXTERNAL_ADDRESS_TYPE);
    }

    @Override
    public String toString() {
        return "InitWriterConfig(" +
                "nodeName=" + getNodeName() +
                ",rackTopologyKey=" + getRackTopologyKey() +
                ",externalAddress=" + isExternalAddress() +
                ",initFolder=" + getInitFolder() +
                ",addressType=" + getAddressType() +
                ")";
    }
}
