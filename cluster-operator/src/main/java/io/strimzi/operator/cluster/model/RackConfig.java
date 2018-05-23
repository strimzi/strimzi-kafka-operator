/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

public class RackConfig {

    private String topologyKey;

    public RackConfig() {

    }

    public RackConfig(String topologyKey) {
        this.topologyKey = topologyKey;
    }

    public String getTopologyKey() {
        return topologyKey;
    }

    public static RackConfig fromJson(String json) {
        RackConfig rackConfig = JsonUtils.fromJson(json, RackConfig.class);
        if (rackConfig != null && (rackConfig.getTopologyKey() == null || rackConfig.getTopologyKey().equals(""))) {
            throw new IllegalArgumentException("In rack configuration the 'topologyKey' field is mandatory");
        }
        return rackConfig;
    }
}
