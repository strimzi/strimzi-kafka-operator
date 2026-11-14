/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.api.kafka.model.rebalance.RebalanceNode;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds Cruise Control rebalance request parameters: config map entries become query parameters,
 * and nodes identify the target brokers/disks for the operation.
 */
public class RebalanceOptions {
    private final Map<String, String> config;
    private final List<RebalanceNode> nodes;

    /**
     * Gets the Cruise Control REST API parameters passed through from {@code spec.config}.
     *
     * @return  Map of config parameters, or null if not set
     */
    public Map<String, String> getConfig() {
        return config;
    }

    /**
     * Gets the list of target nodes for the rebalance operation.
     *
     * @return  List of target nodes, or null if not set
     */
    public List<RebalanceNode> getNodes() {
        return nodes;
    }

    private RebalanceOptions(RebalanceOptionsBuilder builder) {
        this.config = builder.config;
        this.nodes = builder.nodes;
    }

    /**
     * Builder for RebalanceOptions
     */
    public static class RebalanceOptionsBuilder {
        private Map<String, String> config;
        private List<RebalanceNode> nodes;

        /**
         * Constructor
         */
        public RebalanceOptionsBuilder() {
            config = new HashMap<>();
            config.put(CruiseControlParameters.DRY_RUN.toString(), "true");
            config.put(CruiseControlParameters.VERBOSE.toString(), "false");
            nodes = null;
        }

        /**
         * Sets Cruise Control REST API parameters to be passed through as query parameters
         *
         * @param config     Map of config parameters
         *
         * @return  Instance of this builder
         */
        public RebalanceOptionsBuilder withConfig(Map<String, String> config) {
            this.config = config != null ? new HashMap<>(config) : null;
            return this;
        }

        /**
         * Adds a single Cruise Control REST API parameter to the config map
         *
         * @param key       Parameter key
         * @param value     Parameter value
         *
         * @return  Instance of this builder
         */
        public RebalanceOptionsBuilder withConfigEntry(String key, String value) {
            if (this.config == null) {
                this.config = new HashMap<>();
            }
            this.config.put(key, value);
            return this;
        }

        /**
         * Sets the target nodes for the rebalance operation
         *
         * @param nodes     List of target nodes
         *
         * @return  Instance of this builder
         */
        public RebalanceOptionsBuilder withNodes(List<RebalanceNode> nodes) {
            this.nodes = nodes;
            return this;
        }

        /**
         * Builds and returns the rebalance options.
         *
         * @return  The built RebalanceOptions instance
         */
        public RebalanceOptions build() {
            return new RebalanceOptions(this);
        }
    }
}
