/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.api.kafka.model.rebalance.RebalanceNode;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds Cruise Control rebalance request parameters: config entries become query parameters,
 * and nodes identify the target brokers/disks for the operation.
 */
public class RebalanceOptions {
    private final List<RebalanceNode> nodes;
    private final Map<String, String> config;

    /**
     * Gets the list of target nodes for the rebalance operation.
     *
     * @return  List of target nodes, or null if not set
     */
    public List<RebalanceNode> getNodes() {
        return nodes;
    }

    /**
     * Gets the Cruise Control REST API parameters passed through from {@code spec.config}.
     *
     * @return  Map of config parameters, or null if not set
     */
    public Map<String, String> getConfig() {
        return config;
    }

    private RebalanceOptions(RebalanceOptionsBuilder builder) {
        this.config = builder.config;
        this.nodes = builder.nodes;
    }

    /**
     * Builder for RebalanceOptions
     */
    public static class RebalanceOptionsBuilder {
        private List<RebalanceNode> nodes;
        private Map<String, String> config;

        /**
         * Constructor
         */
        public RebalanceOptionsBuilder() {
            nodes = null;
            config = new LinkedHashMap<>();
            config.put(CruiseControlParameters.DRY_RUN.toString(), "true");
            config.put(CruiseControlParameters.VERBOSE.toString(), "false");
            config.put(CruiseControlParameters.SKIP_HARD_GOAL_CHECK.toString(), "false");
        }

        /**
         * Sets Cruise Control REST API parameters to be passed through as query parameters
         *
         * @param config     Map of config parameters
         *
         * @return  Instance of this builder
         */
        public RebalanceOptionsBuilder withConfig(Map<String, String> config) {
            this.config = config != null ? new LinkedHashMap<>(config) : null;
            return this;
        }

        /**
         * Adds a single Cruise Control REST API parameter to the config
         *
         * @param key       Parameter key
         * @param value     Parameter value
         *
         * @return  Instance of this builder
         */
        public RebalanceOptionsBuilder withConfigEntry(String key, String value) {
            if (this.config == null) {
                this.config = new LinkedHashMap<>();
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
