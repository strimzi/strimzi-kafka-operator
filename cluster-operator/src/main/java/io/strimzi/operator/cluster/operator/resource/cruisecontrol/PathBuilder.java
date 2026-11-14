/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.api.kafka.model.rebalance.RebalanceNode;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Builds the path to the Cruise Control APi
 */
public class PathBuilder {

    String constructedPath;
    boolean firstParam;

    /**
     * Constructor
     *
     * @param endpoint  Cruise Control endpoint
     */
    public PathBuilder(CruiseControlEndpoints endpoint) {
        constructedPath = endpoint + "?";
        firstParam = true;
    }

    /**
     * Adds parameter with value to the path
     *
     * @param param     Cruise Control parameter
     * @param value     Parameter value
     *
     * @return  Instance of this builder
     */
    public PathBuilder withParameter(CruiseControlParameters param, String value) {
        if (!firstParam) {
            constructedPath += "&";
        } else {
            firstParam = false;
        }
        try {
            constructedPath += param.asPair(value);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
        return this;
    }

    /**
     * Adds parameter with multiple values to the path
     *
     * @param param     Cruise Control parameter
     * @param values    List of parameter value
     *
     * @return  Instance of this builder
     */
    public PathBuilder withParameter(CruiseControlParameters param, List<String> values) {
        if (!firstParam) {
            constructedPath += "&";
        } else {
            firstParam = false;
        }
        try {
            constructedPath += param.asList(values);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
        return this;
    }

    /**
     * Adds additional Cruise Control REST API parameters as raw key-value query parameters.
     *
     * @param config   Map of additional config parameters
     *
     * @return  Instance of this builder
     */
    public PathBuilder withAdditionalConfig(Map<String, String> config) {
        if (config != null) {
            for (Map.Entry<String, String> entry : config.entrySet()) {
                if (!firstParam) {
                    constructedPath += "&";
                } else {
                    firstParam = false;
                }
                constructedPath += URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)
                    + "="
                    + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8);
            }
        }
        return this;
    }

    /**
     * Adds Rebalance parameters to the path
     *
     * @param options   Rebalance options
     *
     * @return  Instance of this builder
     */
    public PathBuilder withRebalanceParameters(RebalanceOptions options) {
        if (options != null && options.getConfig() != null) {
            withAdditionalConfig(options.getConfig());
        }
        return this;
    }

    /**
     * Adds rebalance options and broker ID parameters for add-broker operations.
     *
     * @param options   Rebalance options containing node list
     *
     * @return  Instance of this builder
     */
    public PathBuilder withAddBrokerParameters(RebalanceOptions options) {
        if (options != null) {
            withRebalanceParameters(options)
                    .withParameter(CruiseControlParameters.BROKER_ID,
                            options.getNodes().stream().map(n -> String.valueOf(n.getBrokerId())).collect(Collectors.joining(",")));
        }
        return this;
    }

    /**
     * Adds rebalance options and broker ID parameters for remove-broker operations.
     *
     * @param options   Rebalance options containing node list
     *
     * @return  Instance of this builder
     */
    public PathBuilder withRemoveBrokerParameters(RebalanceOptions options) {
        if (options != null) {
            withRebalanceParameters(options)
                    .withParameter(CruiseControlParameters.BROKER_ID,
                            options.getNodes().stream().map(n -> String.valueOf(n.getBrokerId())).collect(Collectors.joining(",")));
        }
        return this;
    }

    /**
     * Adds rebalance options and broker-and-logdirs parameters for remove-disks operations.
     *
     * @param options   Rebalance options containing node list with volume IDs
     *
     * @return  Instance of this builder
     */
    public PathBuilder withRemoveDisksParameters(RebalanceOptions options) {
        if (options != null) {
            return withRebalanceParameters(options)
                    .withParameter(CruiseControlParameters.BROKER_ID_AND_LOG_DIRS, nodeParameterList(options.getNodes()));
        }
        return this;
    }

    private static List<String> nodeParameterList(List<RebalanceNode> nodes) {
        List<String> params = new ArrayList<>(nodes.size());
        for (RebalanceNode node : nodes) {
            node.getVolumeIds().forEach(volumeId -> params.add(node.getBrokerId() + "-/var/lib/kafka/data-" + volumeId + "/kafka-log" + node.getBrokerId()));
        }
        return params;
    }

    /**
     * Builds and returns the Cruise Control API path.
     *
     * @return  Builds and returns the path
     */
    public String build() {
        return constructedPath;
    }

}
