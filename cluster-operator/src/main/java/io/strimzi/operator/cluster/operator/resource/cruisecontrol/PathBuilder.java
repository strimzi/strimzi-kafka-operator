/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.api.kafka.model.rebalance.RebalanceNode;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
        withParameter(CruiseControlParameters.JSON, "true");
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
        return withParameter(param.toString(), value);
    }

    /**
     * Adds Rebalance parameters to the path
     *
     * @param options   Rebalance options
     *
     * @return  Instance of this builder
     */
    public PathBuilder withParameters(RebalanceOptions options) {
        if (options != null && options.getConfig() != null) {
            options.getConfig().forEach(this::withParameter);
        }
        return this;
    }

    protected PathBuilder withParameter(String key, String value) {
        if (!firstParam) {
            constructedPath += "&";
        } else {
            firstParam = false;
        }
        constructedPath += URLEncoder.encode(key, StandardCharsets.UTF_8)
            + "="
            + URLEncoder.encode(value, StandardCharsets.UTF_8);
        return this;
    }

    /**
     * Adds the broker ID parameter from the node list.
     *
     * @param options   Rebalance options containing node list
     *
     * @return  Instance of this builder
     */
    public PathBuilder withBrokerIdParameter(RebalanceOptions options) {
        if (options != null && options.getNodes() != null) {
            withParameter(CruiseControlParameters.BROKER_ID,
                    options.getNodes().stream().map(n -> String.valueOf(n.getBrokerId())).collect(Collectors.joining(",")));
        }
        return this;
    }

    /**
     * Adds the broker ID and log dirs parameter from the node list.
     *
     * @param options   Rebalance options containing node list with volume IDs
     *
     * @return  Instance of this builder
     */
    public PathBuilder withBrokerIdAndLogDirParameter(RebalanceOptions options) {
        if (options != null && options.getNodes() != null) {
            withParameter(CruiseControlParameters.BROKER_ID_AND_LOG_DIRS.toString(), String.join(",", brokerIdAndLogDirs(options.getNodes())));
        }
        return this;
    }

    private static List<String> brokerIdAndLogDirs(List<RebalanceNode> nodes) {
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
