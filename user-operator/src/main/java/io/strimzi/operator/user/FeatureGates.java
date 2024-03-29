/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.strimzi.operator.common.config.FeatureGate;
import io.strimzi.operator.common.config.FeatureGatesParser;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class for handling the configuration of feature gates
 */
public class FeatureGates {
    private static final String USE_SERVER_SIDE_APPLY = "UseServerSideApply";

    private final Map<String, FeatureGate> featureGates = Map.ofEntries(
            Map.entry(USE_SERVER_SIDE_APPLY, new FeatureGate(USE_SERVER_SIDE_APPLY, false))
    );

    /**
     * Constructs the feature gates configuration.
     *
     * @param featureGateConfig String with comma separated list of enabled or disabled feature gates
     */
    public FeatureGates(String featureGateConfig) {
        new FeatureGatesParser(featureGateConfig).applyFor(featureGates);
    }

    /**
     * @return  Returns true when the UseServerSideApply feature gate is enabled
     */
    public boolean useServerSideApply() {
        return featureGates.get(USE_SERVER_SIDE_APPLY).isEnabled();
    }

    @Override
    public String toString() {
        String featureGatesValues = featureGates.entrySet()
                .stream()
                .map(featureGate -> featureGate.getKey() + "=" + featureGate.getValue().isEnabled())
                .collect(Collectors.joining(","));
        return "FeatureGates(%s)".formatted(featureGatesValues);
    }
}
