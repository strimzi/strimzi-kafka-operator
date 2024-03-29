/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.config;

import io.strimzi.operator.common.InvalidConfigurationException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * Parses String like "+FeatureA,-FeatureB" and sets the result in a map with possible feature gates.
 */
public class FeatureGatesParser {
    private List<String> featureGates = Collections.emptyList();

    /**
     * @param featureGateConfig String with all feature gates joined by ","
     */
    public FeatureGatesParser(String featureGateConfig) {
        if (featureGateConfig != null && !featureGateConfig.trim().isEmpty()) {
            if (featureGateConfig.matches("(\\s*[+-][a-zA-Z0-9]+\\s*,)*\\s*[+-][a-zA-Z0-9]+\\s*")) {
                featureGates = asList(featureGateConfig.trim().split("\\s*,+\\s*"));
            } else {
                throw new InvalidConfigurationException(featureGateConfig + " is not a valid feature gate configuration");
            }
        }
    }

    /**
     * @param possibleFeatureGateWithDefaultValues Map of possibe feature gates and its default values
     */
    public void applyFor(Map<String, FeatureGate> possibleFeatureGateWithDefaultValues) {
        for (String featureGate : featureGates) {
            boolean value = '+' == featureGate.charAt(0);
            featureGate = featureGate.substring(1);

            if (possibleFeatureGateWithDefaultValues.containsKey(featureGate)) {
                setValueOnlyOnce(possibleFeatureGateWithDefaultValues.get(featureGate), value);
            } else {
                throw new InvalidConfigurationException("Unknown feature gate " + featureGate + " found in the configuration");
            }
        }
    }

    /**
     * Sets the feature gate value if it was not set yet. But if it is already set, then it throws an exception. This
     * helps to ensure that each feature gate is configured always only once.
     *
     * @param gate  Feature gate which is being configured
     * @param value Value which should be set
     */
    private void setValueOnlyOnce(FeatureGate gate, boolean value) {
        if (gate.isSet()) {
            throw new InvalidConfigurationException("Feature gate " + gate.getName() + " is configured multiple times");
        }

        gate.setValue(value);
    }
}
