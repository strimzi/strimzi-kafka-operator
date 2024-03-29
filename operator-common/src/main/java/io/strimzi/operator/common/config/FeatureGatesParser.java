package io.strimzi.operator.common.config;

import io.strimzi.operator.common.InvalidConfigurationException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class FeatureGatesParser {
    private List<String> featureGates = Collections.emptyList();

    public FeatureGatesParser(String featureGateConfig) {
        if (featureGateConfig != null && !featureGateConfig.trim().isEmpty()) {
            if (featureGateConfig.matches("(\\s*[+-][a-zA-Z0-9]+\\s*,)*\\s*[+-][a-zA-Z0-9]+\\s*")) {
                featureGates = asList(featureGateConfig.trim().split("\\s*,+\\s*"));
            } else {
                throw new InvalidConfigurationException(featureGateConfig + " is not a valid feature gate configuration");
            }
        }
    }

    public void applyFor(Map<String, FeatureGate> possibleFeatureGates) {
        for (String featureGate : featureGates) {
            boolean value = '+' == featureGate.charAt(0);
            featureGate = featureGate.substring(1);

            if (possibleFeatureGates.containsKey(featureGate)) {
                setValueOnlyOnce(possibleFeatureGates.get(featureGate), value);
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
