/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.strimzi.operator.common.InvalidConfigurationException;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * Class for handling the configuration of feature gates
 */
public class FeatureGates {
    /* test */ static final FeatureGates NONE = new FeatureGates("");

    private static final String USE_KRAFT = "UseKRaft";

    // When adding new feature gates, do not forget to add them to allFeatureGates() and toString() methods
    private final FeatureGate useKRaft = new FeatureGate(USE_KRAFT, true);

    /**
     * Constructs the feature gates configuration.
     *
     * @param featureGateConfig String with comma separated list of enabled or disabled feature gates
     */
    public FeatureGates(String featureGateConfig) {
        if (featureGateConfig != null && !featureGateConfig.trim().isEmpty()) {
            List<String> featureGates;

            if (featureGateConfig.matches("(\\s*[+-][a-zA-Z0-9]+\\s*,)*\\s*[+-][a-zA-Z0-9]+\\s*")) {
                featureGates = asList(featureGateConfig.trim().split("\\s*,+\\s*"));
            } else {
                throw new InvalidConfigurationException(featureGateConfig + " is not a valid feature gate configuration");
            }

            for (String featureGate : featureGates) {
                boolean value = '+' == featureGate.charAt(0);
                featureGate = featureGate.substring(1);

                switch (featureGate) {
                    case USE_KRAFT:
                        setValueOnlyOnce(useKRaft, value);
                        break;
                    default:
                        throw new InvalidConfigurationException("Unknown feature gate " + featureGate + " found in the configuration");
                }
            }

            validateInterDependencies();
        }
    }

    /**
     * Validates any dependencies between various feature gates. When the dependencies are not satisfied,
     * InvalidConfigurationException is thrown.
     */
    private void validateInterDependencies()    {
        // There are currently no interdependencies between different feature gates.
        // But we keep this method as these might happen again in the future.
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

    /**
     * @return  Returns true when the UseKRaft feature gate is enabled
     */
    public boolean useKRaftEnabled() {
        return useKRaft.isEnabled();
    }

    /**
     * Returns a list of all Feature gates. Used for testing.
     *
     * @return  List of all Feature Gates
     */
    /*test*/ List<FeatureGate> allFeatureGates()  {
        return List.of(
                useKRaft
        );
    }

    @Override
    public String toString() {
        return "FeatureGates(" +
                "UseKRaft=" + useKRaft.isEnabled() +
                ")";
    }

    /**
     * Feature gate class represents individual feature fate
     */
    static class FeatureGate {
        private final String name;
        private final boolean defaultValue;
        private Boolean value = null;

        /**
         * Feature fate constructor
         *
         * @param name          Name of the feature gate
         * @param defaultValue  Default value of the feature gate
         */
        FeatureGate(String name, boolean defaultValue) {
            this.name = name;
            this.defaultValue = defaultValue;
        }

        /**
         * @return  The name of the feature gate
         */
        public String getName() {
            return name;
        }

        /**
         * @return  Returns true if the value for this feature gate is already set or false if it is still null
         */
        public boolean isSet() {
            return value != null;
        }

        /**
         * Sets the value of the feature gate
         *
         * @param value Value of the feature gate
         */
        public void setValue(boolean value) {
            this.value = value;
        }

        /**
         * @return  True if the feature gate is enabled. False otherwise.
         */
        public boolean isEnabled() {
            return value == null ? defaultValue : value;
        }

        /**
         * @return  Returns True if this feature gate is enabled by default. False otherwise.
         */
        public boolean isEnabledByDefault() {
            return defaultValue;
        }
    }
}
