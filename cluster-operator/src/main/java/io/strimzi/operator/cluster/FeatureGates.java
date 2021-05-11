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
    private static final String CONTROL_PLANE_LISTENER = "ControlPlaneListener";
    private static final String NETWORK_POLICY_GENERATION = "NetworkPolicyGeneration";

    private final FeatureGate controlPlaneListener = new FeatureGate(CONTROL_PLANE_LISTENER, false);
    private final FeatureGate networkPolicyGeneration = new FeatureGate(NETWORK_POLICY_GENERATION, true);

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
                    case CONTROL_PLANE_LISTENER:
                        setValueOnlyOnce(controlPlaneListener, value);
                        break;
                    case NETWORK_POLICY_GENERATION:
                        setValueOnlyOnce(networkPolicyGeneration, value);
                        break;
                    default:
                        throw new InvalidConfigurationException("Unknown feature gate " + featureGate + " found in the configuration");
                }
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

    /**
     * @return  Returns true when the ControlPlaneListener feature gate is enabled
     */
    public boolean controlPlaneListenerEnabled() {
        return controlPlaneListener.isEnabled();
    }

    /**
     * @return  Returns true when the NetworkPolicyGeneration feature gate is enabled
     */
    public boolean networkPolicyGenerationEnabled() {
        return networkPolicyGeneration.isEnabled();
    }

    @Override
    public String toString() {
        return "FeatureGates(" +
                "controlPlaneListener=" + controlPlaneListener.isEnabled() + "," +
                "networkPolicyGeneration=" + networkPolicyGeneration.isEnabled() +
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
    }
}
