/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.config;

/**
 * Feature gate class represents individual feature fate
 */
public class FeatureGate {
    private final String name;
    private final boolean defaultValue;
    private Boolean value = null;

    /**
     * Feature fate constructor
     *
     * @param name          Name of the feature gate
     * @param defaultValue  Default value of the feature gate
     */
    public FeatureGate(String name, boolean defaultValue) {
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