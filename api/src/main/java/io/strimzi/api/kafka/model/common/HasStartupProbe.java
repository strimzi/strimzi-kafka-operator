/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

/**
 * This interface is used for sections of our custom resources which support startup probe configuration.
 */
public interface HasStartupProbe {
    /**
     * Gets the startup probe configuration
     *
     * @return  Startup probe configuration
     */
    Probe getStartupProbe();

    /**
     * Sets the startup probe configuration
     *
     * @param startupProbe    Startup probe configuration
     */
    void setStartupProbe(Probe startupProbe);
}
