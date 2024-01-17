/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

/**
 * This interface is used for sections of our custom resources which support liveness probe configuration.
 */
public interface HasLivenessProbe {
    /**
     * Gets the liveness probe configuration
     *
     * @return  Liveness probe configuration
     */
    Probe getLivenessProbe();

    /**
     * Sets the liveness probe configuration
     *
     * @param livenessProbe     Liveness probe configuration
     */
    void setLivenessProbe(Probe livenessProbe);
}
