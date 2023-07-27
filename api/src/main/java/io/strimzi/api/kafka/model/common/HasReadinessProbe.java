/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

/**
 * This interface is used for sections of our custom resources which support readiness probe configuration.
 */
public interface HasReadinessProbe {
    /**
     * Gets the readiness probe configuration
     *
     * @return  Readiness probe configuration
     */
    Probe getReadinessProbe();

    /**
     * Sets the readiness probe configuration
     *
     * @param readinessProbe    Readiness probe configuration
     */
    void setReadinessProbe(Probe readinessProbe);
}
