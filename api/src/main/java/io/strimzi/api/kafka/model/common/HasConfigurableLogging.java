/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

/**
 * This interface is used for sections of our custom resources which support configurable logging.
 */
public interface HasConfigurableLogging {
    /**
     * Gets the logging configuration
     *
     * @return  Logging configuration
     */
    Logging getLogging();

    /**
     * Sets the logging configuration
     *
     * @param logging   Logging configuration
     */
    void setLogging(Logging logging);
}
