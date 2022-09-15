/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.tracing.agent;

/**
 * Define an interface for initializing a distributed tracing system
 */
public interface Tracing {

    /**
     * Specific distributed tracing system initialization
     */
    void initialize();

    /**
     * Implementation for having no distributed tracing
     */
    class NoTracing implements Tracing {

        @Override
        public void initialize() {
            // no distributed tracing enabled
        }
    }
}
