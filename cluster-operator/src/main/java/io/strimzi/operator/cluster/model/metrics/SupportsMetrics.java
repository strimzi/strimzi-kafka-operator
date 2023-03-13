/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

/**
 * This interface is used for models which implement Metrics support
 */
public interface SupportsMetrics {
    /**
     * @return  Metrics model
     */
    MetricsModel metrics();
}
