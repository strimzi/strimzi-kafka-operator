/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.gather.collectors;

import io.skodjob.testframe.MetricsComponent;
import io.strimzi.systemtest.performance.PerformanceConstants;

import java.util.List;

/**
 * Extends {@link BaseMetricsCollector} to specifically collect metrics related to the User Operator within the Strimzi Kafka environment.
 * This class provides functionality to gather detailed metrics concerning user reconciliations, which are crucial for monitoring and
 * analyzing the performance of the User Operator.
 *
 * The {@link UserOperatorMetricsCollector} class is designed to work with metrics specific to Kafka user operations, enabling targeted
 * data gathering that facilitates in-depth performance analysis and troubleshooting. It leverages a builder pattern for flexible configuration,
 * allowing it to be precisely tailored to the needs of specific test setups or operational environments.
 *
 * Metrics collection can be configured to target specific namespaces, pod names, and components, ensuring that the metrics gathered
 * are relevant and useful for the context in which the User Operator is operating.
 */
public class UserOperatorMetricsCollector extends BaseMetricsCollector {

    /**
     * Constructs a {@code UserOperatorMetricsCollector} with the specified builder configuration.
     * @param builder   The builder used to configure this collector.
     */
    protected UserOperatorMetricsCollector(BaseMetricsCollector.Builder builder) {
        super(builder);
    }

    // -----------------------------------------------------------------------------------------------------
    // --------------------------------- RECONCILIATIONS METRICS -------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getReconciliationsTotal(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_TOTAL, selector);
    }

    /**
     * Builder class for {@link UserOperatorMetricsCollector}. This class follows the builder pattern to allow for
     * flexible configuration of the {@link UserOperatorMetricsCollector}.
     */
    public static class Builder extends BaseMetricsCollector.Builder {
        @Override
        public UserOperatorMetricsCollector build() {
            return new UserOperatorMetricsCollector(this);
        }

        // Override the builder methods to return the type of this Builder, allowing method chaining
        @Override
        public UserOperatorMetricsCollector.Builder withNamespaceName(String namespaceName) {
            super.withNamespaceName(namespaceName);
            return this;
        }

        @Override
        public UserOperatorMetricsCollector.Builder withScraperPodName(String scraperPodName) {
            super.withScraperPodName(scraperPodName);
            return this;
        }

        @Override
        public UserOperatorMetricsCollector.Builder withComponent(MetricsComponent component) {
            super.withComponent(component);
            return this;
        }
    }
}
