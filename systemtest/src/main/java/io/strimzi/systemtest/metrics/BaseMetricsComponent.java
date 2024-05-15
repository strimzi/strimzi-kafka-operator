/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.testframe.MetricsComponent;
import io.strimzi.systemtest.TestConstants;

/**
 * Abstract base class for all metrics components in the system tests.
 * Provides common methods and fields that are used across different types of metrics components.
 */
public abstract class BaseMetricsComponent implements MetricsComponent {

    protected String namespaceName;
    protected String componentName;

    /**
     * Constructor for BaseMetricsComponent initializing the namespace and component names.
     * @param namespaceName the namespace in which the component is deployed
     * @param componentName the name of the component
     */
    protected BaseMetricsComponent(String namespaceName, String componentName) {
        this.namespaceName = namespaceName;
        this.componentName = componentName;
    }

    /**
     * Provides the default metrics port number for the component.
     * @return int representing the default metrics port
     */
    @Override
    public int getDefaultMetricsPort() {
        return TestConstants.COMPONENTS_METRICS_PORT;
    }

    /**
     * Provides the default path for accessing metrics.
     * @return String representing the metrics path
     */
    @Override
    public String getDefaultMetricsPath() {
        return "/metrics";
    }

    /**
     * Abstract method to provide the label selector for Kubernetes resources,
     * to be implemented by specific component classes.
     * @return LabelSelector used to select the Kubernetes resources
     */
    @Override
    public abstract LabelSelector getLabelSelector();
}
