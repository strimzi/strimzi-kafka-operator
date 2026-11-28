/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.extrametrics.ExtraMetricsResources;

/**
 * Concrete implementation of BaseMetricsComponent for Extra Metrics.
 */
public class ExtraMetricsMetricsComponent extends BaseMetricsComponent {

    /**
     * Factory method to create a new instance of ExtraMetricsMetricsComponent.
     * @param namespaceName     the namespace in which the component is deployed
     * @param componentName     the name of the component
     * @return                  a new instance of ExtraMetricsMetricsComponent
     */
    public static ExtraMetricsMetricsComponent create(final String namespaceName, final String componentName) {
        return new ExtraMetricsMetricsComponent(namespaceName, componentName);
    }

    /**
     * Private constructor to enforce the use of the factory method.
     */
    private ExtraMetricsMetricsComponent(String namespaceName, String componentName) {
        super(namespaceName, componentName);
    }

    /**
     * Provides the label selector specific to Extra Metrics.
     * @return LabelSelector for the Extra Metrics deployment
     */
    @Override
    public LabelSelector getLabelSelector() {
        return KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(namespaceName).withName(ExtraMetricsResources.componentName(componentName)).get().getSpec().getSelector();
    }
}
