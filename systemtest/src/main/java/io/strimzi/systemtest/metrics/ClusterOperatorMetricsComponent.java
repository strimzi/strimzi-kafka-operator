/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.systemtest.TestConstants;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

/**
 * Concrete implementation of BaseMetricsComponent for the Cluster Operator.
 */
public class ClusterOperatorMetricsComponent extends BaseMetricsComponent {

    /**
     * Factory method to create a new instance of ClusterOperatorMetricsComponent.
     * @param namespaceName     the namespace in which the component is deployed
     * @param componentName     the name of the component
     * @return                  a new instance of ClusterOperatorMetricsComponent
     */
    public static ClusterOperatorMetricsComponent   create(final String namespaceName, final String componentName) {
        return new ClusterOperatorMetricsComponent(namespaceName, componentName);
    }

    /**
     * Private constructor to enforce the use of the factory method.
     */
    private ClusterOperatorMetricsComponent(String namespaceName, String componentName) {
        super(namespaceName, componentName);
    }

    /**
     * Provides the label selector specific to the Cluster Operator.
     * @return LabelSelector for the Cluster Operator deployment
     */
    @Override
    public LabelSelector getLabelSelector() {
        return kubeClient().getDeploymentSelectors(namespaceName, componentName);
    }

    @Override
    public int getDefaultMetricsPort() {
        return TestConstants.CLUSTER_OPERATOR_METRICS_PORT;
    }
}
