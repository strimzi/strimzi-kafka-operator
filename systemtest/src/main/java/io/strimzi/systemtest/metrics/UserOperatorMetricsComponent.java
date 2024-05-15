/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;

/**
 * Concrete implementation of BaseMetricsComponent for the User Operator.
 */
public class UserOperatorMetricsComponent extends BaseMetricsComponent {

    /**
     * Factory method to create a new instance of UserOperatorMetricsComponent.
     * @param namespaceName     the namespace in which the component is deployed
     * @param componentName     the name of the component
     * @return                  a new instance of UserOperatorMetricsComponent
     */
    public static UserOperatorMetricsComponent create(final String namespaceName, final String componentName) {
        return new UserOperatorMetricsComponent(namespaceName, componentName);
    }

    /**
     * Private constructor to enforce the use of the factory method.
     */
    private UserOperatorMetricsComponent(String namespaceName, String componentName) {
        super(namespaceName, componentName);
    }

    /**
     * Provides the default metrics port specifically for the User Operator.
     * @return int representing the User Operator metrics port
     */
    @Override
    public int getDefaultMetricsPort() {
        return TestConstants.USER_OPERATOR_METRICS_PORT;
    }

    /**
     * Provides the label selector specific to the User Operator.
     * @return LabelSelector for the User Operator deployment
     */
    @Override
    public LabelSelector getLabelSelector() {
        return ResourceManager.kubeClient().getDeploymentSelectors(namespaceName, KafkaResources.entityOperatorDeploymentName(componentName));
    }
}
