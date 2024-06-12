/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.TestConstants;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

/**
 * Concrete implementation of BaseMetricsComponent for the Topic Operator.
 */
public class TopicOperatorMetricsComponent extends BaseMetricsComponent {

    /**
     * Factory method to create a new instance of TopicOperatorMetricsComponent.
     * @param namespaceName     the namespace in which the component is deployed
     * @param componentName     the name of the component
     * @return                  a new instance of TopicOperatorMetricsComponent
     */
    public static TopicOperatorMetricsComponent create(final String namespaceName, final String componentName) {
        return new TopicOperatorMetricsComponent(namespaceName, componentName);
    }

    /**
     * Private constructor to enforce the use of the factory method.
     */
    private TopicOperatorMetricsComponent(String namespaceName, String componentName) {
        super(namespaceName, componentName);
    }

    /**
     * Provides the default metrics port specifically for the Topic Operator.
     * @return int representing the Topic Operator metrics port
     */
    @Override
    public int getDefaultMetricsPort() {
        return TestConstants.TOPIC_OPERATOR_METRICS_PORT;
    }

    /**
     * Provides the label selector specific to the Topic Operator.
     * @return LabelSelector for the Topic Operator deployment
     */
    @Override
    public LabelSelector getLabelSelector() {
        return kubeClient().getDeploymentSelectors(namespaceName, KafkaResources.entityOperatorDeploymentName(componentName));
    }
}
