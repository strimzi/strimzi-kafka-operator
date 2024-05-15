/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;

/**
 * Concrete implementation of BaseMetricsComponent for Kafka Connect.
 */
public class KafkaConnectMetricsComponent extends BaseMetricsComponent {

    /**
     * Factory method to create a new instance of KafkaConnectMetricsComponent.
     * @param componentName the name of the component
     * @return a new instance of KafkaConnectMetricsComponent
     */
    public static KafkaConnectMetricsComponent create(final String componentName) {
        return new KafkaConnectMetricsComponent(componentName);
    }

    /**
     * Private constructor to enforce the use of the factory method.
     */
    private KafkaConnectMetricsComponent(String componentName) {
        super(null, componentName);
    }

    /**
     * Provides the label selector specific to Kafka Connect.
     * @return LabelSelector for the Kafka Connect deployment
     */
    @Override
    public LabelSelector getLabelSelector() {
        return KafkaConnectResource.getLabelSelector(componentName, KafkaConnectResources.componentName(componentName));
    }
}
