/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;

/**
 * Concrete implementation of BaseMetricsComponent for Kafka general metrics.
 */
public class KafkaMetricsComponent extends BaseMetricsComponent {

    /**
     * Factory method to create a new instance of KafkaMetricsComponent.
     * @param componentName     the name of the component
     * @return                  a new instance of KafkaMetricsComponent
     */
    public static KafkaMetricsComponent create(final String componentName) {
        return new KafkaMetricsComponent(componentName);
    }

    /**
     * Private constructor to enforce the use of the factory method.
     */
    private KafkaMetricsComponent(String componentName) {
        super(null, componentName);
    }

    /**
     * Provides the label selector specific to Kafka.
     * @return LabelSelector for the Kafka deployment
     */
    @Override
    public LabelSelector getLabelSelector() {
        return KafkaResource.getLabelSelector(componentName, StrimziPodSetResource.getBrokerComponentName(componentName));
    }
}
