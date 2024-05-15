/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;

/**
 * Concrete implementation of BaseMetricsComponent for Kafka MirrorMaker 2.
 */
public class KafkaMirrorMaker2MetricsComponent extends BaseMetricsComponent {

    /**
     * Factory method to create a new instance of KafkaMirrorMaker2MetricsComponent.
     * @param componentName     the name of the component
     * @return                  a new instance of KafkaMirrorMaker2MetricsComponent
     */
    public static KafkaMirrorMaker2MetricsComponent create(final String componentName) {
        return new KafkaMirrorMaker2MetricsComponent(componentName);
    }

    /**
     * Private constructor to enforce the use of the factory method.
     */
    private KafkaMirrorMaker2MetricsComponent(String componentName) {
        super(null, componentName);
    }

    /**
     * Provides the label selector specific to Kafka MirrorMaker 2.
     * @return LabelSelector for the Kafka MirrorMaker 2 deployment
     */
    @Override
    public LabelSelector getLabelSelector() {
        return KafkaMirrorMaker2Resource.getLabelSelector(componentName, KafkaMirrorMaker2Resources.componentName(componentName));
    }
}
