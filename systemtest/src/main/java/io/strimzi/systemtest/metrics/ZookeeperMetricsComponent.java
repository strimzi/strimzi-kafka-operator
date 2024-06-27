/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.crd.KafkaResource;

/**
 * Concrete implementation of BaseMetricsComponent for Zookeeper.
 */
public class ZookeeperMetricsComponent extends BaseMetricsComponent {

    /**
     * Factory method to create a new instance of ZookeeperMetricsComponent.
     * @param componentName     the name of the component
     * @return                  a new instance of ZookeeperMetricsComponent
     */
    public static ZookeeperMetricsComponent create(final String componentName) {
        return new ZookeeperMetricsComponent(componentName);
    }

    /**
     * Private constructor to enforce the use of the factory method.
     */
    private ZookeeperMetricsComponent(String componentName) {
        super(null, componentName);
    }

    /**
     * Provides the default metrics port specifically for Zookeeper.
     * @return int representing the Zookeeper metrics port
     */
    @Override
    public int getDefaultMetricsPort() {
        return TestConstants.COMPONENTS_METRICS_PORT;
    }

    /**
     * Provides the label selector specific to Zookeeper.
     * @return LabelSelector for the Zookeeper deployment
     */
    @Override
    public LabelSelector getLabelSelector() {
        return KafkaResource.getLabelSelector(componentName, KafkaResources.zookeeperComponentName(componentName));
    }
}
