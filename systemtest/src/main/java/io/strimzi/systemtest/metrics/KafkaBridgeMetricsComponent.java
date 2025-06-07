/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.systemtest.TestConstants;

/**
 * Concrete implementation of BaseMetricsComponent for the Kafka Bridge.
 */
public class KafkaBridgeMetricsComponent extends BaseMetricsComponent {

    /**
     * Factory method to create a new instance of KafkaBridgeMetricsComponent.
     * @param namespaceName     the namespace in which the component is deployed
     * @param componentName     the name of the component
     * @return                  a new instance of KafkaBridgeMetricsComponent
     */
    public static KafkaBridgeMetricsComponent create(final String namespaceName, final String componentName) {
        return new KafkaBridgeMetricsComponent(namespaceName, componentName);
    }

    /**
     * Private constructor to enforce the use of the factory method.
     */
    private KafkaBridgeMetricsComponent(String namespaceName, String componentName) {
        super(namespaceName, componentName);
    }

    /**
     * Provides the default metrics port specifically for the Kafka Bridge.
     * @return int representing the Kafka Bridge metrics port
     */
    @Override
    public int getDefaultMetricsPort() {
        return TestConstants.KAFKA_BRIDGE_METRICS_PORT;
    }

    /**
     * Provides the label selector specific to the Kafka Bridge.
     * @return LabelSelector for the Kafka Bridge deployment
     */
    @Override
    public LabelSelector getLabelSelector() {
        return KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(namespaceName).withName(KafkaBridgeResources.componentName(componentName)).get().getSpec().getSelector();
    }
}
