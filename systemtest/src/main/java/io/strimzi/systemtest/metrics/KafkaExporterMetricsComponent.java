/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

/**
 * Concrete implementation of BaseMetricsComponent for Kafka Exporter.
 */
public class KafkaExporterMetricsComponent extends BaseMetricsComponent {

    /**
     * Factory method to create a new instance of KafkaExporterMetricsComponent.
     * @param namespaceName     the namespace in which the component is deployed
     * @param componentName     the name of the component
     * @return                  a new instance of KafkaExporterMetricsComponent
     */
    public static KafkaExporterMetricsComponent create(final String namespaceName, final String componentName) {
        return new KafkaExporterMetricsComponent(namespaceName, componentName);
    }

    /**
     * Private constructor to enforce the use of the factory method.
     */
    private KafkaExporterMetricsComponent(String namespaceName, String componentName) {
        super(namespaceName, componentName);
    }

    /**
     * Provides the label selector specific to the Kafka Exporter.
     * @return LabelSelector for the Kafka Exporter deployment
     */
    @Override
    public LabelSelector getLabelSelector() {
        return kubeClient().getDeploymentSelectors(namespaceName, KafkaExporterResources.componentName(componentName));
    }
}
