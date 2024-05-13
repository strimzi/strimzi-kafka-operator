/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.testframe.MetricsComponent;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.TestConstants;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

public class TopicOperatorMetricsComponent implements MetricsComponent {

    private String namespaceName;
    private String componentName;

    public static TopicOperatorMetricsComponent create(final String namespaceName, final String componentName) {
        return new TopicOperatorMetricsComponent(namespaceName, componentName);
    }

    private TopicOperatorMetricsComponent(String namespaceName, String componentName) {
        this.namespaceName = namespaceName;
        this.componentName = componentName;
    }

    @Override
    public int getDefaultMetricsPort() {
        return TestConstants.TOPIC_OPERATOR_METRICS_PORT;
    }

    @Override
    public String getDefaultMetricsPath() {
        return "/metrics";
    }

    @Override
    public LabelSelector getLabelSelector() {
        return kubeClient().getDeploymentSelectors(namespaceName, KafkaResources.entityOperatorDeploymentName(componentName));

    }
}
