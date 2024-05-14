/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.testframe.MetricsComponent;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;

public class KafkaConnectMetricsComponent implements MetricsComponent {

    private String componentName;

    public static KafkaConnectMetricsComponent create(final String componentName) {
        return new KafkaConnectMetricsComponent(componentName);
    }

    private KafkaConnectMetricsComponent(String componentName) {
        this.componentName = componentName;
    }

    @Override
    public int getDefaultMetricsPort() {
        return TestConstants.COMPONENTS_METRICS_PORT;
    }

    @Override
    public String getDefaultMetricsPath() {
        return "/metrics";
    }

    @Override
    public LabelSelector getLabelSelector() {
        return KafkaConnectResource.getLabelSelector(componentName, KafkaConnectResources.componentName(componentName));
    }
}
