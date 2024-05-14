/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.testframe.MetricsComponent;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.crd.KafkaResource;

public class ZookeeperMetricsComponent implements MetricsComponent {

    private String componentName;

    public static ZookeeperMetricsComponent create(final String componentName) {
        return new ZookeeperMetricsComponent(componentName);
    }

    private ZookeeperMetricsComponent(String componentName) {
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
        return KafkaResource.getLabelSelector(componentName, KafkaResources.zookeeperComponentName(componentName));
    }
}
