/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.testframe.MetricsComponent;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.systemtest.TestConstants;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaBridgeMetricsComponent implements MetricsComponent {

    private String namespaceName;
    private String componentName;

    public static KafkaBridgeMetricsComponent create(final String namespaceName, final String componentName) {
        return new KafkaBridgeMetricsComponent(namespaceName, componentName);
    }

    private KafkaBridgeMetricsComponent(String namespaceName, String componentName) {
        this.namespaceName = namespaceName;
        this.componentName = componentName;
    }

    @Override
    public int getDefaultMetricsPort() {
        return TestConstants.KAFKA_BRIDGE_METRICS_PORT;
    }

    @Override
    public String getDefaultMetricsPath() {
        return "/metrics";
    }

    @Override
    public LabelSelector getLabelSelector() {
        return kubeClient().getDeploymentSelectors(namespaceName, KafkaBridgeResources.componentName(componentName));
    }
}
