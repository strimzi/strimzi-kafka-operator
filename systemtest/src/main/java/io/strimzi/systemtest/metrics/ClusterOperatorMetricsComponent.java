/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.testframe.MetricsComponent;
import io.strimzi.systemtest.TestConstants;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class ClusterOperatorMetricsComponent implements MetricsComponent {

    private String namespaceName;
    private String componentName;

    public static ClusterOperatorMetricsComponent create(final String namespaceName, final String componentName) {
        return new ClusterOperatorMetricsComponent(namespaceName, componentName);
    }

    private ClusterOperatorMetricsComponent(String namespaceName, String componentName) {
        this.namespaceName = namespaceName;
        this.componentName = componentName;
    }

    @Override
    public int getDefaultMetricsPort() {
        return TestConstants.CLUSTER_OPERATOR_METRICS_PORT;
    }

    @Override
    public String getDefaultMetricsPath() {
        return "/metrics";
    }

    @Override
    public LabelSelector getLabelSelector() {
        return kubeClient().getDeploymentSelectors(namespaceName, componentName);
    }
}
