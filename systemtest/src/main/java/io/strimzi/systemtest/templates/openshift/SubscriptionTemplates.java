/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.openshift;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionBuilder;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfiguration;
import io.strimzi.test.k8s.KubeClusterResource;

import java.util.Collections;
import java.util.Map;

public class SubscriptionTemplates {
    public static Subscription clusterOperatorSubscription(ClusterOperatorConfiguration clusterOperatorConfiguration) {
        SubscriptionBuilder subscriptionBuilder = new SubscriptionBuilder()
            .editOrNewMetadata()
                .withName("strimzi-sub")
                .withNamespace(clusterOperatorConfiguration.getNamespaceName())
                .withLabels(Collections.singletonMap("app", "strimzi"))
            .endMetadata()
            .editOrNewSpec()
                .withName(clusterOperatorConfiguration.getOlmOperatorName())
                .withSource(clusterOperatorConfiguration.getOlmSourceName())
                .withSourceNamespace(clusterOperatorConfiguration.getOlmSourceNamespace())
                .withChannel(clusterOperatorConfiguration.getOlmChannelName())
                .withInstallPlanApproval(clusterOperatorConfiguration.getOlmInstallationStrategy().toString())
                .editOrNewConfig()
                    .withEnv(clusterOperatorConfiguration.getAllEnvVariablesForOlm())
                .endConfig()
            .endSpec();

        if (clusterOperatorConfiguration.getOlmOperatorVersion() != null && !clusterOperatorConfiguration.getOlmOperatorVersion().isEmpty()) {
            subscriptionBuilder
                .editSpec()
                    .withStartingCSV(clusterOperatorConfiguration.getCsvName())
                .endSpec();
        }

        // Change default values for Cluster Operator memory when RESOURCE_ALLOCATION_STRATEGY is not set to NOT_SHARED
        if (KubeClusterResource.getInstance().fipsEnabled()) {
            ResourceRequirements resourceRequirements = new ResourceRequirementsBuilder()
                .withRequests(Map.of("memory", new Quantity(TestConstants.CO_REQUESTS_MEMORY), "cpu", new Quantity(
                    TestConstants.CO_REQUESTS_CPU)))
                .withLimits(Map.of("memory", new Quantity(TestConstants.CO_LIMITS_MEMORY), "cpu", new Quantity(
                    TestConstants.CO_LIMITS_CPU)))
                .build();

            subscriptionBuilder
                .editSpec()
                    .editOrNewConfig()
                        .withResources(resourceRequirements)
                    .endConfig()
                .endSpec();
        }

        return subscriptionBuilder.build();
    }
}
