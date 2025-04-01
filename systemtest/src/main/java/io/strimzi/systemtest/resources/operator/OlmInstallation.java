/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionBuilder;
import io.skodjob.testframe.installation.InstallationMethod;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.OlmInstallationStrategy;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.OlmUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class OlmInstallation implements InstallationMethod {
    private static final Logger LOGGER = LogManager.getLogger(OlmInstallation.class);

    private ClusterOperatorConfiguration clusterOperatorConfiguration;

    public OlmInstallation(ClusterOperatorConfiguration clusterOperatorConfiguration) {
        this.clusterOperatorConfiguration = clusterOperatorConfiguration;
    }

    public ClusterOperatorConfiguration getClusterOperatorConfiguration() {
        return clusterOperatorConfiguration;
    }

    @Override
    public void install() {
        // Create operator group
        createOperatorGroup();

        // Create subscription
        createAndModifySubscription();

        // Manual installation needs to be approved with a patch
        if (clusterOperatorConfiguration.getOlmInstallationStrategy() == OlmInstallationStrategy.Manual) {
            OlmUtils.waitForNonApprovedInstallPlanWithCsvNameOrPrefix(clusterOperatorConfiguration.getNamespaceName(), clusterOperatorConfiguration.getCsvName());
            OlmUtils.approveNonApprovedInstallPlan(clusterOperatorConfiguration.getNamespaceName(), clusterOperatorConfiguration.getCsvName());
        }

        // Make sure that operator will be created
        DeploymentUtils.waitForCreationOfDeploymentWithPrefix(clusterOperatorConfiguration.getNamespaceName(), clusterOperatorConfiguration.getOlmOperatorDeploymentNamePrefix());

        String clusterOperatorDepName = kubeClient().getDeploymentNameByPrefix(clusterOperatorConfiguration.getNamespaceName(), clusterOperatorConfiguration.getOlmOperatorDeploymentNamePrefix());
        clusterOperatorConfiguration = new ClusterOperatorConfigurationBuilder(clusterOperatorConfiguration)
            .withOperatorDeploymentName(clusterOperatorDepName)
            .build();

        // Wait for operator creation
        DeploymentUtils.waitForDeploymentAndPodsReady(clusterOperatorConfiguration.getNamespaceName(), clusterOperatorConfiguration.getOperatorDeploymentName(), 1);
    }

    @Override
    public void delete() {

    }

    /**
     * Creates OperatorGroup in specific namespace
     */
    private void createOperatorGroup() {
        OperatorGroupBuilder operatorGroup = new OperatorGroupBuilder()
            .editOrNewMetadata()
            .withName(TestConstants.OLM_OPERATOR_GROUP_NAME)
            .withNamespace(clusterOperatorConfiguration.getNamespaceName())
            .endMetadata();

        // single or multiple specific namespaces to watch
        if (!clusterOperatorConfiguration.getNamespacesToWatch().equals(TestConstants.WATCH_ALL_NAMESPACES)) {
            operatorGroup
                .editOrNewSpec()
                    .withTargetNamespaces(clusterOperatorConfiguration.getNamespacesToWatch())
                .endSpec();
        }

        KubeResourceManager.get().createResourceWithWait(operatorGroup.build());
    }

    /**
     * Creates Subscription with spec from OlmConfiguration
     */
    private void createAndModifySubscription() {
        Subscription subscription = prepareSubscription();

        KubeResourceManager.get().createResourceWithWait(subscription);
    }

    public Subscription prepareSubscription() {
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