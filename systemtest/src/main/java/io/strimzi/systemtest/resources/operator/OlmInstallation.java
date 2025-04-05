/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator;

import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.skodjob.testframe.installation.InstallationMethod;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.OlmInstallationStrategy;
import io.strimzi.systemtest.templates.openshift.SubscriptionTemplates;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.OlmUtils;

public class OlmInstallation implements InstallationMethod {
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

        String clusterOperatorDepName = KubeResourceManager.get().kubeClient().getDeploymentNameByPrefix(clusterOperatorConfiguration.getNamespaceName(), clusterOperatorConfiguration.getOlmOperatorDeploymentNamePrefix());
        clusterOperatorConfiguration = new ClusterOperatorConfigurationBuilder(clusterOperatorConfiguration)
            .withOperatorDeploymentName(clusterOperatorDepName)
            .build();

        // Wait for operator creation
        DeploymentUtils.waitForDeploymentAndPodsReady(clusterOperatorConfiguration.getNamespaceName(), clusterOperatorConfiguration.getOperatorDeploymentName(), 1);
    }

    @Override
    public void delete() {
        throw new UnsupportedOperationException("delete() should not be called directly; use KubeResourceManager instead.");
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
        Subscription subscription = SubscriptionTemplates.clusterOperatorSubscription(clusterOperatorConfiguration);

        KubeResourceManager.get().createResourceWithWait(subscription);
    }
}