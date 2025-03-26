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

/**
 * Class for handling the OLM installation.
 * Everything is done by resources and ResourceManager.
 */
public class OlmInstallation implements InstallationMethod {
    private ClusterOperatorConfiguration clusterOperatorConfiguration;

    /**
     * Constructor with specific {@link ClusterOperatorConfiguration}.
     *
     * @param clusterOperatorConfiguration  default or customized {@link ClusterOperatorConfiguration} which should be used
     *                                      for installation of the ClusterOperator via OLM
     */
    public OlmInstallation(ClusterOperatorConfiguration clusterOperatorConfiguration) {
        this.clusterOperatorConfiguration = clusterOperatorConfiguration;
    }

    /**
     * Returns the configured (and updated) {@link ClusterOperatorConfiguration}.
     * This is then used in {@link SetupClusterOperator#getClusterConfiguration()} (based on the installation method).
     *
     * @return  {@link ClusterOperatorConfiguration} configured and used during the installation
     */
    public ClusterOperatorConfiguration getClusterOperatorConfiguration() {
        return clusterOperatorConfiguration;
    }

    /**
     * Installation method for OLM install type.
     * It:
     *   - creates OperatorGroup
     *   - creates modified Subscription based on the {@link #clusterOperatorConfiguration}
     *   - in case of `Manual` installation strategy waits for creation of InstallPlan and approves it
     *   - waits for Deployment creation with prefix
     *   - stores the full Deployment name in the {@link #clusterOperatorConfiguration}
     *   - waits for Deployment readiness
     */
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

    /**
     * No implementation of deletion.
     * Everything is deployed using {@link KubeResourceManager}, meaning it is properly deleted at the end of the tests.
     * In case that this method is used, it throws the {@link UnsupportedOperationException} - as resource manager should be used.
     */
    @Override
    public void delete() {
        throw new UnsupportedOperationException("delete() should not be called directly; use KubeResourceManager instead.");
    }

    /**
     * Creates OperatorGroup in specific Namespace (from {@link ClusterOperatorConfiguration#getNamespaceName()}
     * and in case of different configuration of Namespaces to watch (different from `*`), it configures the Namespaces
     * to watch using {@link ClusterOperatorConfiguration#getNamespacesToWatch()}
     */
    private void createOperatorGroup() {
        OperatorGroupBuilder operatorGroup = new OperatorGroupBuilder()
            .editOrNewMetadata()
                .withName(TestConstants.OLM_OPERATOR_GROUP_NAME)
                .withNamespace(clusterOperatorConfiguration.getNamespaceName())
            .endMetadata();

        // single or multiple specific namespaces to watch
        if (!clusterOperatorConfiguration.isWatchingAllNamespaces()) {
            operatorGroup
                .editOrNewSpec()
                    .withTargetNamespaces(clusterOperatorConfiguration.getNamespacesToWatch())
                .endSpec();
        }

        KubeResourceManager.get().createResourceWithWait(operatorGroup.build());
    }

    /**
     * Creates Subscription with configuration from the {@link #clusterOperatorConfiguration}.
     */
    private void createAndModifySubscription() {
        Subscription subscription = SubscriptionTemplates.clusterOperatorSubscription(clusterOperatorConfiguration);

        KubeResourceManager.get().createResourceWithWait(subscription);
    }
}