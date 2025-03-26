/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.skodjob.testframe.enums.InstallType;
import io.skodjob.testframe.installation.InstallationMethod;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.Environment;

/**
 * Class for handling the installation of ClusterOperator.
 * This is singleton class for easier handling across the tests.
 * Other than installation method encapsulations it contains few methods for obtaining the operator's deployment Namespace
 * and Deployment name.
 */
public class SetupClusterOperator {
    private ClusterOperatorConfiguration clusterOperatorConfiguration;
    private HelmInstallation helmInstallation;
    private OlmInstallation olmInstallation;
    private YamlInstallation yamlInstallation;

    /**
     * Private constructor to prevent external instantiation
     */
    private SetupClusterOperator() {
        this.clusterOperatorConfiguration = new ClusterOperatorConfiguration();
    }

    /**
     * Holder class containing the instance of the {@link SetupClusterOperator}
     */
    private static class Holder {
        private static final SetupClusterOperator INSTANCE = new SetupClusterOperator();
    }

    /**
     * Returns instance from the {@link Holder}.
     *
     * @return  one and only instance from the {@link Holder#INSTANCE}.
     */
    public static SetupClusterOperator getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Configures "default configuration" for the installation of ClusterOperator.
     * The default configuration contains values from the environment variables and constants.
     * This is used for most of the cases, otherwise we need to use the {@link #withCustomConfiguration(ClusterOperatorConfiguration)}
     * and create desired {@link ClusterOperatorConfiguration}.
     *
     * @return  the instance of the {@link SetupClusterOperator} class.
     */
    public SetupClusterOperator withDefaultConfiguration() {
        this.clusterOperatorConfiguration = new ClusterOperatorConfiguration();
        return this;
    }

    /**
     * Configures the customized {@link ClusterOperatorConfiguration}.
     * This is used in cases that we want to add more environment variables or labels to the ClusterOperator deployment, or if we
     * want to change values of already configured attributes for specific test or test class.
     *
     * @param clusterOperatorConfiguration  desired {@link ClusterOperatorConfiguration}
     *
     * @return  the instance of the {@link SetupClusterOperator} class.
     */
    public SetupClusterOperator withCustomConfiguration(ClusterOperatorConfiguration clusterOperatorConfiguration) {
        this.clusterOperatorConfiguration = clusterOperatorConfiguration;
        return this;
    }

    /**
     * Default installation method that will take the installation type from {@link #getInstallationMethodFromEnvVariable()}.
     */
    public void install() {
        install(getInstallationMethodFromEnvVariable());
    }

    /**
     * Installation method which creates the ClusterOperator Namespace (or recreates it) and installs the operator
     * based on installation method's {@link InstallationMethod#install()}.
     *
     * @param installationMethod    desired installation method containing the desired {@link ClusterOperatorConfiguration}
     */
    private void install(InstallationMethod installationMethod) {
        createClusterOperatorNamespace();
        installationMethod.install();
    }

    /**
     * Installation method specifically for OLM.
     * This is used in tests that require to run the installation using OLM.
     * The {@link #olmInstallation} is used for this case directly.
     */
    public void installUsingOlm() {
        olmInstallation = new OlmInstallation(clusterOperatorConfiguration);
        install(olmInstallation);
    }

    /**
     * Installation method specifically for Helm.
     * This is used in tests that require to run the installation using Helm.
     * The {@link #helmInstallation} is used for this case directly.
     */
    public void installUsingHelm() {
        helmInstallation = new HelmInstallation(clusterOperatorConfiguration);
        install(helmInstallation);
    }

    /**
     * Creates the ClusterOperator's Namespace based on {@link ClusterOperatorConfiguration#getNamespaceName()}.
     * In case that the Namespace already exists, the method firstly deletes the Namespace (in order to clean the environment)
     * and then creates it again.
     */
    public void createClusterOperatorNamespace() {
        if (KubeResourceManager.get().kubeClient().namespaceExists(clusterOperatorConfiguration.getNamespaceName())) {
            if (clusterOperatorConfiguration.shouldDeleteNamespace()) {
                Namespace coNamespace = KubeResourceManager.get().kubeClient().getClient().namespaces().withName(clusterOperatorConfiguration.getNamespaceName()).get();
                KubeResourceManager.get().deleteResourceWithWait(coNamespace);
            } else {
                // in case we are skipping the deletion, there is no need recreating the Namespace
                return;
            }
        }

        KubeResourceManager.get().createResourceWithWait(new NamespaceBuilder()
            .withNewMetadata()
                .withName(clusterOperatorConfiguration.getNamespaceName())
            .endMetadata()
            .build()
        );
    }

    /**
     * Returns Namespace where the ClusterOperator is installed.
     * That is done by returning the {@link ClusterOperatorConfiguration} first from the particular {@link InstallationMethod}.
     *
     * @return ClusterOperator's deployment Namespace
     */
    public String getOperatorNamespace() {
        return getClusterConfiguration().getNamespaceName();
    }

    /**
     * Returns name of the ClusterOperator Deployment.
     * That is done by returning the {@link ClusterOperatorConfiguration} first from the particular {@link InstallationMethod}.
     *
     * @return ClusterOperator's Deployment name
     */
    public String getOperatorDeploymentName() {
        return getClusterConfiguration().getOperatorDeploymentName();
    }

    /**
     * Returns the {@link ClusterOperatorConfiguration} specifically for the OLM installation type.
     * Used in tests where we know we installed the ClusterOperator via OLM and we want to have this exact configuration.
     * Otherwise, it can happen that the default installation type will be different (for example `Yaml`) and the check will fail.
     *
     * @return  {@link ClusterOperatorConfiguration} for OLM installation type - from {@link #olmInstallation}
     */
    public ClusterOperatorConfiguration getOlmClusterOperatorConfiguration() {
        return getClusterConfiguration(InstallType.Olm);
    }

    /**
     * Based on {@link Environment#CLUSTER_OPERATOR_INSTALL_TYPE} returns the {@link ClusterOperatorConfiguration} stored in
     * one of the {@link InstallationMethod}.
     *
     * @return  {@link ClusterOperatorConfiguration} for particular {@link InstallationMethod}
     */
    public ClusterOperatorConfiguration getClusterConfiguration() {
        return getClusterConfiguration(Environment.CLUSTER_OPERATOR_INSTALL_TYPE);
    }

    /**
     * Based on the {@param installType} parameter it goes through all the available installation methods and returns
     * the one's {@link ClusterOperatorConfiguration} stored inside this {@link InstallationMethod}.
     *
     * @param installType   desired installation type for which we want to acquire {@link ClusterOperatorConfiguration}
     *
     * @return  {@link ClusterOperatorConfiguration} for desired installation type
     */
    private ClusterOperatorConfiguration getClusterConfiguration(InstallType installType) {
        return switch (installType) {
            case Yaml -> yamlInstallation.getClusterOperatorConfiguration();
            case Helm -> helmInstallation.getClusterOperatorConfiguration();
            case Olm -> olmInstallation.getClusterOperatorConfiguration();
            case Unknown -> throw new RuntimeException("Unknown installation type");
        };
    }

    /**
     * Based on {@link Environment#CLUSTER_OPERATOR_INSTALL_TYPE} returns the particular {@link InstallationMethod}
     * together with newly configured {@link ClusterOperatorConfiguration}.
     *
     * @return  {@link InstallationMethod} based on the {@link Environment#CLUSTER_OPERATOR_INSTALL_TYPE}.
     */
    private InstallationMethod getInstallationMethodFromEnvVariable() {
        return switch (Environment.CLUSTER_OPERATOR_INSTALL_TYPE) {
            case Yaml -> {
                yamlInstallation = new YamlInstallation(clusterOperatorConfiguration);
                yield yamlInstallation;
            }
            case Helm -> {
                helmInstallation = new HelmInstallation(clusterOperatorConfiguration);
                yield helmInstallation;
            }
            case Olm -> {
                olmInstallation = new OlmInstallation(clusterOperatorConfiguration);
                yield olmInstallation;
            }
            case Unknown -> throw new RuntimeException("Unknown installation type");
        };
    }
}