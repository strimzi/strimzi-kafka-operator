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

public class SetupClusterOperator {

    private ClusterOperatorConfiguration clusterOperatorConfiguration = new ClusterOperatorConfiguration();

    private HelmInstallation helmInstallation;
    private OlmInstallation olmInstallation;
    private BundleInstallation bundleInstallation;

    public SetupClusterOperator withDefaultConfiguration() {
        this.clusterOperatorConfiguration = new ClusterOperatorConfiguration();
        return this;
    }

    public SetupClusterOperator withCustomConfiguration(ClusterOperatorConfiguration clusterOperatorConfiguration) {
        this.clusterOperatorConfiguration = clusterOperatorConfiguration;
        return this;
    }

    public void install() {
        install(getInstallationMethodFromEnvVariable());
    }

    private void install(InstallationMethod installationMethod) {
        createClusterOperatorNamespace();
        installationMethod.install();
    }

    public void installUsingOlm() {
        olmInstallation = new OlmInstallation(clusterOperatorConfiguration);
        install(olmInstallation);
    }

    public void installUsingHelm() {
        helmInstallation = new HelmInstallation(clusterOperatorConfiguration);
        install(helmInstallation);
    }

    public void createClusterOperatorNamespace() {
        if (KubeResourceManager.get().kubeClient().namespaceExists(clusterOperatorConfiguration.getNamespaceName())) {
            Namespace coNamespace = KubeResourceManager.get().kubeClient().getClient().namespaces().withName(clusterOperatorConfiguration.getNamespaceName()).get();
            KubeResourceManager.get().deleteResource(coNamespace);
        }

        KubeResourceManager.get().createResourceWithWait(new NamespaceBuilder()
            .withNewMetadata()
                .withName(clusterOperatorConfiguration.getNamespaceName())
            .endMetadata()
            .build()
        );
    }

    public String getOperatorNamespace() {
        return getClusterConfiguration().getNamespaceName();
    }

    public String getOperatorDeploymentName() {
        return getClusterConfiguration().getOperatorDeploymentName();
    }

    public ClusterOperatorConfiguration getOlmClusterOperatorConfiguration() {
        return getClusterConfiguration(InstallType.Olm);
    }

    public ClusterOperatorConfiguration getClusterConfiguration() {
        return getClusterConfiguration(Environment.CLUSTER_OPERATOR_INSTALL_TYPE);
    }

    private ClusterOperatorConfiguration getClusterConfiguration(InstallType installType) {
        return switch (installType) {
            case Yaml -> bundleInstallation.getClusterOperatorConfiguration();
            case Helm -> helmInstallation.getClusterOperatorConfiguration();
            case Olm -> olmInstallation.getClusterOperatorConfiguration();
            case Unknown -> throw new RuntimeException("Unknown installation type");
        };
    }

    private InstallationMethod getInstallationMethodFromEnvVariable() {
        return switch (Environment.CLUSTER_OPERATOR_INSTALL_TYPE) {
            case Yaml -> {
                bundleInstallation = new BundleInstallation(clusterOperatorConfiguration);
                yield bundleInstallation;
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