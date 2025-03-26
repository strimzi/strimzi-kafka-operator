/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator;

import com.marcnuri.helm.Helm;
import com.marcnuri.helm.InstallCommand;
import com.marcnuri.helm.UninstallCommand;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.skodjob.testframe.installation.InstallationMethod;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.resources.ResourceItem;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.specific.BridgeUtils;
import io.strimzi.test.TestUtils;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.regex.Matcher;

/**
 * Class for handling the Helm installation via the Helm client.
 */
public class HelmInstallation implements InstallationMethod {
    public static final String HELM_CHART = TestUtils.USER_PATH + "/../packaging/helm-charts/helm3/strimzi-kafka-operator/";
    public static final String HELM_RELEASE_NAME = "strimzi-systemtests";

    private final ClusterOperatorConfiguration clusterOperatorConfiguration;

    /**
     * Constructor with specific {@link ClusterOperatorConfiguration}.
     *
     * @param clusterOperatorConfiguration  default or customized {@link ClusterOperatorConfiguration} which should be used
     *                                      for installation of the ClusterOperator via Helm
     */
    public HelmInstallation(ClusterOperatorConfiguration clusterOperatorConfiguration) {
        this.clusterOperatorConfiguration = clusterOperatorConfiguration;
    }

    /**
     * Returns the configured (and maybe updated) {@link ClusterOperatorConfiguration}.
     * This is then used in {@link SetupClusterOperator#getClusterConfiguration()} (based on the installation method).
     *
     * @return  {@link ClusterOperatorConfiguration} configured and used during the installation
     */
    public ClusterOperatorConfiguration getClusterOperatorConfiguration() {
        return clusterOperatorConfiguration;
    }

    /**
     * Install method that goes through the configured values in the {@link #clusterOperatorConfiguration}.
     * It uses the Helm client and because we are not using the ResourceManager for handling it, we are adding the
     * {@link #delete()} method to the stack.
     */
    @Override
    public void install() {
        InstallCommand installCommand = new Helm(Paths.get(HELM_CHART))
            .install()
            .withName(HELM_RELEASE_NAME)
            .withNamespace(clusterOperatorConfiguration.getNamespaceName())
            .waitReady();

        installCommand.set("defaultImageRegistry", Environment.getIfNotEmptyOrDefault(Environment.STRIMZI_REGISTRY, Environment.STRIMZI_REGISTRY_DEFAULT));
        installCommand.set("defaultImageRepository", Environment.getIfNotEmptyOrDefault(Environment.STRIMZI_ORG, Environment.STRIMZI_ORG_DEFAULT));
        installCommand.set("defaultImageTag", Environment.getIfNotEmptyOrDefault(Environment.STRIMZI_TAG, Environment.STRIMZI_TAG_DEFAULT));

        String bridgeRegistry = Environment.STRIMZI_REGISTRY_DEFAULT;
        String bridgeRepository = Environment.STRIMZI_ORG_DEFAULT;
        String bridgeName = "kafka-bridge";
        String bridgeTag = BridgeUtils.getBridgeVersion();

        // parse Bridge image if needed
        if (!Environment.useLatestReleasedBridge()) {
            Matcher matcher = StUtils.IMAGE_PATTERN_FULL_PATH.matcher(Environment.BRIDGE_IMAGE);

            // in case that the image has correct value, we can configure the values
            if (matcher.matches()) {
                bridgeRegistry = Optional.ofNullable(matcher.group("registry")).orElse(bridgeRegistry);
                bridgeRepository = Optional.ofNullable(matcher.group("org")).orElse(bridgeRepository);
                bridgeName = Optional.ofNullable(matcher.group("image")).orElse(bridgeName);
                bridgeTag = Optional.ofNullable(matcher.group("tag")).orElse(bridgeTag);
            }
        }

        installCommand.set("kafkaBridge.image.registry", bridgeRegistry);
        installCommand.set("kafkaBridge.image.repository", bridgeRepository);
        installCommand.set("kafkaBridge.image.name", bridgeName);
        installCommand.set("kafkaBridge.image.tag", bridgeTag);

        // Additional config
        installCommand.set("image.imagePullPolicy", Environment.COMPONENTS_IMAGE_PULL_POLICY);
        installCommand.set("resources.requests.memory", TestConstants.CO_REQUESTS_MEMORY);
        installCommand.set("resources.requests.cpu", TestConstants.CO_REQUESTS_CPU);
        installCommand.set("resources.limits.memory", TestConstants.CO_LIMITS_MEMORY);
        installCommand.set("resources.limits.cpu", TestConstants.CO_LIMITS_CPU);
        installCommand.set("logLevelOverride", Environment.STRIMZI_LOG_LEVEL);
        installCommand.set("fullReconciliationIntervalMs", Long.toString(clusterOperatorConfiguration.getReconciliationInterval()));
        installCommand.set("operationTimeoutMs", Long.toString(clusterOperatorConfiguration.getOperationTimeout()));
        // As FG is CSV, we need to escape commas for interpretation of helm installation string
        installCommand.set("featureGates", Environment.STRIMZI_FEATURE_GATES.replaceAll(",", "\\\\,"));
        installCommand.set("watchAnyNamespace", clusterOperatorConfiguration.isWatchingAllNamespaces());
        installCommand.set("replicas", clusterOperatorConfiguration.getReplicas());

        if (!clusterOperatorConfiguration.isWatchingAllNamespaces() && !clusterOperatorConfiguration.getNamespacesToWatch().equals(clusterOperatorConfiguration.getNamespaceName())) {
            installCommand.set("watchNamespaces", buildWatchNamespaces());
        }

        // Set extraEnvVars into Helm Chart
        if (clusterOperatorConfiguration.getExtraEnvVars() != null) {
            int envVarIndex = 0;
            for (EnvVar envVar: clusterOperatorConfiguration.getExtraEnvVars()) {
                installCommand.set("extraEnvs[" + envVarIndex + "].name", envVar.getName());
                installCommand.set("extraEnvs[" + envVarIndex + "].value", envVar.getValue());
                envVarIndex++;
            }
        }


        installCommand.call();
        // Push the delete method to KubeResourceManager's stack, so it is automatically deleted after the tests
        KubeResourceManager.get().pushToStack(new ResourceItem<>(this::delete));
    }

    /**
     * Delete method that uses Helm client to delete the whole installation.
     * This is added into ResourceManager's stack in the {@link #install()} method.
     */
    @Override
    public void delete() {
        Helm.uninstall(HELM_RELEASE_NAME)
            .withCascade(UninstallCommand.Cascade.ORPHAN)
            .withNamespace(clusterOperatorConfiguration.getNamespaceName())
            .call();

        KubeResourceManager.get().kubeClient().getClient().namespaces().withName(clusterOperatorConfiguration.getNamespaceName()).delete();
    }

    /**
     * Setting watch namespace is little bit tricky in case of Helm installation. We have following options:
     *  1. Watch only specific namespace        -   @code{namespaceToWatch}="infra-namespace"
     *  2. Watch all namespaces at once         -   @code{namespaceToWatch}="*"
     *  3. Watch multiple namespaces at once    -   @code{namespaceToWatch}="{infra-namespace, namespace-1, namespace2}"
     *
     *  In a third option we must not include namespace (reason: avoid creation of roles and role-bindings in @code{namespaceInstallTo}
     *  namespace where Cluster Operator is installed) where the Cluster Operator is already installed. So, we are forced
     *  to extract such namespace from @code{namespaceToWatch}, because in other installation types such as Bundle or OLM
     *  we need to specify also the namespace where Cluster Operator is installed.
     *
     * @return namespaces, which watches Cluster Operator (without explicit Cluster Operator namespace @code{namespaceInstallTo}.
     */
    private String buildWatchNamespaces() {
        return "{" + this.clusterOperatorConfiguration.getNamespacesToWatch().replaceAll(",*" + clusterOperatorConfiguration.getNamespaceName() + ",*", "") + "}";
    }
}