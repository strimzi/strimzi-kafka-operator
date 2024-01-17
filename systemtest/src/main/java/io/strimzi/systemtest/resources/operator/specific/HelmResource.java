/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator.specific;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceItem;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.BridgeUtils;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class HelmResource implements SpecificResourceType {

    public static final String HELM_CHART = TestUtils.USER_PATH + "/../packaging/helm-charts/helm3/strimzi-kafka-operator/";
    public static final String HELM_RELEASE_NAME = "strimzi-systemtests";

    private String namespaceToWatch;
    private String namespaceInstallTo;

    public HelmResource(String namespace) {
        this.namespaceInstallTo = namespace;
        this.namespaceToWatch = namespace;
    }
    public HelmResource(String namespaceInstallTo, String namespaceToWatch) {
        this.namespaceInstallTo = namespaceInstallTo;
        this.namespaceToWatch = namespaceToWatch;
    }

    public void create(ExtensionContext extensionContext) {
        this.create(extensionContext, TestConstants.CO_OPERATION_TIMEOUT_DEFAULT, TestConstants.RECONCILIATION_INTERVAL, null, 1);
    }

    public void create(ExtensionContext extensionContext, long operationTimeout, long reconciliationInterval, List<EnvVar> extraEnvVars, int replicas) {
        ResourceManager.STORED_RESOURCES.computeIfAbsent(extensionContext.getDisplayName(), k -> new Stack<>());
        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem(this::delete));
        this.clusterOperator(operationTimeout, reconciliationInterval, extraEnvVars, replicas);
    }

    @Override
    public void delete() {
        this.deleteClusterOperator();
    }

    private void clusterOperator(long operationTimeout, long reconciliationInterval, List<EnvVar> extraEnvVars, int replicas) {

        Map<String, Object> values = new HashMap<>();
        // image registry config
        values.put("defaultImageRegistry", Environment.STRIMZI_REGISTRY);
        values.put("kafkaBridge.image.registry", Environment.STRIMZI_REGISTRY_DEFAULT);

        // image repository config
        values.put("defaultImageRepository", Environment.STRIMZI_ORG);
        values.put("kafkaBridge.image.repository", Environment.STRIMZI_ORG_DEFAULT);

        // image tags config
        values.put("defaultImageTag", Environment.STRIMZI_TAG);
        values.put("kafkaBridge.image.tag", BridgeUtils.getBridgeVersion());

        // Additional config
        values.put("image.imagePullPolicy", Environment.OPERATOR_IMAGE_PULL_POLICY);
        values.put("resources.requests.memory", TestConstants.CO_REQUESTS_MEMORY);
        values.put("resources.requests.cpu", TestConstants.CO_REQUESTS_CPU);
        values.put("resources.limits.memory", TestConstants.CO_LIMITS_MEMORY);
        values.put("resources.limits.cpu", TestConstants.CO_LIMITS_CPU);
        values.put("logLevelOverride", Environment.STRIMZI_LOG_LEVEL);
        values.put("fullReconciliationIntervalMs", Long.toString(reconciliationInterval));
        values.put("operationTimeoutMs", Long.toString(operationTimeout));
        // As FG is CSV, we need to escape commas for interpretation of helm installation string
        values.put("featureGates", Environment.STRIMZI_FEATURE_GATES.replaceAll(",", "\\\\,"));
        values.put("watchAnyNamespace", this.namespaceToWatch.equals(TestConstants.WATCH_ALL_NAMESPACES));
        values.put("replicas", replicas);

        if (!this.namespaceToWatch.equals("*") && !this.namespaceToWatch.equals(this.namespaceInstallTo)) {
            values.put("watchNamespaces", buildWatchNamespaces());
        }

        // Set extraEnvVars into Helm Chart
        if (extraEnvVars != null) {
            int envVarIndex = 0;
            for (EnvVar envVar: extraEnvVars) {
                values.put("extraEnvs[" + envVarIndex + "].name", envVar.getName());
                values.put("extraEnvs[" + envVarIndex + "].value", envVar.getValue());
                envVarIndex++;
            }
        }

        Path pathToChart = new File(HELM_CHART).toPath();
        ResourceManager.helmClient().namespace(namespaceInstallTo).install(pathToChart, HELM_RELEASE_NAME, values);
        DeploymentUtils.waitForDeploymentReady(namespaceInstallTo, ResourceManager.getCoDeploymentName());
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
        return "{" + this.namespaceToWatch.replaceAll(",*" + namespaceInstallTo + ",*", "") + "}";
    }

    /**
     * Delete CO deployed via helm chart.
     * NOTE: CRDs are not deleted as part of the uninstallation:
     * <a href="https://github.com/helm/community/blob/f9e06c16d89ccea1bea77c01a6a96ae3b309f823/architecture/crds.md#deleting-crds">CRDs architecture in Helm</a>
     */
    private void deleteClusterOperator() {
        ResourceManager.helmClient().delete(namespaceInstallTo, HELM_RELEASE_NAME);
        DeploymentUtils.waitForDeploymentDeletion(namespaceInstallTo, ResourceManager.getCoDeploymentName());
    }
}
