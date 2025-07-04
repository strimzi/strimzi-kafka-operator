/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.ClusterOperatorRBACType;
import io.strimzi.systemtest.enums.OlmInstallationStrategy;
import io.sundr.builder.annotations.Buildable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Class containing all the configuration options for the ClusterOperator -> including OLM.
 * This class is and should be used across all the installation methods.
 */
@Buildable(editableEnabled = false)
public class ClusterOperatorConfiguration {
    private String operatorDeploymentName = TestConstants.STRIMZI_DEPLOYMENT_NAME;
    private String namespaceName = TestConstants.CO_NAMESPACE;
    private boolean deleteNamespace = true;
    // in case that we have Namespace-scoped RBAC, then we are running tests just in the co-namespace (by default)
    // otherwise, the default is watching all Namespaces ('*')
    private String namespacesToWatch = Environment.isNamespaceRbacScope() ? TestConstants.CO_NAMESPACE : TestConstants.WATCH_ALL_NAMESPACES;
    private String featureGates = Environment.STRIMZI_FEATURE_GATES;
    private int replicas = 1;
    private ClusterOperatorRBACType clusterOperatorRBACType = Environment.STRIMZI_RBAC_SCOPE;

    private List<EnvVar> extraEnvVars;
    private Map<String, String> extraLabels;

    private long reconciliationInterval = TestConstants.RECONCILIATION_INTERVAL;
    private long operationTimeout = TestConstants.CO_OPERATION_TIMEOUT_DEFAULT;

    // --------------------
    // OLM related configuration
    // --------------------
    private final String olmAppBundlePrefix = Environment.OLM_APP_BUNDLE_PREFIX;
    private final String olmOperatorName = Environment.OLM_OPERATOR_NAME;
    private final String olmSourceName = Environment.OLM_SOURCE_NAME;
    private final String olmSourceNamespace = Environment.OLM_SOURCE_NAMESPACE;
    private final String olmOperatorDeploymentNamePrefix = Environment.OLM_OPERATOR_DEPLOYMENT_NAME;
    private String olmOperatorVersion;
    private OlmInstallationStrategy olmInstallationStrategy = OlmInstallationStrategy.Automatic;
    private String olmChannelName = Environment.OLM_OPERATOR_CHANNEL;

    public String getOperatorDeploymentName() {
        return operatorDeploymentName;
    }

    public void setOperatorDeploymentName(String operatorDeploymentName) {
        this.operatorDeploymentName = operatorDeploymentName;
    }

    public String getNamespaceName() {
        return namespaceName;
    }

    public void setNamespaceName(String namespaceName) {
        this.namespaceName = namespaceName;
    }

    public boolean shouldDeleteNamespace() {
        return deleteNamespace;
    }

    public void setDeleteNamespace(boolean skipNamespaceDeletion) {
        this.deleteNamespace = skipNamespaceDeletion;
    }

    public String getNamespacesToWatch() {
        return namespacesToWatch;
    }

    public void setNamespacesToWatch(String namespacesToWatch) {
        this.namespacesToWatch = namespacesToWatch;
    }

    public String getFeatureGates() {
        return featureGates;
    }

    public void setFeatureGates(String featureGates) {
        this.featureGates = featureGates;
    }

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    public ClusterOperatorRBACType getClusterOperatorRBACType() {
        return clusterOperatorRBACType;
    }

    public void setClusterOperatorRBACType(ClusterOperatorRBACType clusterOperatorRBACType) {
        this.clusterOperatorRBACType = clusterOperatorRBACType;
    }

    public List<EnvVar> getExtraEnvVars() {
        return extraEnvVars;
    }

    public void setExtraEnvVars(List<EnvVar> extraEnvVars) {
        this.extraEnvVars = extraEnvVars;
    }

    public Map<String, String> getExtraLabels() {
        return extraLabels;
    }

    public void setExtraLabels(Map<String, String> extraLabels) {
        this.extraLabels = extraLabels;
    }

    public long getReconciliationInterval() {
        return reconciliationInterval;
    }

    public void setReconciliationInterval(long reconciliationInterval) {
        this.reconciliationInterval = reconciliationInterval;
    }

    public long getOperationTimeout() {
        return operationTimeout;
    }

    public void setOperationTimeout(long operationTimeout) {
        this.operationTimeout = operationTimeout;
    }

    public String getOlmAppBundlePrefix() {
        return olmAppBundlePrefix;
    }

    public String getOlmOperatorName() {
        return olmOperatorName;
    }

    public String getOlmSourceName() {
        return olmSourceName;
    }

    public String getOlmSourceNamespace() {
        return olmSourceNamespace;
    }

    public String getOlmOperatorDeploymentNamePrefix() {
        return olmOperatorDeploymentNamePrefix;
    }

    public String getOlmOperatorVersion() {
        return olmOperatorVersion;
    }

    public void setOlmOperatorVersion(String operatorVersion) {
        this.olmOperatorVersion = operatorVersion;
    }

    public OlmInstallationStrategy getOlmInstallationStrategy() {
        return olmInstallationStrategy;
    }

    public void setOlmInstallationStrategy(OlmInstallationStrategy olmInstallationStrategy) {
        this.olmInstallationStrategy = olmInstallationStrategy;
    }

    public String getOlmChannelName() {
        return olmChannelName;
    }

    public void setOlmChannelName(String olmChannelName) {
        this.olmChannelName = olmChannelName;
    }

    public String getCsvName() {
        return olmAppBundlePrefix + ".v" + olmOperatorVersion;
    }

    public List<EnvVar> getAllEnvVariablesForOlm() {
        List<EnvVar> olmEnvVars = getExtraEnvVars() == null ? new ArrayList<>() : new ArrayList<>(getExtraEnvVars());

        olmEnvVars.add(new EnvVar("STRIMZI_FULL_RECONCILIATION_INTERVAL_MS", String.valueOf(getReconciliationInterval()), null));
        olmEnvVars.add(new EnvVar("STRIMZI_OPERATION_TIMEOUT_MS", String.valueOf(getOperationTimeout()), null));
        olmEnvVars.add(new EnvVar("STRIMZI_FEATURE_GATES", getFeatureGates(), null));

        return olmEnvVars;
    }

    public boolean isWatchingAllNamespaces() {
        return TestConstants.WATCH_ALL_NAMESPACES.equals(namespacesToWatch);
    }

    public boolean isNamespaceScopedInstallation() {
        return ClusterOperatorRBACType.NAMESPACE.equals(clusterOperatorRBACType);
    }

    public List<String> getListOfNamespacesToWatch() {
        return Arrays.stream(namespacesToWatch.split(",")).toList();
    }
}