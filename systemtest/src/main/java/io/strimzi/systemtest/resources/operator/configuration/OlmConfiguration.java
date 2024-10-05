/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator.configuration;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.OlmInstallationStrategy;
import io.sundr.builder.annotations.Buildable;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;

@Buildable(editableEnabled = false)
public class OlmConfiguration {
    private ExtensionContext extensionContext;
    private String namespaceName;
    private String namespaceToWatch;
    private final String featureGates = Environment.STRIMZI_FEATURE_GATES;
    private final String olmAppBundlePrefix = Environment.OLM_APP_BUNDLE_PREFIX;
    private final String olmOperatorName = Environment.OLM_OPERATOR_NAME;
    private final String olmSourceName = Environment.OLM_SOURCE_NAME;
    private final String olmSourceNamespace = Environment.OLM_SOURCE_NAMESPACE;
    private String olmOperatorDeploymentNamePrefix = Environment.OLM_OPERATOR_DEPLOYMENT_NAME;
    private String olmOperatorDeploymentName = "";
    private String operatorVersion;
    private OlmInstallationStrategy olmInstallationStrategy;
    private String channelName = Environment.OLM_OPERATOR_CHANNEL;
    private List<EnvVar> envVars;
    private long reconciliationInterval;
    private long operationTimeout;

    public void setExtensionContext(ExtensionContext extensionContext) {
        if (extensionContext == null) {
            throw new IllegalArgumentException("Extension context cannot be empty");
        }
        this.extensionContext = extensionContext;
    }

    public ExtensionContext getExtensionContext() {
        return extensionContext;
    }

    public void setNamespaceName(String namespaceName) {
        this.namespaceName = namespaceName;
    }

    public String getNamespaceName() {
        return namespaceName;
    }

    public void setNamespaceToWatch(String namespaceToWatch) {
        this.namespaceToWatch = namespaceToWatch;
    }

    public String getNamespaceToWatch() {
        return namespaceToWatch;
    }

    public void setOlmInstallationStrategy(OlmInstallationStrategy olmInstallationStrategy) {
        this.olmInstallationStrategy = olmInstallationStrategy == null ? OlmInstallationStrategy.Automatic : olmInstallationStrategy;
    }

    public OlmInstallationStrategy getOlmInstallationStrategy() {
        return olmInstallationStrategy;
    }

    public void setOperatorVersion(String operatorVersion) {
        this.operatorVersion = operatorVersion;
    }

    public String getOperatorVersion() {
        return operatorVersion;
    }

    public String getCsvName() {
        return olmAppBundlePrefix + ".v" + operatorVersion;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName == null ? this.channelName : channelName;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setEnvVars(List<EnvVar> envVars) {
        this.envVars = envVars;
    }

    public List<EnvVar> getEnvVars() {
        return envVars;
    }

    public void setReconciliationInterval(long reconciliationInterval) {
        this.reconciliationInterval = reconciliationInterval;
    }

    public long getReconciliationInterval() {
        return reconciliationInterval;
    }

    public void setOperationTimeout(long operationTimeout) {
        this.operationTimeout = operationTimeout;
    }

    public long getOperationTimeout() {
        return operationTimeout;
    }

    public String getFeatureGates() {
        return featureGates;
    }

    public String getOlmAppBundlePrefix() {
        return olmAppBundlePrefix;
    }

    public String getOlmOperatorDeploymentNamePrefix() {
        return olmOperatorDeploymentNamePrefix;
    }

    public void setOlmOperatorDeploymentName(String olmOperatorDeploymentName) {
        this.olmOperatorDeploymentName = olmOperatorDeploymentName;
    }

    public String getOlmOperatorDeploymentName() {
        return olmOperatorDeploymentName;
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

    public List<EnvVar> getAllEnvVariablesForOlm() {
        List<EnvVar> olmEnvVars = envVars == null ? new ArrayList<>() : new ArrayList<>(envVars);

        olmEnvVars.add(new EnvVar("STRIMZI_FULL_RECONCILIATION_INTERVAL_MS", String.valueOf(getReconciliationInterval()), null));
        olmEnvVars.add(new EnvVar("STRIMZI_OPERATION_TIMEOUT_MS", String.valueOf(getOperationTimeout()), null));
        olmEnvVars.add(new EnvVar("STRIMZI_FEATURE_GATES", getFeatureGates(), null));

        return olmEnvVars;
    }
}
