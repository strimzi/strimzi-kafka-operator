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
    private String featureGates = Environment.STRIMZI_FEATURE_GATES;
    private String olmAppBundlePrefix = Environment.OLM_APP_BUNDLE_PREFIX;
    private String olmOperatorName = Environment.OLM_OPERATOR_NAME;
    private String olmOperatorDeploymentNamePrefix = Environment.OLM_OPERATOR_DEPLOYMENT_NAME;
    private String olmSourceName = Environment.OLM_SOURCE_NAME;
    private String olmSourceNamespace = Environment.OLM_SOURCE_NAMESPACE;
    // Init value is needed due to UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR
    private String operatorVersion = "";
    private OlmInstallationStrategy olmInstallationStrategy;
    private String channelName;
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
        this.operatorVersion = operatorVersion != null && operatorVersion.isEmpty() ? Environment.OLM_OPERATOR_LATEST_RELEASE_VERSION : operatorVersion;
    }

    public String getOperatorVersion() {
        return operatorVersion;
    }

    public String getCsvName() {
        return olmAppBundlePrefix + ".v" + operatorVersion;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName == null && operatorVersion.equals(Environment.OLM_OPERATOR_LATEST_RELEASE_VERSION) ? "stable" : channelName;
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

    public String getOlmOperatorDeploymentName() {
        return olmOperatorDeploymentNamePrefix + "-v" + operatorVersion;
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
