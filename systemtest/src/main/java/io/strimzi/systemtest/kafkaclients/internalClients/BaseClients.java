/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

import io.strimzi.systemtest.resources.ResourceManager;
import io.sundr.builder.annotations.Buildable;

import java.security.InvalidParameterException;

@Buildable(editableEnabled = false)
public abstract class BaseClients {
    private String bootstrapAddress;
    private String topicName;
    private String additionalConfig;
    private String namespaceName;

    public String getBootstrapAddress() {
        return bootstrapAddress;
    }

    public void setBootstrapAddress(String bootstrapAddress) {
        if (bootstrapAddress == null || bootstrapAddress.isEmpty()) {
            throw new InvalidParameterException("Bootstrap server is not set.");
        }
        this.bootstrapAddress = bootstrapAddress;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        if (topicName == null || topicName.isEmpty()) {
            throw new InvalidParameterException("Topic name is not set.");
        }
        this.topicName = topicName;
    }

    public String getNamespaceName() {
        return namespaceName;
    }

    public void setNamespaceName(String namespaceName) {
        this.namespaceName = (namespaceName == null || namespaceName.isEmpty()) ? ResourceManager.kubeClient().getNamespace() : namespaceName;
    }

    public String getAdditionalConfig() {
        return additionalConfig;
    }

    public void setAdditionalConfig(String additionalConfig) {
        this.additionalConfig = (additionalConfig == null || additionalConfig.isEmpty()) ? "" : additionalConfig;
    }
}
