/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.admissionregistration.v1.ValidatingWebhookConfiguration;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;

public class ValidatingWebhookConfigurationResource implements ResourceType<ValidatingWebhookConfiguration> {

    @Override
    public String getKind() {
        return Constants.VALIDATION_WEBHOOK_CONFIG;
    }

    @Override
    public ValidatingWebhookConfiguration get(String namespace, String name) {
        return ResourceManager.kubeClient().getValidatingWebhookConfiguration(name);
    }

    @Override
    public void create(ValidatingWebhookConfiguration resource) {
        ResourceManager.kubeClient().createValidatingWebhookConfiguration(resource);
    }

    @Override
    public void delete(ValidatingWebhookConfiguration resource) {
        ResourceManager.kubeClient().deleteValidatingWebhookConfiguration(resource);
    }

    @Override
    public boolean waitForReadiness(ValidatingWebhookConfiguration resource) {
        return resource != null;
    }
}
