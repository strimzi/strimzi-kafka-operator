/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.admissionregistration.v1.ValidatingWebhookConfiguration;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.test.k8s.KubeClusterResource;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class ValidatingWebhookConfigurationResource implements ResourceType<ValidatingWebhookConfiguration> {

    @Override
    public String getKind() {
        return TestConstants.VALIDATION_WEBHOOK_CONFIG;
    }

    @Override
    public ValidatingWebhookConfiguration get(String namespace, String name) {
        return kubeClient(KubeClusterResource.getInstance().defaultNamespace()).getValidatingWebhookConfiguration(name);
    }

    @Override
    public void create(ValidatingWebhookConfiguration resource) {
        kubeClient(KubeClusterResource.getInstance().defaultNamespace()).createValidatingWebhookConfiguration(resource);
    }

    @Override
    public void delete(ValidatingWebhookConfiguration resource) {
        kubeClient(KubeClusterResource.getInstance().defaultNamespace()).deleteValidatingWebhookConfiguration(resource);
    }

    @Override
    public void update(ValidatingWebhookConfiguration resource) {
        kubeClient(KubeClusterResource.getInstance().defaultNamespace()).updateValidatingWebhookConfiguration(resource);
    }

    @Override
    public boolean waitForReadiness(ValidatingWebhookConfiguration resource) {
        return resource != null;
    }
}
