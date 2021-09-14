/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceType;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class ClusterOperatorCustomResourceDefinition implements ResourceType<CustomResourceDefinition> {

    @Override
    public String getKind() {
        return Constants.CUSTOM_RESOURCE_DEFINITION;
    }
    @Override
    public CustomResourceDefinition get(String namespace, String name) {
        return kubeClient().getCustomResourceDefinition(name);
    }
    @Override
    public void create(CustomResourceDefinition resource) {
        kubeClient().createOrReplaceCustomResourceDefinition(resource);
    }
    @Override
    public void delete(CustomResourceDefinition resource) {
        kubeClient().deleteCustomResourceDefinition(resource);
    }
    @Override
    public boolean waitForReadiness(CustomResourceDefinition resource) {
        return resource != null;
    }
}
