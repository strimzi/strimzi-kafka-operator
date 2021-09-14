/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceType;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class ServiceAccountResource implements ResourceType<ServiceAccount> {

    @Override
    public String getKind() {
        return Constants.SERVICE_ACCOUNT;
    }
    @Override
    public ServiceAccount get(String namespace, String name) {
        return kubeClient(namespace).getServiceAccount(namespace, name);
    }
    @Override
    public void create(ServiceAccount resource) {
        kubeClient().createOrReplaceServiceAccount(resource);
    }
    @Override
    public void delete(ServiceAccount resource) {
        kubeClient().deleteServiceAccount(resource);
    }

    @Override
    public boolean waitForReadiness(ServiceAccount resource) {
        return resource != null;
    }
}
