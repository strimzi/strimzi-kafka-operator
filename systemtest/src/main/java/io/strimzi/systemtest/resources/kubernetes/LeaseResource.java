/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.coordination.v1.Lease;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceType;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class LeaseResource implements ResourceType<Lease> {
    @Override
    public String getKind() {
        return TestConstants.LEASE;
    }

    @Override
    public Lease get(String namespace, String name) {
        return kubeClient(namespace).getClient().leases().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(Lease resource) {
        kubeClient().getClient().leases().resource(resource).create();
    }

    @Override
    public void delete(Lease resource) {
        kubeClient().getClient().leases().resource(resource).delete();
    }

    @Override
    public void update(Lease resource) {
        kubeClient().getClient().leases().resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(Lease resource) {
        return resource != null;
    }
}
