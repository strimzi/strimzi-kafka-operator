/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceType;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class ClusterRoleResource implements ResourceType<ClusterRole> {

    @Override
    public String getKind() {
        return Constants.CLUSTER_ROLE;
    }
    @Override
    public ClusterRole get(String namespace, String name) {
        return kubeClient("default").getClusterRole(name);
    }
    @Override
    public void create(ClusterRole resource) {
        resource.getMetadata().setNamespace("default");
        kubeClient().createOrReplaceClusterRoles(resource);
    }
    @Override
    public void delete(ClusterRole resource) {
        resource.getMetadata().setNamespace("default");
        kubeClient().deleteClusterRole(resource);
    }
    @Override
    public boolean waitForReadiness(ClusterRole resource) {
        return resource != null;
    }
}
