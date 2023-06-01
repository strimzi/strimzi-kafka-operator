/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

/**
 * Operator for managing Cluster Role Bindings
 */
public class ClusterRoleBindingOperator extends AbstractNonNamespacedResourceOperator<KubernetesClient,
        ClusterRoleBinding, ClusterRoleBindingList, Resource<ClusterRoleBinding>> {

    /**
     * Constructor.
     * @param client The Kubernetes client.
     */
    public ClusterRoleBindingOperator(KubernetesClient client) {
        super(client, "ClusterRoleBinding");
    }

    @Override
    protected NonNamespaceOperation<ClusterRoleBinding, ClusterRoleBindingList, Resource<ClusterRoleBinding>> operation() {
        return client.rbac().clusterRoleBindings();
    }
}
