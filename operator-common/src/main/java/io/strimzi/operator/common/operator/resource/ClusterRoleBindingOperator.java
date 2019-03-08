/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingList;
import io.fabric8.kubernetes.api.model.rbac.DoneableClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

public class ClusterRoleBindingOperator extends AbstractNonNamespacedResourceOperator<KubernetesClient,
        ClusterRoleBinding, ClusterRoleBindingList, DoneableClusterRoleBinding,
        Resource<ClusterRoleBinding, DoneableClusterRoleBinding>> {

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */

    public ClusterRoleBindingOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "ClusterRoleBinding");
    }

    @Override
    protected MixedOperation<ClusterRoleBinding, ClusterRoleBindingList,
            DoneableClusterRoleBinding, Resource<ClusterRoleBinding,
            DoneableClusterRoleBinding>> operation() {
        return client.rbac().clusterRoleBindings();
    }
}
