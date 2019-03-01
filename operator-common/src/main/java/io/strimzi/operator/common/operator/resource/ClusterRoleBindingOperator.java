/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.rbac.DoneableKubernetesClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBindingList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public class ClusterRoleBindingOperator extends AbstractNonNamespacedResourceOperator<KubernetesClient,
        KubernetesClusterRoleBinding, KubernetesClusterRoleBindingList, DoneableKubernetesClusterRoleBinding,
        Resource<KubernetesClusterRoleBinding, DoneableKubernetesClusterRoleBinding>> {

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */

    public ClusterRoleBindingOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "ClusterRoleBinding");
    }

    @Override
    protected MixedOperation<KubernetesClusterRoleBinding, KubernetesClusterRoleBindingList,
            DoneableKubernetesClusterRoleBinding, Resource<KubernetesClusterRoleBinding,
            DoneableKubernetesClusterRoleBinding>> operation() {
        return client.rbac().kubernetesClusterRoleBindings();
    }

    @Override
    protected Future<ReconcileResult<KubernetesClusterRoleBinding>> internalPatch(String name,
                                                                                  KubernetesClusterRoleBinding current,
                                                                                  KubernetesClusterRoleBinding desired) {
        return Future.succeededFuture(ReconcileResult.noop(current));
    }
}
