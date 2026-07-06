/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperator;

import java.util.concurrent.Executor;

/**
 * Operator for managing Role Bindings
 */
public class RoleBindingOperator extends AbstractNamespacedResourceOperator<KubernetesClient, RoleBinding,
        RoleBindingList,
        Resource<RoleBinding>> {
    /**
     * Constructor
     * @param asyncExecutor Executor to use for asynchronous subroutines
     * @param client The Kubernetes client
     */
    public RoleBindingOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, "RoleBinding");
    }

    @Override
    protected MixedOperation<RoleBinding, RoleBindingList, Resource<RoleBinding>> operation() {
        return client.rbac().roleBindings();
    }
}
