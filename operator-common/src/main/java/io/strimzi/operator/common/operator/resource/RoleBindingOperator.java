/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

/**
 * Operator for managing Role Bindings
 */
public class RoleBindingOperator extends AbstractNamespacedResourceOperator<KubernetesClient, RoleBinding,
        RoleBindingList,
        Resource<RoleBinding>> {
    /**
     * Constructor
     * @param client The Kubernetes client
     */
    public RoleBindingOperator(KubernetesClient client) {
        super(client, "RoleBinding");
    }

    @Override
    protected MixedOperation<RoleBinding, RoleBindingList, Resource<RoleBinding>> operation() {
        return client.rbac().roleBindings();
    }
}
