/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

/**
 * Operator for managing Roles
 */
public class RoleOperator extends AbstractNamespacedResourceOperator<
        KubernetesClient,
        Role,
        RoleList,
        Resource<Role>> {
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public RoleOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Role");
    }

    @Override
    protected MixedOperation<Role, RoleList, Resource<Role>> operation() {
        return client.rbac().roles();
    }
}
