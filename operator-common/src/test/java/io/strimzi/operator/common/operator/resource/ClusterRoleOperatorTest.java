/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.rbac.DoneableKubernetesClusterRole;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRole;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RbacAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterRoleOperatorTest extends AbstractNonNamespacedResourceOperatorTest<KubernetesClient,
        KubernetesClusterRole, KubernetesClusterRoleList, DoneableKubernetesClusterRole,
        Resource<KubernetesClusterRole, DoneableKubernetesClusterRole>> {

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        RbacAPIGroupDSL mockRbac = mock(RbacAPIGroupDSL.class);
        when(mockClient.rbac()).thenReturn(mockRbac);
        when(mockRbac.kubernetesClusterRoles()).thenReturn(op);
    }

    @Override
    protected AbstractNonNamespacedResourceOperator<KubernetesClient, KubernetesClusterRole, KubernetesClusterRoleList,
            DoneableKubernetesClusterRole, Resource<KubernetesClusterRole, DoneableKubernetesClusterRole>> createResourceOperations(
                    Vertx vertx, KubernetesClient mockClient) {
        return new ClusterRoleOperator(vertx, mockClient);
    }

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<? extends Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected KubernetesClusterRole resource() {
        return new KubernetesClusterRoleBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
            .build();
    }
}
