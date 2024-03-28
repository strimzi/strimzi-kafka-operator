/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RbacAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterRoleOperatorTest extends AbstractNonNamespacedResourceOperatorTest<KubernetesClient,
        ClusterRole, ClusterRoleList, Resource<ClusterRole>> {

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        RbacAPIGroupDSL mockRbac = mock(RbacAPIGroupDSL.class);
        when(mockClient.rbac()).thenReturn(mockRbac);
        when(mockRbac.clusterRoles()).thenReturn(op);
    }

    @Override
    protected AbstractNonNamespacedResourceOperator<KubernetesClient, ClusterRole, ClusterRoleList,
            Resource<ClusterRole>> createResourceOperations(
                    Vertx vertx, KubernetesClient mockClient) {
        return new ClusterRoleOperator(vertx, mockClient) {
            @Override
            protected long deleteTimeoutMs() {
                return 100;
            }
        };
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
    protected ClusterRole resource() {
        return new ClusterRoleBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
            .build();
    }

    @Override
    protected ClusterRole modifiedResource() {
        return new ClusterRoleBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withLabels(singletonMap("foo2", "bar2"))
                .endMetadata()
            .build();
    }
}
