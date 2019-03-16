/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.rbac.DoneableKubernetesClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBindingList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RbacAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterRoleBindingOperatorTest extends AbstractNonNamespacedResourceOperatorTest<KubernetesClient,
        KubernetesClusterRoleBinding, KubernetesClusterRoleBindingList, DoneableKubernetesClusterRoleBinding,
        Resource<KubernetesClusterRoleBinding, DoneableKubernetesClusterRoleBinding>> {

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        RbacAPIGroupDSL mockRbac = mock(RbacAPIGroupDSL.class);
        when(mockClient.rbac()).thenReturn(mockRbac);
        when(mockRbac.kubernetesClusterRoleBindings()).thenReturn(op);
    }

    @Override
    protected AbstractNonNamespacedResourceOperator<KubernetesClient, KubernetesClusterRoleBinding,
            KubernetesClusterRoleBindingList, DoneableKubernetesClusterRoleBinding,
            Resource<KubernetesClusterRoleBinding, DoneableKubernetesClusterRoleBinding>> createResourceOperations(
                    Vertx vertx, KubernetesClient mockClient) {
        return new ClusterRoleBindingOperator(vertx, mockClient);
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
    protected KubernetesClusterRoleBinding resource() {
        return new KubernetesClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
            .build();
    }
}
