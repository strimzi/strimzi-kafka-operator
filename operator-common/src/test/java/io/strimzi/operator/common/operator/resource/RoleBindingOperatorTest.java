/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingList;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RbacAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RoleBindingOperatorTest extends AbstractNamespacedResourceOperatorTest<KubernetesClient, RoleBinding, RoleBindingList, Resource<RoleBinding>> {


    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<? extends Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected RoleBinding resource(String name) {
        Subject ks = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName("some-service-account")
                .withNamespace(NAMESPACE)
                .build();

        RoleRef roleRef = new RoleRefBuilder()
                .withName("some-role")
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("ClusterRole")
                .build();

        return new RoleBindingBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withRoleRef(roleRef)
                .withSubjects(singletonList(ks))
                .build();
    }

    @Override
    protected RoleBinding modifiedResource(String name) {
        RoleRef roleRef = new RoleRefBuilder()
                .withName("some-other-role")
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("ClusterRole")
                .build();

        return new RoleBindingBuilder(resource(name))
                .withRoleRef(roleRef)
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        RbacAPIGroupDSL mockRbac = mock(RbacAPIGroupDSL.class);
        when(mockClient.rbac()).thenReturn(mockRbac);
        when(mockClient.rbac().roleBindings()).thenReturn(op);
    }

    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, RoleBinding, RoleBindingList,
                Resource<RoleBinding>> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new RoleBindingOperator(vertx, mockClient);
    }
}
