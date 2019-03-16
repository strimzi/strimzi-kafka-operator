/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.rbac.DoneableKubernetesRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBindingList;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleRef;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesSubject;
import io.fabric8.kubernetes.api.model.rbac.KubernetesSubjectBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RbacAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RoleBindingOperatorTest extends AbstractResourceOperatorTest<KubernetesClient, KubernetesRoleBinding, KubernetesRoleBindingList,
        DoneableKubernetesRoleBinding, Resource<KubernetesRoleBinding, DoneableKubernetesRoleBinding>> {


    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<? extends Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected KubernetesRoleBinding resource() {
        KubernetesSubject ks = new KubernetesSubjectBuilder()
                .withKind("ServiceAccount")
                .withName("some-service-account")
                .withNamespace(NAMESPACE)
                .build();

        KubernetesRoleRef roleRef = new KubernetesRoleRefBuilder()
                .withName("some-role")
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("ClusterRole")
                .build();

        return new KubernetesRoleBindingBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withRoleRef(roleRef)
                .withSubjects(singletonList(ks))
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        RbacAPIGroupDSL mockRbac = mock(RbacAPIGroupDSL.class);
        when(mockClient.rbac()).thenReturn(mockRbac);
        when(mockClient.rbac().kubernetesRoleBindings()).thenReturn(op);
    }

    @Override
    protected AbstractResourceOperator<KubernetesClient, KubernetesRoleBinding, KubernetesRoleBindingList, DoneableKubernetesRoleBinding,
            Resource<KubernetesRoleBinding, DoneableKubernetesRoleBinding>> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new RoleBindingOperator(vertx, mockClient);
    }
}
