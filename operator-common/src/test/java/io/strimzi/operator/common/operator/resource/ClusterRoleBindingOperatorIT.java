/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingList;
import io.fabric8.kubernetes.api.model.rbac.DoneableClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.runner.RunWith;

import static java.util.Collections.singletonMap;

@RunWith(VertxUnitRunner.class)
public class ClusterRoleBindingOperatorIT extends AbstractNonNamespacedResourceOperatorIT<KubernetesClient,
        ClusterRoleBinding, ClusterRoleBindingList, DoneableClusterRoleBinding,
        Resource<ClusterRoleBinding, DoneableClusterRoleBinding>> {

    @Override
    protected AbstractNonNamespacedResourceOperator<KubernetesClient,
            ClusterRoleBinding, ClusterRoleBindingList, DoneableClusterRoleBinding,
            Resource<ClusterRoleBinding, DoneableClusterRoleBinding>> operator() {
        return new ClusterRoleBindingOperator(vertx, client);
    }

    @Override
    protected ClusterRoleBinding getOriginal()  {
        Subject ks = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName("my-service-account")
                .withNamespace("my-namespace")
                .build();

        RoleRef roleRef = new RoleRefBuilder()
                .withName("my-cluster-role")
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("ClusterRole")
                .build();

        return new ClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withLabels(singletonMap("state", "new"))
                .endMetadata()
                    .withSubjects(ks)
                    .withRoleRef(roleRef)
                .build();
    }

    @Override
    protected ClusterRoleBinding getModified()  {
        Subject ks = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName("my-service-account2")
                .withNamespace("my-namespace2")
                .build();

        // RoleRef cannot be changed
        RoleRef roleRef = new RoleRefBuilder()
                .withName("my-cluster-role")
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("ClusterRole")
                .build();

        return new ClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withLabels(singletonMap("state", "modified"))
                .endMetadata()
                .withSubjects(ks)
                .withRoleRef(roleRef)
                .build();
    }

    @Override
    protected void assertResources(TestContext context, ClusterRoleBinding expected, ClusterRoleBinding actual)   {
        context.assertEquals(expected.getMetadata().getName(), actual.getMetadata().getName());
        context.assertEquals(expected.getMetadata().getLabels(), actual.getMetadata().getLabels());
        context.assertEquals(expected.getSubjects().size(), actual.getSubjects().size());
        context.assertEquals(expected.getSubjects().get(0).getKind(), actual.getSubjects().get(0).getKind());
        context.assertEquals(expected.getSubjects().get(0).getNamespace(), actual.getSubjects().get(0).getNamespace());
        context.assertEquals(expected.getSubjects().get(0).getName(), actual.getSubjects().get(0).getName());

        context.assertEquals(expected.getRoleRef().getKind(), actual.getRoleRef().getKind());
        context.assertEquals(expected.getRoleRef().getApiGroup(), actual.getRoleRef().getApiGroup());
        context.assertEquals(expected.getRoleRef().getName(), actual.getRoleRef().getName());

    }
}
