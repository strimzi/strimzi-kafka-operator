/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingList;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

@ExtendWith(VertxExtension.class)
public class ClusterRoleBindingOperatorIT extends AbstractNonNamespacedResourceOperatorIT<KubernetesClient,
        ClusterRoleBinding, ClusterRoleBindingList, Resource<ClusterRoleBinding>> {

    @Override
    protected AbstractNonNamespacedResourceOperator<KubernetesClient,
            ClusterRoleBinding, ClusterRoleBindingList,
            Resource<ClusterRoleBinding>> operator() {
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
                    .withName(resourceName)
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
                    .withName(resourceName)
                    .withLabels(singletonMap("state", "modified"))
                .endMetadata()
                .withSubjects(ks)
                .withRoleRef(roleRef)
                .build();
    }

    @Override
    protected void assertResources(VertxTestContext context, ClusterRoleBinding expected, ClusterRoleBinding actual) {
        context.verify(() -> {
            assertThat(actual.getMetadata().getName(), is(expected.getMetadata().getName()));
            assertThat(actual.getMetadata().getLabels(), is(expected.getMetadata().getLabels()));
            assertThat(actual.getSubjects(), hasSize(expected.getSubjects().size()));
            assertThat(actual.getSubjects().get(0).getKind(), is(expected.getSubjects().get(0).getKind()));
            assertThat(actual.getSubjects().get(0).getNamespace(), is(expected.getSubjects().get(0).getNamespace()));
            assertThat(actual.getSubjects().get(0).getName(), is(expected.getSubjects().get(0).getName()));

            assertThat(actual.getRoleRef().getKind(), is(expected.getRoleRef().getKind()));
            assertThat(actual.getRoleRef().getApiGroup(), is(expected.getRoleRef().getApiGroup()));
            assertThat(actual.getRoleRef().getName(), is(expected.getRoleRef().getName()));
        });
    }
}
