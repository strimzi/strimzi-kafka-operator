/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingList;
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

@ExtendWith(VertxExtension.class)
public class RoleBindingOperatorIT extends AbstractNamespacedResourceOperatorIT<KubernetesClient, RoleBinding, RoleBindingList, Resource<RoleBinding>> {

    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, RoleBinding, RoleBindingList, Resource<RoleBinding>> operator() {
        return new RoleBindingOperator(vertx, client);
    }

    @Override
    protected RoleBinding getOriginal()  {
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

        return new RoleBindingBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(namespace)
                    .withLabels(singletonMap("state", "new"))
                .endMetadata()
                    .withSubjects(ks)
                    .withRoleRef(roleRef)
                .build();
    }

    @Override
    protected RoleBinding getModified()  {
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

        return new RoleBindingBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(namespace)
                    .withLabels(singletonMap("state", "modified"))
                .endMetadata()
                .withSubjects(ks)
                .withRoleRef(roleRef)
                .build();
    }

    @Override
    protected void assertResources(VertxTestContext context, RoleBinding expected, RoleBinding actual)   {
        context.verify(() -> assertThat(actual.getMetadata().getName(), is(expected.getMetadata().getName())));
        context.verify(() -> assertThat(actual.getMetadata().getNamespace(), is(expected.getMetadata().getNamespace())));
        context.verify(() -> assertThat(actual.getMetadata().getLabels(), is(expected.getMetadata().getLabels())));
        context.verify(() -> assertThat(actual.getSubjects().size(), is(expected.getSubjects().size())));
        context.verify(() -> assertThat(actual.getSubjects().get(0).getKind(), is(expected.getSubjects().get(0).getKind())));
        context.verify(() -> assertThat(actual.getSubjects().get(0).getNamespace(), is(expected.getSubjects().get(0).getNamespace())));
        context.verify(() -> assertThat(actual.getSubjects().get(0).getName(), is(expected.getSubjects().get(0).getName())));

        context.verify(() -> assertThat(actual.getRoleRef().getKind(), is(expected.getRoleRef().getKind())));
        context.verify(() -> assertThat(actual.getRoleRef().getApiGroup(), is(expected.getRoleRef().getApiGroup())));
        context.verify(() -> assertThat(actual.getRoleRef().getName(), is(expected.getRoleRef().getName())));
    }
}
