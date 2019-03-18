/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.rbac.DoneableKubernetesClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBindingList;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleRef;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesSubject;
import io.fabric8.kubernetes.api.model.rbac.KubernetesSubjectBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.runner.RunWith;

import static java.util.Collections.singletonMap;

@RunWith(VertxUnitRunner.class)
public class ClusterRoleBindingOperatorIT extends AbstractNonNamespacedResourceOperatorIT<KubernetesClient,
        KubernetesClusterRoleBinding, KubernetesClusterRoleBindingList, DoneableKubernetesClusterRoleBinding,
        Resource<KubernetesClusterRoleBinding, DoneableKubernetesClusterRoleBinding>> {

    @Override
    protected AbstractNonNamespacedResourceOperator<KubernetesClient,
            KubernetesClusterRoleBinding, KubernetesClusterRoleBindingList, DoneableKubernetesClusterRoleBinding,
            Resource<KubernetesClusterRoleBinding, DoneableKubernetesClusterRoleBinding>> operator() {
        return new ClusterRoleBindingOperator(vertx, client);
    }

    @Override
    protected KubernetesClusterRoleBinding getOriginal()  {
        KubernetesSubject ks = new KubernetesSubjectBuilder()
                .withKind("ServiceAccount")
                .withName("my-service-account")
                .withNamespace("my-namespace")
                .build();

        KubernetesRoleRef roleRef = new KubernetesRoleRefBuilder()
                .withName("my-cluster-role")
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("ClusterRole")
                .build();

        return new KubernetesClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withLabels(singletonMap("state", "new"))
                .endMetadata()
                    .withSubjects(ks)
                    .withRoleRef(roleRef)
                .build();
    }

    @Override
    protected KubernetesClusterRoleBinding getModified()  {
        KubernetesSubject ks = new KubernetesSubjectBuilder()
                .withKind("ServiceAccount")
                .withName("my-service-account2")
                .withNamespace("my-namespace2")
                .build();

        // RoleRef cannot be changed
        KubernetesRoleRef roleRef = new KubernetesRoleRefBuilder()
                .withName("my-cluster-role")
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("ClusterRole")
                .build();

        return new KubernetesClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withLabels(singletonMap("state", "modified"))
                .endMetadata()
                .withSubjects(ks)
                .withRoleRef(roleRef)
                .build();
    }

    @Override
    protected void assertResources(TestContext context, KubernetesClusterRoleBinding expected, KubernetesClusterRoleBinding actual)   {
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
