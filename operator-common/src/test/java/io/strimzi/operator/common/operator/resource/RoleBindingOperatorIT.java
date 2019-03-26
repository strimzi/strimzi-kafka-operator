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
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.runner.RunWith;

import static java.util.Collections.singletonMap;

@RunWith(VertxUnitRunner.class)
public class RoleBindingOperatorIT extends AbstractResourceOperatorIT<KubernetesClient, KubernetesRoleBinding, KubernetesRoleBindingList,
        DoneableKubernetesRoleBinding, Resource<KubernetesRoleBinding, DoneableKubernetesRoleBinding>> {

    @Override
    protected AbstractResourceOperator<KubernetesClient, KubernetesRoleBinding, KubernetesRoleBindingList,
            DoneableKubernetesRoleBinding, Resource<KubernetesRoleBinding, DoneableKubernetesRoleBinding>> operator() {
        return new RoleBindingOperator(vertx, client);
    }

    @Override
    protected KubernetesRoleBinding getOriginal()  {
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

        return new KubernetesRoleBindingBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withNamespace(namespace)
                    .withLabels(singletonMap("state", "new"))
                .endMetadata()
                    .withSubjects(ks)
                    .withRoleRef(roleRef)
                .build();
    }

    @Override
    protected KubernetesRoleBinding getModified()  {
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

        return new KubernetesRoleBindingBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withNamespace(namespace)
                    .withLabels(singletonMap("state", "modified"))
                .endMetadata()
                .withSubjects(ks)
                .withRoleRef(roleRef)
                .build();
    }

    @Override
    protected void assertResources(TestContext context, KubernetesRoleBinding expected, KubernetesRoleBinding actual)   {
        context.assertEquals(expected.getMetadata().getName(), actual.getMetadata().getName());
        context.assertEquals(expected.getMetadata().getNamespace(), actual.getMetadata().getNamespace());
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
