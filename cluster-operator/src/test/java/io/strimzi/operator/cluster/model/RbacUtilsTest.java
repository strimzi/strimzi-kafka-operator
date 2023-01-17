/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.strimzi.api.kafka.model.template.ResourceTemplate;
import io.strimzi.api.kafka.model.template.ResourceTemplateBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class RbacUtilsTest {
    private final static String NAME = "my-name";
    private final static String NAMESPACE = "my-namespace";
    private static final OwnerReference OWNER_REFERENCE = new OwnerReferenceBuilder()
            .withApiVersion("v1")
            .withKind("my-kind")
            .withName("my-cluster")
            .withUid("my-uid")
            .withBlockOwnerDeletion(false)
            .withController(false)
            .build();
    private static final Labels LABELS = Labels
            .forStrimziKind("my-kind")
            .withStrimziName("my-name")
            .withStrimziCluster("my-cluster")
            .withStrimziComponentType("my-component-type")
            .withAdditionalLabels(Map.of("label-1", "value-1", "label-2", "value-2"));
    private final static ResourceTemplate TEMPLATE = new ResourceTemplateBuilder()
            .withNewMetadata()
                .withLabels(Map.of("label-3", "value-3", "label-4", "value-4"))
                .withAnnotations(Map.of("anno-1", "value-1", "anno-2", "value-2"))
            .endMetadata()
            .build();

    @ParallelTest
    public void testRole()  {
        PolicyRule rule = new PolicyRuleBuilder()
                .withApiGroups("kafka.strimzi.io")
                .withResources("kafkausers", "kafkausers/status")
                .withVerbs("get", "list", "watch", "create", "patch", "update", "delete")
                .build();

        Role role = RbacUtils.createRole(NAME, NAMESPACE, List.of(rule), LABELS, OWNER_REFERENCE, null);

        assertThat(role.getMetadata().getName(), is(NAME));
        assertThat(role.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(role.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(role.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(role.getMetadata().getAnnotations(), is(nullValue()));

        assertThat(role.getRules(), is(List.of(rule)));
    }

    @ParallelTest
    public void testRoleWithTemplate()  {
        PolicyRule rule = new PolicyRuleBuilder()
                .withApiGroups("kafka.strimzi.io")
                .withResources("kafkausers", "kafkausers/status")
                .withVerbs("get", "list", "watch", "create", "patch", "update", "delete")
                .build();

        Role role = RbacUtils.createRole(NAME, NAMESPACE, List.of(rule), LABELS, OWNER_REFERENCE, TEMPLATE);

        assertThat(role.getMetadata().getName(), is(NAME));
        assertThat(role.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(role.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(role.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(role.getMetadata().getAnnotations(), is(Map.of("anno-1", "value-1", "anno-2", "value-2")));

        assertThat(role.getRules(), is(List.of(rule)));
    }

    @ParallelTest
    public void testRoleBinding()  {
        Subject subject = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName("my-sa")
                .withNamespace(NAMESPACE)
                .build();

        RoleRef roleRef = new RoleRefBuilder()
                .withName("my-role")
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("Role")
                .build();

        RoleBinding roleBinding = RbacUtils.createRoleBinding(NAME, NAMESPACE, roleRef, List.of(subject), LABELS, OWNER_REFERENCE, null);

        assertThat(roleBinding.getMetadata().getName(), is(NAME));
        assertThat(roleBinding.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(roleBinding.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(roleBinding.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(roleBinding.getMetadata().getAnnotations(), is(nullValue()));

        assertThat(roleBinding.getRoleRef(), is(roleRef));
        assertThat(roleBinding.getSubjects(), is(List.of(subject)));
    }

    @ParallelTest
    public void testRoleBindingWithTemplate()  {
        Subject subject = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName("my-sa")
                .withNamespace(NAMESPACE)
                .build();

        RoleRef roleRef = new RoleRefBuilder()
                .withName("my-role")
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("Role")
                .build();

        RoleBinding roleBinding = RbacUtils.createRoleBinding(NAME, NAMESPACE, roleRef, List.of(subject), LABELS, OWNER_REFERENCE, TEMPLATE);

        assertThat(roleBinding.getMetadata().getName(), is(NAME));
        assertThat(roleBinding.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(roleBinding.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(roleBinding.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(roleBinding.getMetadata().getAnnotations(), is(Map.of("anno-1", "value-1", "anno-2", "value-2")));

        assertThat(roleBinding.getRoleRef(), is(roleRef));
        assertThat(roleBinding.getSubjects(), is(List.of(subject)));
    }

    @ParallelTest
    public void testClusterRoleBinding()  {
        Subject subject = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName("my-sa")
                .withNamespace(NAMESPACE)
                .build();

        RoleRef roleRef = new RoleRefBuilder()
                .withName("my-role")
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("Role")
                .build();

        ClusterRoleBinding roleBinding = RbacUtils.createClusterRoleBinding(NAME, roleRef, List.of(subject), LABELS, null);

        assertThat(roleBinding.getMetadata().getName(), is(NAME));
        assertThat(roleBinding.getMetadata().getNamespace(), is(nullValue()));
        assertThat(roleBinding.getMetadata().getOwnerReferences(), is(List.of()));
        assertThat(roleBinding.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(roleBinding.getMetadata().getAnnotations(), is(nullValue()));

        assertThat(roleBinding.getRoleRef(), is(roleRef));
        assertThat(roleBinding.getSubjects(), is(List.of(subject)));
    }

    @ParallelTest
    public void testClusterRoleBindingWithTemplate()  {
        Subject subject = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName("my-sa")
                .withNamespace(NAMESPACE)
                .build();

        RoleRef roleRef = new RoleRefBuilder()
                .withName("my-role")
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("Role")
                .build();

        ClusterRoleBinding roleBinding = RbacUtils.createClusterRoleBinding(NAME, roleRef, List.of(subject), LABELS, TEMPLATE);

        assertThat(roleBinding.getMetadata().getName(), is(NAME));
        assertThat(roleBinding.getMetadata().getNamespace(), is(nullValue()));
        assertThat(roleBinding.getMetadata().getOwnerReferences(), is(List.of()));
        assertThat(roleBinding.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(roleBinding.getMetadata().getAnnotations(), is(Map.of("anno-1", "value-1", "anno-2", "value-2")));

        assertThat(roleBinding.getRoleRef(), is(roleRef));
        assertThat(roleBinding.getSubjects(), is(List.of(subject)));
    }
}
