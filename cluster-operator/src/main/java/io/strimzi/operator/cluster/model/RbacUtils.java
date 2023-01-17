/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.strimzi.api.kafka.model.template.ResourceTemplate;
import io.strimzi.operator.common.model.Labels;

import java.util.List;

/**
 * Shared methods for working with RBAC resources
 */
public class RbacUtils {
    /**
     * Creates a Role with rules passed as parameter
     *
     * @param name              Name of the Role
     * @param namespace         Namespace of the Role
     * @param rules             The list of rules associated with this role
     * @param labels            Labels of the Role
     * @param ownerReference    OwnerReference of the Role
     * @param template          PDB template with user's custom configuration
     *
     * @return The role for the component.
     */
    public static Role createRole(String name, String namespace, List<PolicyRule> rules, Labels labels, OwnerReference ownerReference, ResourceTemplate template) {
        return new RoleBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withOwnerReferences(ownerReference)
                    .withLabels(labels.withAdditionalLabels(TemplateUtils.labels(template)).toMap())
                    .withAnnotations(TemplateUtils.annotations(template))
                .endMetadata()
                .withRules(rules)
                .build();
    }

    /**
     * Creates RoleBinding for a role and subjects passed as parameters
     *
     * @param name              Name of the RoleBinding
     * @param namespace         Namespace of the RoleBinding
     * @param roleRef           Role reference
     * @param subjects          Subjects which should be bound
     * @param labels            Labels of the RoleBinding
     * @param ownerReference    OwnerReference of the RoleBinding
     * @param template          PDB template with user's custom configuration
     *
     * @return  The RoleBinding with the given name, namespace, role reference and subjects
     */
    public static RoleBinding createRoleBinding(String name, String namespace, RoleRef roleRef, List<Subject> subjects, Labels labels, OwnerReference ownerReference, ResourceTemplate template) {
        return new RoleBindingBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withOwnerReferences(ownerReference)
                    .withLabels(labels.withAdditionalLabels(TemplateUtils.labels(template)).toMap())
                    .withAnnotations(TemplateUtils.annotations(template))
                .endMetadata()
                .withRoleRef(roleRef)
                .withSubjects(subjects)
                .build();
    }

    /**
     * Creates ClusterRoleBinding for a role and subjects passed as parameters
     *
     * @param name      Name of the ClusterRoleBinding
     * @param roleRef   Role reference
     * @param subjects  Subjects which should be bound
     * @param labels    Labels of the ClusterRoleBinding
     * @param template  PDB template with user's custom configuration
     *
     * @return The RoleBinding with the given name, namespace, role reference and subjects
     */
    public static ClusterRoleBinding createClusterRoleBinding(String name, RoleRef roleRef, List<Subject> subjects, Labels labels, ResourceTemplate template) {
        return new ClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.withAdditionalLabels(TemplateUtils.labels(template)).toMap())
                    .withAnnotations(TemplateUtils.annotations(template))
                .endMetadata()
                .withSubjects(subjects)
                .withRoleRef(roleRef)
                .build();
    }
}