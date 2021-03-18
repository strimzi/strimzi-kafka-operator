/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class ClusterRoleBindingTemplates {

    private static final Logger LOGGER = LogManager.getLogger(ClusterRoleBindingTemplates.class);

    public static List<ClusterRoleBinding> clusterRoleBindingsForAllNamespaces(String namespace) {
        LOGGER.info("Creating ClusterRoleBinding that grant cluster-wide access to all OpenShift projects");
        return clusterRoleBindingsForAllNamespaces(namespace, "strimzi-cluster-operator");
    }

    public static List<ClusterRoleBinding> clusterRoleBindingsForAllNamespaces(String namespace, String coName) {
        LOGGER.info("Creating ClusterRoleBinding that grant cluster-wide access to all OpenShift projects");

        List<ClusterRoleBinding> kCRBList = new ArrayList<>();

        kCRBList.add(
            new ClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName(coName + "-namespaced")
                .endMetadata()
                .withNewRoleRef()
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .withName("strimzi-cluster-operator-namespaced")
                .endRoleRef()
                .withSubjects(new SubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName("strimzi-cluster-operator")
                    .withNamespace(namespace)
                    .build()
                )
                .build()
        );

        kCRBList.add(
            new ClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName(coName + "-entity-operator")
                .endMetadata()
                .withNewRoleRef()
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .withName("strimzi-entity-operator")
                .endRoleRef()
                .withSubjects(new SubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName("strimzi-cluster-operator")
                    .withNamespace(namespace)
                    .build()
                )
                .build()
        );

        kCRBList.add(
            new ClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName(coName + "-topic-operator")
                .endMetadata()
                .withNewRoleRef()
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .withName("strimzi-topic-operator")
                .endRoleRef()
                .withSubjects(new SubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName("strimzi-cluster-operator")
                    .withNamespace(namespace)
                    .build()
                )
                .build()
        );
        return kCRBList;
    }
}
