/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.strimzi.test.ReadWriteUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;

public class ClusterRoleBindingTemplates {

    private static final Logger LOGGER = LogManager.getLogger(ClusterRoleBindingTemplates.class);

    public static List<ClusterRoleBinding> clusterRoleBindingsForAllNamespaces(String namespaceName) {
        LOGGER.info("Creating ClusterRoleBinding that grant cluster-wide access to all OpenShift projects");
        return clusterRoleBindingsForAllNamespaces(namespaceName, "strimzi-cluster-operator");
    }

    public static List<ClusterRoleBinding> clusterRoleBindingsForAllNamespaces(String namespaceName, String coName) {
        LOGGER.info("Creating ClusterRoleBinding that grant cluster-wide access to all OpenShift projects");

        return Arrays.asList(
            getClusterOperatorNamespacedCrb(namespaceName, coName),
            getClusterOperatorEntityOperatorCrb(namespaceName, coName),
            getClusterOperatorWatchedCrb(namespaceName, coName)
        );
    }

    public static ClusterRoleBinding getClusterOperatorNamespacedCrb(final String namespaceName, final String coName) {
        return new ClusterRoleBindingBuilder()
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
                .withNamespace(namespaceName)
                .build()
            )
            .build();
    }

    public static ClusterRoleBinding getClusterOperatorEntityOperatorCrb(final String namespaceName, final String coName) {
        return new ClusterRoleBindingBuilder()
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
                .withNamespace(namespaceName)
                .build()
            )
            .build();
    }

    public static ClusterRoleBinding getClusterOperatorWatchedCrb(final String namespaceName, final String coName) {
        return new ClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName(coName + "-watched")
                .endMetadata()
                .withNewRoleRef()
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("ClusterRole")
                    .withName("strimzi-cluster-operator-watched")
                .endRoleRef()
                .withSubjects(new SubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName("strimzi-cluster-operator")
                    .withNamespace(namespaceName)
                    .build()
                )
                .build();
    }


    /**
     * It reads the ClusterRoleBinding object from the {@param pathToFile}, then it returns the CRB with first subject updated
     * with the Namespace name set to {@param namespaceName}.
     *
     * @param namespaceName     name of the Namespace that should be used for the first subject
     * @param pathToFile        path to the CRB file
     *
     * @return  CRB object from {@param pathToFile} updated with {@param namespaceName} set in the first subject
     */
    public static ClusterRoleBinding clusterRoleBindingFromFile(String namespaceName, String pathToFile) {
        ClusterRoleBinding clusterRoleBinding = ReadWriteUtils.readObjectFromYamlFilepath(pathToFile, ClusterRoleBinding.class);

        return new ClusterRoleBindingBuilder(clusterRoleBinding)
            .editFirstSubject()
                .withNamespace(namespaceName)
            .endSubject()
            .build();
    }
}
