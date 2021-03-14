/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;

public class ClusterRoleBindingResource implements ResourceType<ClusterRoleBinding> {

    private static final Logger LOGGER = LogManager.getLogger(ClusterRoleBindingResource.class);

    @Override
    public String getKind() {
        return "ClusterRoleBinding";
    }
    @Override
    public ClusterRoleBinding get(String namespace, String name) {
        return ResourceManager.kubeClient().namespace(namespace).getClusterRoleBinding(name);
    }
    @Override
    public void create(ClusterRoleBinding resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).createOrReplaceClusterRoleBinding(resource);
    }
    @Override
    public void delete(ClusterRoleBinding resource) throws Exception {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteClusterRoleBinding(resource);
    }
    @Override
    public boolean isReady(ClusterRoleBinding resource) {
        return resource != null;
    }
    @Override
    public void replaceResource(ClusterRoleBinding existing, ClusterRoleBinding newResource) {
        existing.setMetadata(newResource.getMetadata());
    }

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

    public static ClusterRoleBinding clusterRoleBinding(ExtensionContext extensionContext, String yamlPath, String namespace) {
        LOGGER.info("Creating ClusterRoleBinding in test case {} from {} in namespace {}",
            extensionContext.getDisplayName(), yamlPath, namespace);
        ClusterRoleBinding clusterRoleBinding = getClusterRoleBindingFromYaml(yamlPath);
        clusterRoleBinding = new ClusterRoleBindingBuilder(clusterRoleBinding)
            .editFirstSubject()
                .withNamespace(namespace)
            .endSubject().build();

        ResourceManager.getInstance().createResource(extensionContext, clusterRoleBinding);

        return clusterRoleBinding;
    }

    public static ClusterRoleBinding clusterRoleBinding(ExtensionContext extensionContext, ClusterRoleBinding clusterRoleBinding) {
        ResourceManager.getInstance().createResource(extensionContext, clusterRoleBinding);
        return clusterRoleBinding;
    }

    private static ClusterRoleBinding getClusterRoleBindingFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, ClusterRoleBinding.class);
    }
}

