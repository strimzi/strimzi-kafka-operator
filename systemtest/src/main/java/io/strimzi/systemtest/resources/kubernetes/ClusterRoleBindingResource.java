/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class ClusterRoleBindingResource implements ResourceType<ClusterRoleBinding> {

    private static final Logger LOGGER = LogManager.getLogger(ClusterRoleBindingResource.class);

    @Override
    public String getKind() {
        return TestConstants.CLUSTER_ROLE_BINDING;
    }
    @Override
    public ClusterRoleBinding get(String namespace, String name) {
        // ClusterRoleBinding his operation namespace is only 'default'
        return kubeClient(KubeClusterResource.getInstance().defaultNamespace()).getClusterRoleBinding(name);
    }
    @Override
    public void create(ClusterRoleBinding resource) {
        kubeClient(KubeClusterResource.getInstance().defaultNamespace()).createOrUpdateClusterRoleBinding(resource);
    }
    @Override
    public void delete(ClusterRoleBinding resource) {
        // ClusterRoleBinding his operation namespace is only 'default'
        kubeClient(KubeClusterResource.getInstance().defaultNamespace()).deleteClusterRoleBinding(resource);
    }

    @Override
    public void update(ClusterRoleBinding resource) {
        kubeClient(KubeClusterResource.getInstance().defaultNamespace()).createOrUpdateClusterRoleBinding(resource);
    }

    @Override
    public boolean waitForReadiness(ClusterRoleBinding resource) {
        return resource != null;
    }

    public static ClusterRoleBinding clusterRoleBinding(String yamlPath, String namespace) {
        LOGGER.info("Creating ClusterRoleBinding in test case {} from {} in Namespace: {}",
            ResourceManager.getTestContext().getDisplayName(), yamlPath, namespace);
        ClusterRoleBinding clusterRoleBinding = getClusterRoleBindingFromYaml(yamlPath);
        clusterRoleBinding = new ClusterRoleBindingBuilder(clusterRoleBinding)
            .editFirstSubject()
            .withNamespace(namespace)
            .endSubject().build();

        ResourceManager.getInstance().createResourceWithWait(clusterRoleBinding);

        return clusterRoleBinding;
    }

    public static ClusterRoleBinding clusterRoleBinding(ClusterRoleBinding clusterRoleBinding) {
        ResourceManager.getInstance().createResourceWithWait(clusterRoleBinding);
        return clusterRoleBinding;
    }

    private static ClusterRoleBinding getClusterRoleBindingFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, ClusterRoleBinding.class);
    }
}

