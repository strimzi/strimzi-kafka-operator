/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RoleBindingResource implements ResourceType<RoleBinding> {

    private static final Logger LOGGER = LogManager.getLogger(RoleBindingResource.class);

    @Override
    public String getKind() {
        return TestConstants.ROLE_BINDING;
    }
    @Override
    public RoleBinding get(String namespace, String name) {
        return ResourceManager.kubeClient().namespace(namespace).getRoleBinding(name);
    }
    @Override
    public void create(RoleBinding resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).createOrUpdateRoleBinding(resource);
    }
    @Override
    public void delete(RoleBinding resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteRoleBinding(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }

    @Override
    public void update(RoleBinding resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).createOrUpdateRoleBinding(resource);
    }

    @Override
    public boolean waitForReadiness(RoleBinding resource) {
        return resource != null;
    }

    public static void roleBinding(String yamlPath, String namespace, String clientNamespace) {
        LOGGER.info("Creating RoleBinding in test case {} from {} in Namespace: {}",
                ResourceManager.getTestContext().getDisplayName(), yamlPath, namespace);
        RoleBinding roleBinding = getRoleBindingFromYaml(yamlPath);

        ResourceManager.getInstance().createResourceWithWait(new RoleBindingBuilder(roleBinding)
            .editMetadata()
                .withNamespace(clientNamespace)
            .endMetadata()
            .editFirstSubject()
                .withNamespace(namespace)
            .endSubject()
            .build());
    }

    private static RoleBinding getRoleBindingFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, RoleBinding.class);
    }
}
