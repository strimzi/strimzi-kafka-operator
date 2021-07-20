/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

public class RoleBindingResource implements ResourceType<RoleBinding> {

    private static final Logger LOGGER = LogManager.getLogger(RoleBindingResource.class);

    @Override
    public String getKind() {
        return Constants.ROLE_BINDING;
    }
    @Override
    public RoleBinding get(String namespace, String name) {
        return ResourceManager.kubeClient().namespace(namespace).getRoleBinding(name);
    }
    @Override
    public void create(RoleBinding resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).createOrReplaceRoleBinding(resource);
    }
    @Override
    public void delete(RoleBinding resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteRoleBinding(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }
    @Override
    public boolean waitForReadiness(RoleBinding resource) {
        return resource != null;
    }

    public static void roleBinding(ExtensionContext extensionContext, String yamlPath, String namespace, String clientNamespace) {
        LOGGER.info("Creating RoleBinding in test case {} from {} in namespace {}",
                extensionContext.getDisplayName(), yamlPath, namespace);
        RoleBinding roleBinding = getRoleBindingFromYaml(yamlPath);

        ResourceManager.getInstance().createResource(extensionContext, new RoleBindingBuilder(roleBinding)
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
