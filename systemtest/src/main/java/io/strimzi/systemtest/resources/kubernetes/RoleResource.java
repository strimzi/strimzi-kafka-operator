/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RoleResource implements ResourceType<Role> {

    private static final Logger LOGGER = LogManager.getLogger(RoleResource.class);

    @Override
    public String getKind() {
        return Constants.ROLE;
    }
    @Override
    public Role get(String namespace, String name) {
        return ResourceManager.kubeClient().namespace(namespace).getRole(name);
    }
    @Override
    public void create(Role resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).createOrReplaceRole(resource);
    }
    @Override
    public void delete(Role resource) {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteRole(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }
    @Override
    public boolean waitForReadiness(Role resource) {
        return resource != null;
    }

    public static Role role(String yamlPath, String namespace) {
        LOGGER.info("Creating Role from {} in namespace {}", yamlPath, namespace);
        Role role = getRoleFromYaml(yamlPath);

        return createRole(
            new RoleBuilder(role)
                .editMetadata()
                    .withNamespace(namespace)
                .endMetadata()
                .build(),
                namespace);
    }

    public static Role createRole(Role role, String clientNamespace) {
        ResourceManager.kubeClient().namespace(clientNamespace).createOrReplaceRole(role);
        return role;
    }

    private static Role getRoleFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, Role.class);
    }
}
