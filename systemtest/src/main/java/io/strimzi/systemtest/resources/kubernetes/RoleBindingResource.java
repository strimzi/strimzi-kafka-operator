package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.rbac.DoneableRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
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
        return "RoleBinding";
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
    public void delete(RoleBinding resource) throws Exception {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteRoleBinding(resource.getMetadata().getName());
    }
    @Override
    public boolean isReady(RoleBinding resource) {
        return resource != null;
    }
    @Override
    public void refreshResource(RoleBinding existing, RoleBinding newResource) {
        existing.setMetadata(newResource.getMetadata());
    }

    public static DoneableRoleBinding roleBinding(ExtensionContext extensionContext, String yamlPath, String namespace) {
        LOGGER.info("Creating RoleBinding from {} in namespace {}", yamlPath, namespace);
        RoleBinding roleBinding = getRoleBindingFromYaml(yamlPath);
        return roleBinding(extensionContext, new RoleBindingBuilder(roleBinding)
            .editFirstSubject()
            .withNamespace(namespace)
            .endSubject().build());
    }

    private static DoneableRoleBinding roleBinding(ExtensionContext extensionContext, RoleBinding roleBinding) {
        ResourceManager.getInstance().createResource(extensionContext, roleBinding);
        return new DoneableRoleBinding(roleBinding);
    }

    private static RoleBinding getRoleBindingFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, RoleBinding.class);
    }
}
