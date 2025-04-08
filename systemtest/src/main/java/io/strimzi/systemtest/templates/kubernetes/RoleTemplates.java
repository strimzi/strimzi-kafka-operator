/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.strimzi.test.ReadWriteUtils;

/**
 * Class containing Role templates
 */
public class RoleTemplates {
    /**
     * It reads the Role object from the {@param pathToFile}, then it returns the Role with updated creation Namespace (in metadata.namespace).
     *
     * @param namespaceName     main Namespace name where the RoleBinding should point to
     * @param pathToFile        path to Role file
     *
     * @return  Role object updated with specified parameters
     */
    public static Role roleFromFile(String namespaceName, String pathToFile) {
        Role role = ReadWriteUtils.readObjectFromYamlFilepath(pathToFile, Role.class);

        return new RoleBuilder(role)
            .editMetadata()
                .withNamespace(namespaceName)
            .endMetadata()
            .build();
    }
}
