/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.strimzi.test.ReadWriteUtils;

public class RoleTemplates {
    public static Role roleFromFile(String namespaceName, String pathToYaml) {
        Role role = ReadWriteUtils.readObjectFromYamlFilepath(pathToYaml, Role.class);

        return new RoleBuilder(role)
            .editMetadata()
                .withNamespace(namespaceName)
            .endMetadata()
            .build();
    }
}
