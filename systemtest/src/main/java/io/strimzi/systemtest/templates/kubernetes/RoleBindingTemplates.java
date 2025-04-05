/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.strimzi.test.ReadWriteUtils;

public class RoleBindingTemplates {
    public static RoleBinding roleBindingFromFile(String mainNamespaceName, String creationNamespace, String pathToYaml) {
        RoleBinding roleBinding = ReadWriteUtils.readObjectFromYamlFilepath(pathToYaml, RoleBinding.class);

        return new RoleBindingBuilder(roleBinding)
            .editMetadata()
                .withNamespace(creationNamespace)
            .endMetadata()
            .editFirstSubject()
                .withNamespace(mainNamespaceName)
            .endSubject()
            .build();
    }
}
