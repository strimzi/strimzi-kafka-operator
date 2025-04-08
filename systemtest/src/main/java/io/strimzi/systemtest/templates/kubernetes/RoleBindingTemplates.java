/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.strimzi.test.ReadWriteUtils;

/**
 * Class for RoleBinding templates
 */
public class RoleBindingTemplates {

    /**
     * It reads the RoleBinding object from the {@param pathToFile}, then it returns the RoleBinding with first subject updated
     * with the Namespace name set to {@param mainNamespaceName} - where it should point - and creation Namespace updated to {@param creationNamespace}.
     *
     * @param mainNamespaceName     main Namespace name where the RoleBinding should point to
     * @param creationNamespace     name of the Namespace where the RoleBinding should be created
     * @param pathToFile            path to RoleBinding file
     *
     * @return  RoleBinding object updated with specified parameters
     */
    public static RoleBinding roleBindingFromFile(String mainNamespaceName, String creationNamespace, String pathToFile) {
        RoleBinding roleBinding = ReadWriteUtils.readObjectFromYamlFilepath(pathToFile, RoleBinding.class);

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
