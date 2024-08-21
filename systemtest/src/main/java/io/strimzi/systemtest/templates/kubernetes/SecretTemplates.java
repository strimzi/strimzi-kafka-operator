/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.kubernetes;

import io.fabric8.kubernetes.api.model.SecretBuilder;

import java.util.Collections;

public class SecretTemplates {

    private SecretTemplates() {}

    public static SecretBuilder secret(String namespaceName, String secretName, String dataKey, String dataValue) {
        return new SecretBuilder()
            .withNewMetadata()
                .withName(secretName)
                .withNamespace(namespaceName)
            .endMetadata()
            .withType("Opaque")
            .withStringData(Collections.singletonMap(dataKey, dataValue));
    }
}
