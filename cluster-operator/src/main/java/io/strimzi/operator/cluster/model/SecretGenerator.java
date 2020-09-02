/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.operator.common.model.Labels;

import java.util.Map;

public class SecretGenerator {

    private SecretGenerator() { }

    public static Secret create(String name, String namespace, Labels labels, OwnerReference ownerReference, Map<String, String> data) {
        SecretBuilder secret = new SecretBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
                .withLabels(labels.toMap())
                .endMetadata()
                .withData(data);

        if (ownerReference != null) {
            secret.editMetadata()
                    .withOwnerReferences(ownerReference)
                    .endMetadata();
        }

        return secret.build();
    }
}
