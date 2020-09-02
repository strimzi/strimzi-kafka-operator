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

    private final String name;
    private final String namespace;
    private final Labels labels;
    private final OwnerReference ownerReference;
    private final Map<String, String> data;

    private SecretGenerator(String name, String namespace, Labels labels, OwnerReference ownerReference, Map<String, String> data) {
        this.name = name;
        this.namespace = namespace;
        this.labels = labels;
        this.ownerReference = ownerReference;
        this.data = data;
    }

    public static SecretGenerator of(String name, String namespace, Labels labels, OwnerReference ownerReference, Map<String, String> data) {
        if (ownerReference == null) {
            throw new IllegalArgumentException();
        }
        return new SecretGenerator(name, namespace, labels, ownerReference, data);
    }

    public static SecretGenerator of(String name, String namespace, Labels labels, Map<String, String> data) {
        return new SecretGenerator(name, namespace, labels, null, data);
    }

    public Secret create() {
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

    public static Secret create(String name, String namespace, Labels labels, OwnerReference ownerReference, Map<String, String> data) {
        return of(name, namespace, labels, data)
                .create();
    }
}
