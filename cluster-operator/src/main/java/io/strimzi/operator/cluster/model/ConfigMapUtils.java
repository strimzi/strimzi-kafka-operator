/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.strimzi.operator.common.model.Labels;

import java.util.Map;

/**
 * Shared methods for working with Config Maps
 */
public class ConfigMapUtils {
    /**
     * Creates a Config Map
     *
     * @param name              Name of the Config Map
     * @param namespace         Namespace of the Config Map
     * @param labels            Labels of the Config Map
     * @param ownerReference    OwnerReference of the Config Map
     * @param data              Data which will be stored int he Config Map
     *
     * @return  New Config Map
     */
    public static ConfigMap createConfigMap(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            Map<String, String> data
    ) {
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withData(data)
                .build();
    }
}
