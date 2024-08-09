/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.kubernetes;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;

import java.util.Collections;

public class ConfigMapTemplates {

    /**
     * Creates a ConfigMap in a specified Kubernetes namespace.
     *
     * @param namespaceName     the name of the Kubernetes namespace where the ConfigMap will be created
     * @param configMapName     the name of the ConfigMap to be created
     * @param dataKey           the key for the data to be stored in the ConfigMap
     * @param dataValue         the value associated with the key in the ConfigMap
     */
    public static ConfigMap buildConfigMap(String namespaceName, String configMapName, String dataKey, String dataValue) {
        return new ConfigMapBuilder()
            .withApiVersion("v1")
            .withKind("ConfigMap")
            .withNewMetadata()
                .withName(configMapName)
                .withNamespace(namespaceName)
            .endMetadata()
            .withData(Collections.singletonMap(dataKey, dataValue))
            .build();
    }
}
