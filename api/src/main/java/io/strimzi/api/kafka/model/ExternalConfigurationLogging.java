/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation for logging configuration
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@EqualsAndHashCode
public class ExternalConfigurationLogging implements Serializable, UnknownPropertyPreserving {

    private static final long serialVersionUID = 1L;

    private ConfigMapKeySelector configMapKeyRef;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Reference to the key in the ConfigMap containing the logging configuration.")
    @KubeLink(group = "core", version = "v1", kind = "configmapkeyselector")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public ConfigMapKeySelector getConfigMapKeyRef() {
        return configMapKeyRef;
    }

    public void setConfigMapKeyRef(ConfigMapKeySelector configMapKeyRef) {
        this.configMapKeyRef = configMapKeyRef;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}

