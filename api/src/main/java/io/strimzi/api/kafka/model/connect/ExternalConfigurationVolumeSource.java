/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation for environment variables which will be passed to Kafka Connect
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"name", "secret", "configMap"})
@EqualsAndHashCode
@ToString
public class ExternalConfigurationVolumeSource implements Serializable, UnknownPropertyPreserving {

    private static final long serialVersionUID = 1L;

    private String name;
    private SecretVolumeSource secret;
    private ConfigMapVolumeSource configMap;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Name of the volume which will be added to the Kafka Connect pods.")
    @JsonProperty(required = true)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    // TODO: We should make it possible to generate a CRD configuring that exactly one of secret and configMap has to be defined.

    @Description("Reference to a key in a Secret. " +
            "Exactly one Secret or ConfigMap has to be specified.")
    @KubeLink(group = "core", version = "v1", kind = "secretvolumesource")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public SecretVolumeSource getSecret() {
        return secret;
    }

    public void setSecret(SecretVolumeSource secret) {
        this.secret = secret;
    }

    @Description("Reference to a key in a ConfigMap. " +
            "Exactly one Secret or ConfigMap has to be specified.")
    @KubeLink(group = "core", version = "v1", kind = "configmapvolumesource")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public ConfigMapVolumeSource getConfigMap() {
        return configMap;
    }

    public void setConfigMap(ConfigMapVolumeSource configMap) {
        this.configMap = configMap;
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

