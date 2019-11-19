/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker2;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation for external configuration for Kafka Mirror Maker 2 connectors passed from Secrets or ConfigMaps
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({ "env", "volumes" })
@EqualsAndHashCode
public class ExternalConfiguration implements Serializable, UnknownPropertyPreserving {

    private static final long serialVersionUID = 1L;

    private List<ExternalConfigurationEnv> env;
    private List<ExternalConfigurationVolumeSource> volumes;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Allows to pass data from Secret or ConfigMap to the Kafka Mirror Maker pods as environment variables.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public List<ExternalConfigurationEnv> getEnv() {
        return env;
    }

    public void setEnv(List<ExternalConfigurationEnv> env) {
        this.env = env;
    }

    @Description("Allows to pass data from Secret or ConfigMap to the Kafka Mirror Maker pods as volumes.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public List<ExternalConfigurationVolumeSource> getVolumes() {
        return volumes;
    }

    public void setVolumes(List<ExternalConfigurationVolumeSource> volumes) {
        this.volumes = volumes;
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

