/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation for external configuration for Kafka Connect connectors passed from Secrets or ConfigMaps
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({ "env", "volumes" })
@EqualsAndHashCode
@ToString
public class ExternalConfiguration implements Serializable, UnknownPropertyPreserving {

    private static final long serialVersionUID = 1L;

    private List<ExternalConfigurationEnv> env;
    private List<ExternalConfigurationVolumeSource> volumes;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Makes data from a Secret or ConfigMap available in the Kafka Connect pods as environment variables.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public List<ExternalConfigurationEnv> getEnv() {
        return env;
    }

    public void setEnv(List<ExternalConfigurationEnv> env) {
        this.env = env;
    }

    @Description("Makes data from a Secret or ConfigMap available in the Kafka Connect pods as volumes.")
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
