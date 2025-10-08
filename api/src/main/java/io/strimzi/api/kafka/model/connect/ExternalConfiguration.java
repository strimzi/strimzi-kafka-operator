/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.annotations.DeprecatedType;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

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
@Deprecated
@DeprecatedType(replacedWithType = KafkaConnectTemplate.class)
@PresentInVersions("v1beta2")
@EqualsAndHashCode
@ToString
public class ExternalConfiguration implements UnknownPropertyPreserving {
    private List<ExternalConfigurationEnv> env;
    private List<ExternalConfigurationVolumeSource> volumes;
    private Map<String, Object> additionalProperties;

    @Description("Makes data from a Secret or ConfigMap available in the Kafka Connect pods as environment variables.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    @Deprecated
    @DeprecatedProperty(description = "The external configuration environment variables are deprecated and will be removed in the future. " +
            "Please use the environment variables in a container template instead.")
    public List<ExternalConfigurationEnv> getEnv() {
        return env;
    }

    public void setEnv(List<ExternalConfigurationEnv> env) {
        this.env = env;
    }

    @Description("Makes data from a Secret or ConfigMap available in the Kafka Connect pods as volumes.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    @Deprecated
    @DeprecatedProperty(description = "The external configuration volumes are deprecated and will be removed in the future. " +
            "Please use the additional volumes and volume mounts in pod and container templates instead to mount additional secrets or config maps.")
    public List<ExternalConfigurationVolumeSource> getVolumes() {
        return volumes;
    }

    public void setVolumes(List<ExternalConfigurationVolumeSource> volumes) {
        this.volumes = volumes;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
