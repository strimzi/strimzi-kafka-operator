/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a connector plugin in Kafka Connect (not in Kafka Connect Build)
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({ "name", "artifacts" })
@DescriptionFile
@EqualsAndHashCode
@ToString
public class MountedPlugin implements UnknownPropertyPreserving {
    private String name;
    private List<MountedArtifact> artifacts;
    private Map<String, Object> additionalProperties;

    @Description("A unique name for the connector plugin. " +
            "This name is used to generate the mount path for the connector artifacts. " +
            "The name has to be unique within the KafkaConnect resource. " +
            "The name must be unique within the `KafkaConnect` resource and match the pattern: `^[a-z][-_a-z0-9]*[a-z]$`. " +
            "Required")
    @JsonProperty(required = true)
    @Pattern("^[a-z0-9][-_a-z0-9]*[a-z0-9]$")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Description("List of artifacts associated with this connector plugin. " +
            "Required.")
    @JsonProperty(required = true)
    public List<MountedArtifact> getArtifacts() {
        return artifacts;
    }

    public void setArtifacts(List<MountedArtifact> artifacts) {
        this.artifacts = artifacts;
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

