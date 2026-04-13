/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the rack awareness configuration based on an environment variable configured on the Pod.
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "envVarName"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class EnvironmentVariableRack extends Rack {
    public static final String TYPE_ENVIRONMENT_VARIABLE = "environment-variable";

    private String envVarName;
    private Map<String, Object> additionalProperties;

    @Description("Must be `" + TYPE_ENVIRONMENT_VARIABLE + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_ENVIRONMENT_VARIABLE;
    }

    @Description("The name of the environment variable that defines the rack ID. " +
            "Its value sets the `broker.rack` configuration for Kafka brokers and the `client.rack` configuration for Kafka Connect or MirrorMaker 2.")
    public String getEnvVarName() {
        return envVarName;
    }

    public void setEnvVarName(String envVarName) {
        this.envVarName = envVarName;
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
