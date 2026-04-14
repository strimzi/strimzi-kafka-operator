/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.crdgenerator.annotations.CelValidation;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the rack configuration.
 */
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    defaultImpl = TopologyLabelRack.class,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(name = TopologyLabelRack.TYPE_TOPOLOGY_LABEL, value = TopologyLabelRack.class),
    @JsonSubTypes.Type(name = EnvironmentVariableRack.TYPE_ENVIRONMENT_VARIABLE, value = EnvironmentVariableRack.class),
})
@CelValidation(rules = {
    @CelValidation.CelValidationRule(
        // When type is not specified or is set to topology-label, topologyKey is required
        rule = "(has(self.type) && self.type != \"topology-label\") || self.topologyKey != \"\"",
        message = "topologyKey property is required"
        ),
    @CelValidation.CelValidationRule(
        // When type is set to environment variable, envVarName is required
        rule = "has(self.type) == false || self.type != \"environment-variable\" || self.envVarName != \"\"",
        message = "envVarName property is required"
        )
})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
@ToString
public abstract class Rack implements UnknownPropertyPreserving {
    private Map<String, Object> additionalProperties;

    @Description("Specifies the rack awareness type. " +
            "Supported types are `topology-label` and `environment-variable`. " +
            "`topology-label` uses a Kubernetes worker node label to set the `broker.rack` configuration for Kafka brokers and the `client.rack` configuration for Kafka Connect and MirrorMaker 2. " +
            "`environment-variable` uses an environment variable to set the `broker.rack` configuration for Kafka brokers and the `client.rack` configuration for Kafka Connect and MirrorMaker 2. " +
            "When not specified, `topology-label` type is used by default.")
    public abstract String getType();

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
