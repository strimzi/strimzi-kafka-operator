/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.cruisecontrol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Configures auto-rebalancing mode and corresponding configuration template
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"mode", "template"})
@EqualsAndHashCode
@ToString
public class KafkaAutoRebalanceConfiguration implements UnknownPropertyPreserving {

    private KafkaAutoRebalanceMode mode;
    private LocalObjectReference template;
    private Map<String, Object> additionalProperties;

    @Description("Specifies the mode for automatically rebalancing when brokers are added or removed. " +
            "Supported modes are `add-brokers` and `remove-brokers`. \n")
    @JsonProperty(required = true)
    public KafkaAutoRebalanceMode getMode() {
        return mode;
    }

    public void setMode(KafkaAutoRebalanceMode mode) {
        this.mode = mode;
    }

    @Description("Reference to the KafkaRebalance custom resource to be used as the configuration template " +
            "for the auto-rebalancing on scaling when running for the corresponding mode.")
    public LocalObjectReference getTemplate() {
        return template;
    }

    public void setTemplate(LocalObjectReference template) {
        this.template = template;
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
