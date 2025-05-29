/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.rebalance;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Configures the progress section of the status of a `KafkaRebalance` resource with progress information
 * related to a partition rebalance.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonPropertyOrder({"rebalanceProgressConfigMap"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
@ToString
public class KafkaRebalanceProgress implements UnknownPropertyPreserving {

    private String rebalanceProgressConfigMap;
    private Map<String, Object> additionalProperties;

    @Description("The name of the `ConfigMap` containing information related to the progress of a partition rebalance.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getRebalanceProgressConfigMap() {
        return rebalanceProgressConfigMap;
    }

    public void setRebalanceProgressConfigMap(String rebalanceProgressConfigMap) {
        this.rebalanceProgressConfigMap = rebalanceProgressConfigMap;
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

