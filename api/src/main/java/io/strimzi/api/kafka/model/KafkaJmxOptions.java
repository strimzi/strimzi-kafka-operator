/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"isEnabled"})
@EqualsAndHashCode
public class KafkaJmxOptions implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    private boolean isEnabled;

    @Description("Description for whether jmx options are enabled")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public boolean isEnabled() {
        return isEnabled;
    }

    public void setEnabled(boolean enabled) {
        isEnabled = enabled;
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

