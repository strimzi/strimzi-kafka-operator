/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connector;

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
import java.util.Map;

/**
 * Representation of the AutoRestart configuration.
 */
@DescriptionFile
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"enabled", "maxRestarts"})
@EqualsAndHashCode
@ToString
public class AutoRestart implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private boolean enabled = true;
    private Integer maxRestarts;

    private final Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Whether automatic restart for failed connectors and tasks should be enabled or disabled")
    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Description("The maximum number of connector restarts that the operator will try. " +
            "If the connector remains in a failed state after reaching this limit, it must be restarted manually by the user. " +
            "Defaults to an unlimited number of restarts.")
    public Integer getMaxRestarts() {
        return maxRestarts;
    }

    public void setMaxRestarts(Integer maxRestarts) {
        this.maxRestarts = maxRestarts;
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