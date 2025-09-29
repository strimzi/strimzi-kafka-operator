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
@JsonPropertyOrder({"enabled", "maxRestarts", "maxBackoffMinutes"})
@EqualsAndHashCode
@ToString
public class AutoRestart implements UnknownPropertyPreserving {
    public static final int DEFAULT_MAX_BACKOFF_MINUTES = 60;
    
    private boolean enabled = true;
    private Integer maxRestarts;
    private Integer maxBackoffMinutes = DEFAULT_MAX_BACKOFF_MINUTES;

    private Map<String, Object> additionalProperties;

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
    
    @Description("The maximum backoff time in minutes between restarts. Defaults 60 minutes.")
    public Integer getMaxBackoffMinutes() { 
        return maxBackoffMinutes; 
    }

    public void setMaxBackoffMinutes(Integer maxBackoffMinutes) { 
        this.maxBackoffMinutes = maxBackoffMinutes; 
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
