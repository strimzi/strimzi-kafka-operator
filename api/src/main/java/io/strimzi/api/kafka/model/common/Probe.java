/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A representation of the configurable aspect of a probe (used for health checks).
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"initialDelaySeconds", "timeoutSeconds", "periodSeconds", "successThreshold", "failureThreshold"})
@EqualsAndHashCode
@ToString
public class Probe implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private int initialDelaySeconds = 15;
    private int timeoutSeconds = 5;
    private Integer periodSeconds;
    private Integer successThreshold;
    private Integer failureThreshold;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    public Probe() {
    }

    public Probe(int initialDelaySeconds, int timeoutSeconds) {
        this.initialDelaySeconds = initialDelaySeconds;
        this.timeoutSeconds = timeoutSeconds;
    }

    @Description("The initial delay before first the health is first checked. Default to 15 seconds. Minimum value is 0.")
    @Minimum(0)
    @JsonProperty(defaultValue = "15")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public int getInitialDelaySeconds() {
        return initialDelaySeconds;
    }

    public void setInitialDelaySeconds(int initialDelaySeconds) {
        this.initialDelaySeconds = initialDelaySeconds;
    }

    @Description("The timeout for each attempted health check. Default to 5 seconds. Minimum value is 1.")
    @Minimum(1)
    @JsonProperty(defaultValue = "5")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    @Description("How often (in seconds) to perform the probe. Default to 10 seconds. Minimum value is 1.")
    @Minimum(1)
    @JsonProperty(defaultValue = "10")
    public Integer getPeriodSeconds() {
        return periodSeconds;
    }

    public void setPeriodSeconds(Integer periodSeconds) {
        this.periodSeconds = periodSeconds;
    }

    @Description("Minimum consecutive successes for the probe to be considered successful after having failed. Defaults to 1. Must be 1 for liveness. Minimum value is 1.")
    @Minimum(1)
    @JsonProperty(defaultValue = "1")
    public Integer getSuccessThreshold() {
        return successThreshold;
    }

    public void setSuccessThreshold(Integer successThreshold) {
        this.successThreshold = successThreshold;
    }

    @Description("Minimum consecutive failures for the probe to be considered failed after having succeeded. Defaults to 3. Minimum value is 1.")
    @Minimum(1)
    @JsonProperty(defaultValue = "3")
    public Integer getFailureThreshold() {
        return failureThreshold;
    }

    public void setFailureThreshold(Integer failureThreshold) {
        this.failureThreshold = failureThreshold;
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
