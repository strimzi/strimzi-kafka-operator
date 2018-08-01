/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import io.vertx.core.cli.annotations.DefaultValue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A representation of the configurable aspect of a probe (used for health checks).
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Probe implements Serializable {

    private static final long serialVersionUID = 1L;

    private int initialDelaySeconds = 15;
    private int timeoutSeconds = 5;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    public Probe() {
    }

    public Probe(int initialDelaySeconds, int timeoutSeconds) {
        this.initialDelaySeconds = initialDelaySeconds;
        this.timeoutSeconds = timeoutSeconds;
    }

    @Description("The initial delay before first the health is first checked.")
    @Minimum(0)
    @DefaultValue("15")
    public int getInitialDelaySeconds() {
        return initialDelaySeconds;
    }

    public void setInitialDelaySeconds(int initialDelaySeconds) {
        this.initialDelaySeconds = initialDelaySeconds;
    }

    @Description("The timeout for each attempted health check.")
    @Minimum(0)
    @DefaultValue("5")
    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
