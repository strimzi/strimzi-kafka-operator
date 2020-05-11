/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Abstracts connector config. Connectors for MM2 do not have the {@code className} property
 * while {@code KafkaConnectors} must have it.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"pause", "tasksMax", "config"})
@EqualsAndHashCode
public abstract class AbstractConnectorSpec implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;
    private static final String FORBIDDEN_PARAMETERS = "connector.class, tasks.max";

    private Integer tasksMax;
    private Boolean pause;
    private Map<String, Object> config = new HashMap<>(0);
    private Map<String, Object> additionalProperties;

    @Description("The maximum number of tasks for the Kafka Connector")
    @Minimum(1)
    public Integer getTasksMax() {
        return tasksMax;
    }

    public void setTasksMax(Integer tasksMax) {
        this.tasksMax = tasksMax;
    }

    @Description("The Kafka Connector configuration. The following properties cannot be set: " + FORBIDDEN_PARAMETERS)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> getConfig() {
        return config;
    }
    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Description("Whether the connector should be paused. Defaults to false.")
    public Boolean getPause() {
        return pause;
    }

    public void setPause(Boolean pause) {
        this.pause = pause;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(1);
        }
        this.additionalProperties.put(name, value);
    }
}
