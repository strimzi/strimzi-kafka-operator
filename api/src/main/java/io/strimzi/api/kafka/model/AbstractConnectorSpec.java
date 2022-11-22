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

import java.util.HashMap;
import java.util.Map;

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
public abstract class AbstractConnectorSpec extends Spec {
    private static final long serialVersionUID = 1L;

    /**
     * Forbidden options in the connector configuration
     */
    public static final String FORBIDDEN_PARAMETERS = "connector.class, tasks.max";

    private Integer tasksMax;
    private Boolean pause;
    private Map<String, Object> config = new HashMap<>(0);

    private AutoRestart autoRestart;

    /**
     * @return  Max number of tasks
     */
    @Description("The maximum number of tasks for the Kafka Connector")
    @Minimum(1)
    public Integer getTasksMax() {
        return tasksMax;
    }

    /**
     * Sets the maximum number of tasks
     *
     * @param tasksMax  Max number of tasks
     */
    public void setTasksMax(Integer tasksMax) {
        this.tasksMax = tasksMax;
    }

    /**
     * @return  Connector configuration
     */
    @Description("The Kafka Connector configuration. The following properties cannot be set: " + FORBIDDEN_PARAMETERS)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> getConfig() {
        return config;
    }

    /**
     * Sets the connector configuration
     *
     * @param config    Map with the connector configuration
     */
    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    /**
     * @return  Flag indicating whether the connector should paused or not
     */
    @Description("Whether the connector should be paused. Defaults to false.")
    public Boolean getPause() {
        return pause;
    }

    /**
     * Sets the flag to indicate if the connector should be paused or not
     *
     * @param pause     Set to true to request the connector to be paused. False to have it running.
     */
    public void setPause(Boolean pause) {
        this.pause = pause;
    }

    /**
     * @return  Auto-restart configuration of this connector
     */
    @Description("Automatic restart of connector and tasks configuration")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public AutoRestart getAutoRestart() {
        return autoRestart;
    }

    /**
     * Configures auto-restarting of this connector
     *
     * @param autoRestart   Auto-restart configuration
     */
    public void setAutoRestart(AutoRestart autoRestart) {
        this.autoRestart = autoRestart;
    }
}
