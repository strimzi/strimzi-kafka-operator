/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connector;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.Map;

/**
 * Represents a status of the KafkaConnector resource
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "conditions", "observedGeneration", "autoRestart", "connectorStatus", "tasksMax", "topics" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaConnectorStatus extends Status {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> connectorStatus;
    private int tasksMax;
    private List<String> topics;
    private AutoRestartStatus autoRestart;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The connector status, as reported by the Kafka Connect REST API.")
    public Map<String, Object> getConnectorStatus() {
        return connectorStatus;
    }

    public void setConnectorStatus(Map<String, Object> connectorStatus) {
        this.connectorStatus = connectorStatus;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The maximum number of tasks for the Kafka Connector.")
    public int getTasksMax() {
        return tasksMax;
    }

    public void setTasksMax(int tasksMax) {
        this.tasksMax = tasksMax;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The list of topics used by the Kafka Connector.")
    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The auto restart status")
    public AutoRestartStatus getAutoRestart() {
        return autoRestart;
    }

    public void setAutoRestart(AutoRestartStatus autoRestart) {
        this.autoRestart = autoRestart;
    }
}
