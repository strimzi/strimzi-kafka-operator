/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

/**
 * Represents a status of the Kafka Connect resource
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "conditions", "observedGeneration", "url", "connectorPlugins" })
@EqualsAndHashCode
@ToString(callSuper = true)
public class KafkaConnectStatus extends Status {
    private static final long serialVersionUID = 1L;

    private String url;
    private List<ConnectorPlugin> connectorPlugins;
    private int replicas;
    private LabelSelector podSelector;

    @Description("The URL of the REST API endpoint for managing and monitoring Kafka Connect connectors.")
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The list of connector plugins available in this Kafka Connect deployment.")
    public List<ConnectorPlugin> getConnectorPlugins() {
        return connectorPlugins;
    }

    public void setConnectorPlugins(List<ConnectorPlugin> connectorPlugins) {
        this.connectorPlugins = connectorPlugins;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Total number of pods representing this resource.")
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Label selector for pods representing this resource.")
    public LabelSelector getPodSelector() {
        return podSelector;
    }

    public void setPodSelector(LabelSelector podSelector) {
        this.podSelector = podSelector;
    }
}
