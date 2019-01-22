/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.Objects;

/**
 * Configures external broker service and advertised addresses
 */
@JsonPropertyOrder({"index", "advertisedHost", "advertisedPort", "nodePort"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    generateBuilderPackage = false,
    builderPackage = "io.fabric8.kubernetes.api.builder"
)
public class KafkaExternalBrokerService implements Serializable {
    private static final long serialVersionUID = -7537621108590168627L;

    @JsonProperty("index")
    private Integer index;
    @JsonProperty("advertisedHost")
    private String advertisedHost;
    @JsonProperty("advertisedPort")
    private Integer advertisedPort;
    @JsonProperty("nodePort")
    private Integer nodePort;

    public KafkaExternalBrokerService() {
    }

    public KafkaExternalBrokerService(Integer index, String advertisedHost, Integer advertisedPort, Integer nodePort) {
        this.index = index;
        this.advertisedHost = advertisedHost;
        this.advertisedPort = advertisedPort;
        this.nodePort = nodePort;
    }

    @Description("Index of the kafka broker (broker ID)")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("index")
    public Integer getIndex() {
        return index;
    }

    @JsonProperty("index")
    public void setIndex(Integer index) {
        this.index = index;
    }

    @Description("External advertised host")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty("advertisedHost")
    public String getAdvertisedHost() {
        return advertisedHost;
    }

    @JsonProperty("advertisedHost")
    public void setAdvertisedHost(String advertisedHost) {
        this.advertisedHost = advertisedHost;
    }

    @Description("External advertised port")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("advertisedPort")
    public Integer getAdvertisedPort() {
        return advertisedPort;
    }

    @JsonProperty("advertisedPort")
    public void setAdvertisedPort(Integer advertisedPort) {
        this.advertisedPort = advertisedPort;
    }

    @Description("Broker service node port")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("nodePort")
    public Integer getNodePort() {
        return nodePort;
    }

    @JsonProperty("nodePort")
    public void setNodePort(Integer nodePort) {
        this.nodePort = nodePort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaExternalBrokerService that = (KafkaExternalBrokerService) o;
        return Objects.equals(index, that.index) &&
            Objects.equals(advertisedHost, that.advertisedHost) &&
            Objects.equals(advertisedPort, that.advertisedPort) &&
            Objects.equals(nodePort, that.nodePort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, advertisedHost, advertisedPort, nodePort);
    }

    @Override
    public String toString() {
        return "KafkaExternalBrokerService{" +
            "index=" + index +
            ", advertisedHost='" + advertisedHost + '\'' +
            ", advertisedPort=" + advertisedPort +
            ", nodePort=" + nodePort +
            '}';
    }
}
