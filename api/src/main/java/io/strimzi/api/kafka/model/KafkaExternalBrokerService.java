/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.Objects;

/**
 * Configures external broker service and advertised addresses
 */
@JsonPropertyOrder({"broker", "advertisedHost", "advertisedPort", "nodePort"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    generateBuilderPackage = false,
    builderPackage = "io.fabric8.kubernetes.api.builder"
)
public class KafkaExternalBrokerService implements Serializable {
    private static final long serialVersionUID = -7537621108590168627L;

    private Integer broker;
    private String advertisedHost;
    private Integer advertisedPort;
    private Integer nodePort;

    public KafkaExternalBrokerService() {
    }

    public KafkaExternalBrokerService(Integer broker, String advertisedHost, Integer advertisedPort, Integer nodePort) {
        this.broker = broker;
        this.advertisedHost = advertisedHost;
        this.advertisedPort = advertisedPort;
        this.nodePort = nodePort;
    }

    @Description("Index of the kafka broker (broker identifier)")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getBroker() {
        return broker;
    }

    public void setBroker(Integer broker) {
        this.broker = broker;
    }

    @Description("Externally advertised host")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getAdvertisedHost() {
        return advertisedHost;
    }

    public void setAdvertisedHost(String advertisedHost) {
        this.advertisedHost = advertisedHost;
    }

    @Description("Externally advertised port")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getAdvertisedPort() {
        return advertisedPort;
    }

    public void setAdvertisedPort(Integer advertisedPort) {
        this.advertisedPort = advertisedPort;
    }

    @Description("Broker service node port")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getNodePort() {
        return nodePort;
    }

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
        return Objects.equals(broker, that.broker) &&
            Objects.equals(advertisedHost, that.advertisedHost) &&
            Objects.equals(advertisedPort, that.advertisedPort) &&
            Objects.equals(nodePort, that.nodePort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(broker, advertisedHost, advertisedPort, nodePort);
    }

    @Override
    public String toString() {
        return "KafkaExternalBrokerService{" +
            "broker=" + broker +
            ", advertisedHost='" + advertisedHost + '\'' +
            ", advertisedPort=" + advertisedPort +
            ", nodePort=" + nodePort +
            '}';
    }
}
