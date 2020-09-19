/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Configures external listener overrides for broker services and advertised addresses
 */
@JsonPropertyOrder({"broker", "advertisedHost", "advertisedPort"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode
public class ExternalListenerBrokerOverride implements Serializable, UnknownPropertyPreserving {

    private static final long serialVersionUID = 1L;

    private Integer broker;
    private String advertisedHost;
    private Integer advertisedPort;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Id of the kafka broker (broker identifier)")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getBroker() {
        return broker;
    }

    public void setBroker(Integer broker) {
        this.broker = broker;
    }

    @Description("The host name which will be used in the brokers' `advertised.brokers`")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getAdvertisedHost() {
        return advertisedHost;
    }

    public void setAdvertisedHost(String advertisedHost) {
        this.advertisedHost = advertisedHost;
    }

    @Description("The port number which will be used in the brokers' `advertised.brokers`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getAdvertisedPort() {
        return advertisedPort;
    }

    public void setAdvertisedPort(Integer advertisedPort) {
        this.advertisedPort = advertisedPort;
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
