/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Represents a single address of particular listener
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "address", "host", "port" })
@EqualsAndHashCode
@ToString(callSuper = true)
public class ListenerAddress implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private String host;
    private Integer port;
    private Map<String, Object> additionalProperties;

    @Description("The DNS name or IP address of Kafka bootstrap service")
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Description("The port of the Kafka bootstrap service")
    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>();
        }
        this.additionalProperties.put(name, value);
    }
}
