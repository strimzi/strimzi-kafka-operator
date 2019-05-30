/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaBridgeConfigurationSpec implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private String bootstrapServers;
    private KafkaBridgeTls tls;
    private KafkaBridgeAuthentication authentication;
    private Map<String, Object> additionalProperties;

    @Description("Authentication configuration for connecting to the cluster.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public KafkaBridgeAuthentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(KafkaBridgeAuthentication authentication) {
        this.authentication = authentication;
    }

    @Description("TLS configuration for connecting to the cluster.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaBridgeTls getTls() {
        return tls;
    }

    public void setTls(KafkaBridgeTls tls) {
        this.tls = tls;
    }

    @Description("A list of host:port pairs to use for establishing the initial connection to the Kafka cluster.")
    @JsonProperty(required = true)
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
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
