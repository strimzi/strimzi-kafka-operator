/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.strimzi.api.kafka.model.authentication.KafkaClientAuthentication;
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
@JsonPropertyOrder({ "alias", "bootstrapServers"})
@EqualsAndHashCode
public class KafkaMirrorMaker2ClusterSpec implements UnknownPropertyPreserving, Serializable {

    public static final String FORBIDDEN_PREFIXES = "ssl., bootstrap.servers, group.id, sasl., security., interceptor.classes";
    public static final String FORBIDDEN_PREFIX_EXCEPTIONS = "ssl.endpoint.identification.algorithm";

    private static final long serialVersionUID = 1L;

    private String alias;
    private String bootstrapServers;
    protected Map<String, Object> config = new HashMap<>(0);
    private KafkaMirrorMaker2Tls tls;
    private KafkaClientAuthentication authentication;
    private Map<String, Object> additionalProperties;

    @Description("Alias used to reference the Kafka cluster.")
    @JsonProperty(required = true)
    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Description("Authentication configuration for connecting to the cluster.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaClientAuthentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(KafkaClientAuthentication authentication) {
        this.authentication = authentication;
    }


    @Description("The Mirror Maker 2 cluster config. Properties with the following prefixes cannot be set: " + FORBIDDEN_PREFIXES)
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Description("TLS configuration for connecting Mirror Maker 2 connectors to a cluster.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaMirrorMaker2Tls getTls() {
        return tls;
    }

    public void setTls(KafkaMirrorMaker2Tls tls) {
        this.tls = tls;
    }

    @Description("A list of host:port pairs for establishing the connection to the Kafka cluster.")
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
