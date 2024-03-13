/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker2;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.ClientTls;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "alias", "bootstrapServers", "tls", "authentication", "config"})
@EqualsAndHashCode
@ToString
public class KafkaMirrorMaker2ClusterSpec implements UnknownPropertyPreserving, Serializable {

    public static final String FORBIDDEN_PREFIXES = "ssl., sasl., security., listeners, plugin.path, rest., bootstrap.servers, consumer.interceptor.classes, producer.interceptor.classes";
    public static final String FORBIDDEN_PREFIX_EXCEPTIONS = "ssl.endpoint.identification.algorithm, ssl.cipher.suites, ssl.protocol, ssl.enabled.protocols";

    private static final long serialVersionUID = 1L;

    private String alias;
    private String bootstrapServers;
    protected Map<String, Object> config = new HashMap<>(0);
    private ClientTls tls;
    private KafkaClientAuthentication authentication;
    private Map<String, Object> additionalProperties;

    @Description("Alias used to reference the Kafka cluster.")
    @JsonProperty(required = true)
    @Pattern("^[a-zA-Z0-9\\._\\-]{1,100}$")
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

    @Description("The MirrorMaker 2 cluster config. Properties with the following prefixes cannot be set: " + FORBIDDEN_PREFIXES + " (with the exception of: " + FORBIDDEN_PREFIX_EXCEPTIONS + ").")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Description("TLS configuration for connecting MirrorMaker 2 connectors to a cluster.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public ClientTls getTls() {
        return tls;
    }

    public void setTls(ClientTls tls) {
        this.tls = tls;
    }

    @Description("A comma-separated list of `host:port` pairs for establishing the connection to the Kafka cluster.")
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
            this.additionalProperties = new HashMap<>(1);
        }
        this.additionalProperties.put(name, value);
    }
}
