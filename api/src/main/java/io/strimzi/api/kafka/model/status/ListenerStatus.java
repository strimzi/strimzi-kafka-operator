/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * Represents a single listener
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "type", "addresses", "bootstrapServers", "certificates" })
@EqualsAndHashCode
public class ListenerStatus implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private String type;
    private List<ListenerAddress> addresses;
    private String bootstrapServers;
    private List<String> certificates;
    private Map<String, Object> additionalProperties;

    @Description("The type of the listener. " +
            "Can be one of the following three types: `plain`, `tls`, and `external`.")
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Description("A list of the addresses for this listener.")
    public List<ListenerAddress> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<ListenerAddress> addresses) {
        this.addresses = addresses;
        if ((addresses == null) || addresses.isEmpty()) {
            bootstrapServers = null;
        } else {
            bootstrapServers = addresses.stream().map(a -> a.getHost() + ":" + a.getPort()).collect(Collectors.joining(","));
        }
    }

    @Description("A comma-separated list of `host:port` pairs for connecting to the Kafka cluster using this listener.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    @Description("A list of TLS certificates which can be used to verify the identity of the server when connecting " +
            "to the given listener. Set only for `tls` and `external` listeners.")
    public List<String> getCertificates() {
        return certificates;
    }

    public void setCertificates(List<String> certificates) {
        this.certificates = certificates;
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
