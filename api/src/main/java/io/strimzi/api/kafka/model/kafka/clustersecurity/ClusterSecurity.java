/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.clustersecurity;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.RequiredInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the Cluster Security configuration.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"authentication", "encryption"})
@EqualsAndHashCode
@ToString
public class ClusterSecurity implements UnknownPropertyPreserving {
    private ClusterSecurityEncryption encryption;
    private ClusterSecurityAuthentication authentication;
    private Map<String, Object> additionalProperties;

    @Description("Encryption configuration of the Kafka cluster's internal communication.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @RequiredInVersions("v1+")
    public ClusterSecurityEncryption getEncryption() {
        return encryption;
    }

    public void setEncryption(ClusterSecurityEncryption encryption) {
        this.encryption = encryption;
    }

    @Description("Authentication configuration of the Kafka cluster's internal communication.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @RequiredInVersions("v1+")
    public ClusterSecurityAuthentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(ClusterSecurityAuthentication authentication) {
        this.authentication = authentication;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
