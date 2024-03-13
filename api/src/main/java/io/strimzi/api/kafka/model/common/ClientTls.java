/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Represent the TLS configuration for all the Clients(KafkaConnect, KafkaBridge, KafkaMirrorMaker, KafkaMirrorMaker2).
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"trustedCertificates"})
@EqualsAndHashCode
@ToString
public class ClientTls implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private List<CertSecretSource> trustedCertificates;
    private Map<String, Object> additionalProperties;

    @Description("Trusted certificates for TLS connection")
    public List<CertSecretSource> getTrustedCertificates() {
        return trustedCertificates;
    }

    public void setTrustedCertificates(List<CertSecretSource> trustedCertificates) {
        this.trustedCertificates = trustedCertificates;
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
