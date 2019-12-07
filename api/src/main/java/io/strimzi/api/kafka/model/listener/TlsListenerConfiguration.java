/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Configures Tls listeners
 */
@JsonPropertyOrder({"brokerCertAndKey"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    generateBuilderPackage = false,
    builderPackage = "io.fabric8.kubernetes.api.builder"
)
@EqualsAndHashCode
public class TlsListenerConfiguration implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private CertAndKeySecretSource brokerCertAndKey;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Reference to the `Secret` which holds the certificate and private key pair. " +
            "The certificate can optionally contain the whole chain.")
    public CertAndKeySecretSource getBrokerCertAndKey() {
        return brokerCertAndKey;
    }

    public void setBrokerCertAndKey(CertAndKeySecretSource brokerCertAndKey) {
        this.brokerCertAndKey = brokerCertAndKey;
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
