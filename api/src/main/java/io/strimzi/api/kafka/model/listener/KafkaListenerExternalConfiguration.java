/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
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
 * Configures External listeners
 */

@JsonPropertyOrder({"brokerCertChainAndKey"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode
public class KafkaListenerExternalConfiguration implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private CertAndKeySecretSource brokerCertChainAndKey;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Reference to the `Secret` which holds the certificate and private key pair. " +
            "The certificate can optionally contain the whole chain.")
    public CertAndKeySecretSource getBrokerCertChainAndKey() {
        return brokerCertChainAndKey;
    }

    public void setBrokerCertChainAndKey(CertAndKeySecretSource brokerCertChainAndKey) {
        this.brokerCertChainAndKey = brokerCertChainAndKey;
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
