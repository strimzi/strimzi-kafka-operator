/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.certmanager;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Description("Configuration for using cert-manager to issue certificates. " +
        "This only applies if the CA type is set to `cert-manager`.")
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "issuerRef", "caCertRef" })
@EqualsAndHashCode
@ToString
public class CertManager implements UnknownPropertyPreserving {
    private IssuerRef issuerRef;
    private CaCertRef caCertRef;
    private Map<String, Object> additionalProperties;

    @Description("Reference to the cert-manager issuer to use for issuing certificates. " +
            "Required.")
    @JsonProperty(required = true)
    public IssuerRef getIssuerRef() {
        return issuerRef;
    }

    public void setIssuerRef(IssuerRef issuerRef) {
        this.issuerRef = issuerRef;
    }

    @Description("Reference to the Secret containing the CA certificate (public key) " +
            "that trusts certificates issued by cert-manager. " +
            "Required.")
    @JsonProperty(required = true)
    public CaCertRef getCaCertRef() {
        return caCertRef;
    }

    public void setCaCertRef(CaCertRef caCertRef) {
        this.caCertRef = caCertRef;
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
