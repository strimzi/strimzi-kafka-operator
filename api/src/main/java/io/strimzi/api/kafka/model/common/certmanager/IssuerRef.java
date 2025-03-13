/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.certmanager;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Description("Reference to the cert-manager issuer for TLS certificates. " +
        "This only applies if the CA type is set to `cert-manager.io`.")
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "name", "kind", "group" })
@EqualsAndHashCode
@ToString
public class IssuerRef implements UnknownPropertyPreserving {
    private String name;
    private IssuerKind kind;
    private String group = "cert-manager.io";
    private Map<String, Object> additionalProperties;

    @Description("The name of the cert-manager issuer. " +
            "Required.")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Description("The kind of the cert-manager issuer. " +
            "Must be either `Issuer` or `ClusterIssuer`. " +
            "Required.")
    public IssuerKind getKind() {
        return kind;
    }

    public void setKind(IssuerKind kind) {
        this.kind = kind;
    }

    @Description("The group of the cert-manager issuer. " +
            "Default is `cert-manager.io`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
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
