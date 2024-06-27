/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.OneOf;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a certificate inside a Secret
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"secretName", "certificate", "pattern"})
@OneOf({@OneOf.Alternative(@OneOf.Alternative.Property("certificate")), @OneOf.Alternative(@OneOf.Alternative.Property("pattern"))})
@EqualsAndHashCode
@ToString
public class CertSecretSource implements UnknownPropertyPreserving {
    private String secretName;
    private String certificate;
    private String pattern;
    private Map<String, Object> additionalProperties;

    @Description("The name of the Secret containing the certificate.")
    @JsonProperty(required = true)
    public String getSecretName() {
        return secretName;
    }

    public void setSecretName(String secretName) {
        this.secretName = secretName;
    }

    @Description("The name of the file certificate in the secret.")
    public String getCertificate() {
        return certificate;
    }

    public void setCertificate(String certificate) {
        this.certificate = certificate;
    }

    @Description("Pattern for the certificate files in the secret. " +
            "Use the link:https://en.wikipedia.org/wiki/Glob_(programming)[_glob syntax_] for the pattern. " +
            "All files in the secret that match the pattern are used.")
    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
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
