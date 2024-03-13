/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.authentication;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Configures the Kafka client authentication using TLS client authentication in client based components
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "certificateAndKey"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaClientAuthenticationTls extends KafkaClientAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_TLS = "tls";

    private CertAndKeySecretSource certificateAndKey;

    @Description("Must be `" + TYPE_TLS + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_TLS;
    }

    @Description("Reference to the `Secret` which holds the certificate and private key pair.")
    public CertAndKeySecretSource getCertificateAndKey() {
        return certificateAndKey;
    }

    public void setCertificateAndKey(CertAndKeySecretSource certificateAndKey) {
        this.certificateAndKey = certificateAndKey;
    }
}
