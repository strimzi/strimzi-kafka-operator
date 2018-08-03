/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

/**
 * Configures the Kafka Connect authentication
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaConnectAuthenticationTls extends KafkaConnectAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_TLS = "tls";

    private CertAndKeySecretSource certificate;

    @Description("Must be `" + TYPE_TLS + "`")
    @Override
    public String getType() {
        return TYPE_TLS;
    }

    @Description("Certificate and private key pair for TLS authentication.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public CertAndKeySecretSource getCertificate() {
        return certificate;
    }

    public void setCertificate(CertAndKeySecretSource certificate) {
        this.certificate = certificate;
    }
}
