/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;

@Description("Configuration of how TLS certificates are used within the cluster." +
        "This applies to certificates used for both internal communication within the cluster and to certificates " +
        "used for client access via `Kafka.spec.kafka.listeners.tls`.")
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "generateCertificateAuthority", "validityDays", "renewalDays" })
public class TlsCertificates implements Serializable {

    private static final long serialVersionUID = 1L;

    int validityDays;
    boolean generateCertificateAuthority = true;
    int renewalDays;

    @Description("The number of days generated certificates should be valid for. Default is 365.")
    @Minimum(1)
    public int getValidityDays() {
        return validityDays;
    }

    public void setValidityDays(int validityDays) {
        this.validityDays = validityDays;
    }

    @Description("If true then Certificate Authority certificates will be generated automatically. " +
            "Otherwise the user will need to provide a Secret with the CA certificate. " +
            "Default is true.")
    public boolean isGenerateCertificateAuthority() {
        return generateCertificateAuthority;
    }

    public void setGenerateCertificateAuthority(boolean generateCertificateAuthority) {
        this.generateCertificateAuthority = generateCertificateAuthority;
    }

    @Description("The number of days in the certificate renewal period. " +
            "This is the number of days before the a certificate expires during which renewal actions may be performed." +
            "When `generateCertificateAuthority` is true, this will cause the generation of a new certificate. " +
            "When `generateCertificateAuthority` is true, this will cause extra logging at WARN level about the pending certificate expiry. " +
            "Default is 30.")
    @Minimum(1)
    public int getRenewalDays() {
        return renewalDays;
    }

    public void setRenewalDays(int renewalDays) {
        this.renewalDays = renewalDays;
    }
}
