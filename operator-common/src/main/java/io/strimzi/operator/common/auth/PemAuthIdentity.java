/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.auth;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.Util;

import java.io.ByteArrayInputStream;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Objects;

/**
 * Represents the identity used during TLS client authentication.
 * This consists of an X509 end-entity certificate, corresponding private key, and a (possibly empty) chain of X509 intermediate CA certificates, all in PEM format.
 */
public class PemAuthIdentity {

    private final byte[] privateKeyAsPemBytes;
    private final byte[] certificateChainAsPemBytes;
    private final String secretCertName;
    private final String secretName;
    private final String secretNamespace;

    /**
     * Constructs the PemAuthIdentity.
     * @param secret Kubernetes Secret containing the Cluster Operator public and private key
     * @param secretCertName Key in the Kubernetes Secret that is associated with the requested identity
     */
    private PemAuthIdentity(Secret secret, String secretCertName) {
        Objects.requireNonNull(secret, "Cannot extract auth identity from null secret.");
        this.secretCertName = secretCertName;
        this.secretName = secret.getMetadata().getName();
        this.secretNamespace = secret.getMetadata().getNamespace();
        privateKeyAsPemBytes = Util.decodeBase64FieldFromSecret(secret, String.format("%s.key", secretCertName));
        certificateChainAsPemBytes = Util.decodeBase64FieldFromSecret(secret, String.format("%s.crt", secretCertName));
    }

    /**
     * Returns the instance of PemAuthIdentity that represents the identity of the
     * cluster operator during TLS client authentication.
     *
     * @param secret Kubernetes Secret containing the client authentication identity
     *
     * @return PemAuthIdentity to use as the client authentication identity during TLS authentication
     */
    public static PemAuthIdentity clusterOperator(Secret secret) {
        return new PemAuthIdentity(secret, "cluster-operator");
    }

    /**
     * Returns the instance of PemAuthIdentity that represents the identity of the
     * entity (i.e. user or topic) operator during TLS client authentication.
     *
     * @param secret Kubernetes Secret containing the client authentication identity
     *
     * @return PemAuthIdentity to use as the client authentication identity during TLS authentication
     */
    public static PemAuthIdentity entityOperator(Secret secret) {
        return new PemAuthIdentity(secret, "entity-operator");
    }

    /**
     * End-entity certificate and (possibly empty) chain of intermediate CA certificates for this authentication identity.
     * This also validates that the certificate chain is a valid X509 certificate.
     *
     * @return The certificate chain for this authentication identity as a X509Certificate
     */
    public X509Certificate certificateChain() {
        try {
            final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) certificateFactory.generateCertificate(new ByteArrayInputStream(certificateChainAsPemBytes));
        } catch (CertificateException e) {
            throw new RuntimeException("Bad/corrupt certificate found in data." + secretCertName + ".crt of Secret "
                    + secretName + " in namespace " + secretNamespace);
        }
    }

    /**
     * End-entity certificate and (possibly empty) chain of intermediate CA certificates for this authentication identity.
     *
     * @return The certificate chain for this authentication identity as a byte array
     */
    public byte[] certificateChainAsPemBytes() {
        return certificateChainAsPemBytes;
    }

    /**
     * End-entity certificate and (possibly empty) chain of intermediate CA certificates for this authentication identity.
     *
     * @return The certificate chain for this authentication identity as a String
     */
    public String certificateChainAsPem() {
        return Util.fromAsciiBytes(certificateChainAsPemBytes);
    }

    /**
     * Private key corresponding to the end-entity certificate for this authentication identity.
     *
     * @return The private key for this authentication identity as a byte array
     */
    public byte[] privateKeyAsPemBytes() {
        return privateKeyAsPemBytes;
    }

    /**
     * Private key corresponding to the end-entity certificate for this authentication identity.
     *
     * @return The private key for this authentication identity as a String
     */
    public String privateKeyAsPem() {
        return Util.fromAsciiBytes(privateKeyAsPemBytes);
    }
}
