/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.Util;

import java.io.ByteArrayInputStream;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Optional;

/**
 * Represents the identity used during TLS client authentication.
 * This consists of an X509 end-entity certificate, corresponding private key, and a (possibly empty) chain of X509 intermediate CA certificates, all in PEM format.
 */
public class PemAuthIdentity {

    private final byte[] privateKeyAsPemBytes;
    private final byte[] certificateChainAsPemBytes;
    private final X509Certificate certificateChain;
    private String secretName;
    private String secretNamespace;

    /**
     * @param secret Kubernetes Secret containing the Cluster Operator public and private key
     * @param secretKey Key in the Kubernetes Secret that is associated with the requested identity
     */
    private PemAuthIdentity(Secret secret, String secretKey) {
        Optional.ofNullable(secret)
                .map(Secret::getMetadata)
                .ifPresent(objectMeta -> {
                    secretName = objectMeta.getName();
                    secretNamespace = objectMeta.getNamespace();
                });
        privateKeyAsPemBytes = Util.decodeBase64FieldFromSecret(secret, String.format("%s.key", secretKey));
        certificateChainAsPemBytes = Util.decodeBase64FieldFromSecret(secret, String.format("%s.crt", secretKey));
        certificateChain = validateCertificateChain(secretKey);
    }

    /**
     * Returns the instance of PemAuthIdentity that represents the identity of the
     * cluster operator during TLS client authentication. This also validates the provided
     * Secret contains a valid certificate chain.
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
     * entity (i.e. user or topic) operator during TLS client authentication. This also validates
     * the provided Secret contains a valid certificate chain.
     *
     * @param secret Kubernetes Secret containing the client authentication identity
     *
     * @return PemAuthIdentity to use as the client authentication identity during TLS authentication
     */
    public static PemAuthIdentity entityOperator(Secret secret) {
        return new PemAuthIdentity(secret, "entity-operator");
    }

    /**
     * @return The certificate chain for this authentication identity as a X509Certificate
     */
    public X509Certificate certificateChain() {
        return certificateChain;
    }

    /**
     * @return The certificate chain for this authentication identity as a byte array
     */
    public byte[] certificateChainAsPemBytes() {
        return certificateChainAsPemBytes;
    }

    /**
     * @return The certificate chain for this authentication identity as a String
     */
    public String certificateChainAsPem() {
        return Util.fromAsciiBytes(certificateChainAsPemBytes);
    }

    /**
     * @return The private key for this authentication identity as a byte array
     */
    public byte[] privateKeyAsPemBytes() {
        return privateKeyAsPemBytes;
    }

    /**
     * @return The private key for this authentication identity as a String
     */
    public String privateKeyAsPem() {
        return Util.fromAsciiBytes(privateKeyAsPemBytes);
    }

    private X509Certificate validateCertificateChain(String secretKey) {
        try {
            final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) certificateFactory.generateCertificate(new ByteArrayInputStream(certificateChainAsPemBytes));
        } catch (CertificateException e) {
            throw Util.corruptCertificateException(secretNamespace, secretName, secretKey);
        }
    }
}
