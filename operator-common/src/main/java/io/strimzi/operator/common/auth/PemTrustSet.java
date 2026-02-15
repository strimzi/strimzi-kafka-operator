/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.auth;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents the set of certificates to be trusted by a TLS client or server
 */
public class PemTrustSet {
    private final Map<String, byte[]> trustedCertificateMap;
    private final String secretName;
    private final String secretNamespace;

    /**
     * Constructs the PemTrustSet
     *
     * @param secret    Kubernetes Secret containing the trusted certificates
     */
    public PemTrustSet(Secret secret) {
        Objects.requireNonNull(secret, "Cannot extract trust set from null secret.");
        this.secretName = secret.getMetadata().getName();
        this.secretNamespace = secret.getMetadata().getNamespace();
        trustedCertificateMap = extractCerts(secret);
    }

    /**
     * Certificates to use in a TrustStore for TLS connections, with each certificate on a separate line.
     *
     * @return  The set of trusted certificates as a concatenated String
     */
    public String trustedCertificatesString() {
        return asX509Certificates()
                .stream()
                .map(cert -> {
                    try {
                        return Ca.x509CertificateToPem(cert);
                    } catch (CertificateEncodingException e) {
                        throw new RuntimeException("Failed to convert X509 certificate to PEM format: " + cert.getSubjectX500Principal().getName(), e);
                    }
                })
                .collect(Collectors.joining("\n"));
    }

    /**
     * TrustStore to use for TLS connections. This also validates each one is a valid certificate and
     * throws an exception if it is not.
     *
     * @return  TrustStore file in JKS format
     *
     * @throws GeneralSecurityException     If something goes wrong when creating the truststore
     *
     * @throws IOException  If there is an I/O or format problem with the data used to load the truststore.
     *                      This is not expected as the truststore is loaded with null parameter.
     */
    public KeyStore jksTrustStore() throws GeneralSecurityException, IOException {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(null);
        int aliasIndex = 0;
        for (X509Certificate certificate : asX509Certificates()) {
            trustStore.setEntry(certificate.getSubjectX500Principal().getName() + "-" + aliasIndex, new KeyStore.TrustedCertificateEntry(certificate), null);
            aliasIndex++;
        }
        return trustStore;
    }

    /**
     * Certificates to use in a TrustStore for TLS connections, with each certificate as a separate X509Certificate object.
     * This also validates each one is a valid certificate and throws an exception if it is not.
     *
     * @return  The list of trusted certificates as X509Certificate.
     */
    private List<X509Certificate> asX509Certificates() {
        return trustedCertificateMap.entrySet()
                .stream()
                .map(entry -> {
                    try {
                        return Ca.x509Certificate(entry.getValue());
                    } catch (CertificateException e) {
                        throw new RuntimeException("Bad/corrupt certificate found in data." + entry.getKey() + " of Secret "
                                + secretName + " in namespace " + secretNamespace);
                    }
                })
                .toList();
    }

    /**
     * Extract all public keys (all .crt records) from a secret.
     *
     * @param secret    Secret from which to extract the certificates
     */
    private Map<String, byte[]> extractCerts(Secret secret)  {
        Map<String, byte[]> certs = secret
                .getData()
                .entrySet()
                .stream()
                .filter(record -> Ca.SecretEntry.CRT.matchesType(record.getKey()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> Util.decodeBytesFromBase64(entry.getValue()))
                );

        if (certs.isEmpty()) {
            throw new RuntimeException("The Secret " + secretNamespace + "/" + secretName + " does not contain any fields with the suffix .crt");
        }

        return certs;
    }
}
