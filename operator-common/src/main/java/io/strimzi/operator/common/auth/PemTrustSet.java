/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.auth;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;

import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents the set of certificates to be trusted by a TLS client or server
 */
public class PemTrustSet {
    private static final String CERT_SUFFIX = ".crt";
    private final Map<String, byte[]> trustedCertificateMap;
    private final String secretName;
    private final String secretNamespace;

    /**
     * Constructs the PemTrustSet
     * @param secret Kubernetes Secret containing the trusted certificates
     */
    public PemTrustSet(Secret secret) {
        Objects.requireNonNull(secret, "Cannot extract trust set from null secret.");
        this.secretName = secret.getMetadata().getName();
        this.secretNamespace = secret.getMetadata().getNamespace();
        trustedCertificateMap = extractCerts(secret);
    }

    /**
     * Certificates to use in a TrustStore for TLS connections.
     * This also validates that each certificate is a valid X509 certificate and throws an exception if it is not.
     * @return The set of trusted certificates as X509Certificate objects
     */
    public Set<X509Certificate> trustedCertificates() {
        return new HashSet<>(asX509Certificates().values());
    }

    /**
     * Certificates to use in a TrustStore for TLS connections.
     * @return The set of trusted certificates as byte arrays
     */
    public Set<byte[]> trustedCertificatesBytes() {
        return new HashSet<>(trustedCertificateMap.values());
    }

    /**
     * Certificates to use in a TrustStore for TLS connections, with each certificate on a separate line.
     * @return The set of trusted certificates as a concatenated String
     */
    public String trustedCertificatesString() {
        return trustedCertificateMap.values()
                .stream()
                .map(bytes -> new String(bytes, StandardCharsets.US_ASCII))
                .collect(Collectors.joining("\n"));
    }

    /**
     * Certificates to use in a TrustStore for TLS connections, with each certificate as a separate X509Certificate object.
     * This also validates each one is a valid certificate and throws an exception if it is not.
     * @return The set of trusted certificates as X509Certificate.
     */
    private Map<String, X509Certificate> asX509Certificates() {
        return trustedCertificateMap.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            try {
                                return Ca.x509Certificate(entry.getValue());
                            } catch (CertificateException e) {
                                throw new RuntimeException("Bad/corrupt certificate found in data." + entry.getKey() + ".crt of Secret "
                                        + secretName + " in namespace " + secretNamespace);
                            }
                        }
                ));
    }

    /**
     * Extract all public keys (all .crt records) from a secret.
     */
    private Map<String, byte[]> extractCerts(Secret secret)  {
        Map<String, byte[]> certs = secret
                .getData()
                .entrySet()
                .stream()
                .filter(record -> record.getKey().endsWith(CERT_SUFFIX))
                .collect(Collectors.toMap(
                        entry -> stripCertKeySuffix(entry.getKey()),
                        entry -> Util.decodeBytesFromBase64(entry.getValue()))
                );
        if (certs.isEmpty()) {
            throw new RuntimeException("The Secret " + secretNamespace + "/" + secretName + " does not contain any fields with the suffix .crt");
        }
        return certs;
    }

    private static String stripCertKeySuffix(String key) {
        return key.substring(0, key.length() - CERT_SUFFIX.length());
    }
}
