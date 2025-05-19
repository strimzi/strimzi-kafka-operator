/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.agent;

import io.fabric8.kubernetes.api.model.Secret;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


/**
 * Class with various utility methods for generating KeyStore and TrustStore for KafkaAgent
 */
public class KafkaAgentUtils {

    /**
     * Creates TrustStore to use for TLS connections from the given Secret. This also validates each one is a valid certificate and
     * throws an exception if it is not.
     *
     * @param secret Secret containing the TrustStore certificates
     * @return TrustStore file in JKS format
     * @throws GeneralSecurityException if something goes wrong when creating the truststore
     * @throws IOException if there is an I/O or format problem with the data used to load the truststore.
     * This is not expected as the truststore is loaded with null parameter.
     */
    static KeyStore jksTrustStore(Secret secret) throws GeneralSecurityException, IOException {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(null);
        int aliasIndex = 0;
        for (X509Certificate certificate : asX509Certificates(extractCerts(secret), secret.getMetadata().getName(), secret.getMetadata().getNamespace()).values()) {
            trustStore.setEntry(certificate.getSubjectX500Principal().getName() + "-" + aliasIndex, new KeyStore.TrustedCertificateEntry(certificate), null);
            aliasIndex++;
        }
        return trustStore;
    }

    /**
     * Creates KeyStore to use for TLS connections from the given Secret.
     *
     * @param secret Secret containing private key and certificate
     *
     * @return KeyStore file in JKS format
     * @throws GeneralSecurityException if something goes wrong when creating the truststore
     * @throws IOException if there is an I/O or format problem with the data used to load the truststore.
     */
    static KeyStore jksKeyStore(Secret secret) throws GeneralSecurityException, IOException {
        String secretName = secret.getMetadata().getName();
        String strippedPrivateKey = new String(decodeBase64FieldFromSecret(secret, secretName + ".key"), StandardCharsets.US_ASCII)
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replaceAll(System.lineSeparator(), "")
                .replace("-----END PRIVATE KEY-----", "");
        byte[] decodedKey = Base64.getDecoder().decode(strippedPrivateKey);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decodedKey);
        final KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        final PrivateKey key = keyFactory.generatePrivate(keySpec);

        X509Certificate certificateChain = x509Certificate(decodeBase64FieldFromSecret(secret, secretName + ".crt"));
        KeyStore nodeKeyStore = KeyStore.getInstance("JKS");
        nodeKeyStore.load(null);
        nodeKeyStore.setKeyEntry(secret.getMetadata().getName(), key, "changeit".toCharArray(), new Certificate[]{certificateChain});
        return nodeKeyStore;
    }

    /**
     * Extract all public keys (all .crt records) from a secret.
     */
    private static Map<String, byte[]> extractCerts(Secret secret)  {
        Map<String, byte[]> certs = secret
                .getData()
                .entrySet()
                .stream()
                .filter(record -> record.getKey().endsWith(".crt"))
                .collect(Collectors.toMap(
                        entry -> stripCertKeySuffix(entry.getKey()),
                        entry -> Base64.getDecoder().decode(entry.getValue()))
                );
        if (certs.isEmpty()) {
            throw new RuntimeException("The Secret " + secret.getMetadata().getNamespace() + "/" + secret.getMetadata().getName() + " does not contain any fields with the suffix .crt");
        }
        return certs;
    }

    private static String stripCertKeySuffix(String key) {
        return key.substring(0, key.length() - ".crt".length());
    }

    /**
     * Creates X509Certificate instance from a byte array containing a certificate.
     *
     * @param bytes     Bytes with the X509 certificate
     * @throws CertificateException     Thrown when the creation of the X509Certificate instance fails. Typically, this
     *                                  would happen because the bytes do not contain a valid X509 certificate.
     * @return  X509Certificate instance created based on the Certificate bytes
     */
    private static X509Certificate x509Certificate(byte[] bytes) throws CertificateException {
        final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        Certificate certificate = certificateFactory.generateCertificate(new ByteArrayInputStream(bytes));
        if (certificate instanceof X509Certificate) {
            return (X509Certificate) certificate;
        } else {
            throw new CertificateException("Not an X509Certificate: " + certificate);
        }
    }

    /**
     * Decode binary item from Kubernetes Secret from base64 into byte array
     *
     * @param secret    Kubernetes Secret
     * @param field     Field which should be retrieved and decoded
     * @return          Decoded bytes
     */
    private static byte[] decodeBase64FieldFromSecret(Secret secret, String field) {
        Objects.requireNonNull(secret);
        String data = secret.getData().get(field);
        if (data != null) {
            return Base64.getDecoder().decode(data);
        } else {
            throw new RuntimeException(String.format("The Secret %s/%s is missing the field %s",
                    secret.getMetadata().getNamespace(),
                    secret.getMetadata().getName(),
                    field));
        }
    }

    /**
     * Certificates to use in a TrustStore for TLS connections, with each certificate as a separate X509Certificate object.
     * This also validates each one is a valid certificate and throws an exception if it is not.
     * @return The set of trusted certificates as X509Certificate.
     */
    private static Map<String, X509Certificate> asX509Certificates(Map<String, byte[]> trustedCertificateMap, String secretName, String secretNamespace) {
        return trustedCertificateMap.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            try {
                                return x509Certificate(entry.getValue());
                            } catch (CertificateException e) {
                                throw new RuntimeException("Bad/corrupt certificate found in data." + entry.getKey() + ".crt of Secret "
                                        + secretName + " in namespace " + secretNamespace);
                            }
                        }
                ));
    }
}
