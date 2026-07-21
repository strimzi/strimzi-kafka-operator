/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CertificateUtils;

import java.math.BigInteger;
import java.security.cert.CertificateEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods for handling Secret resource that contain certificates
 */
public class CertSecretUtils {
    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CertSecretUtils.class.getName());

    private CertSecretUtils() { }

    /**
     * Generates a short SHA1-hash (a hash stub) of the certificate which is used to track when the certificate changes and rolling update needs to be triggered.
     *
     * @param certSecret    Secrets with the certificate
     * @param key           Key under which the certificate is stored in the Secret
     * @return              Hash stub of the certificate
     */
    public static String getCertificateShortThumbprint(Secret certSecret, String key) {
        var thumbprint = getCertificateThumbprint(certSecret, key);
        return thumbprint == null ? null : thumbprint.substring(0, Util.HASH_STUB_LENGTH);
    }

    /**
     * Generates the full SHA1-hash of the server certificate which is used to track when the certificate changes and rolling update needs to be triggered.
     *
     * @param certSecret    Secrets with the certificate
     * @param key           Key under which the certificate is stored in the Secret
     * @return              SHA1-Hash of the certificate or null if certSecret contains no valid X509Certificate
     */
    public static String getCertificateThumbprint(Secret certSecret, String key) {
        try {
            var cert = CertificateUtils.cert(certSecret, key);
            return cert == null ? null : String.format("%040x", new BigInteger(1, Util.sha1Digest(cert.getEncoded())));
        } catch (CertificateEncodingException e) {
            throw new RuntimeException("Failed to get certificate thumbprint of " + key + " from Secret " + certSecret.getMetadata().getName(), e);
        }
    }

    /**
     * Constructs a Map containing the provided certificates to be stored in a Kubernetes Secret.
     *
     * @param certificates to store
     * @return Map of certificate identifier to base64 encoded certificate or key
     */
    public static Map<String, String> buildSecretData(Map<String, CertAndKey> certificates) {
        Map<String, String> data = new HashMap<>(certificates.size() * 4);
        certificates.forEach((keyCertName, certAndKey) -> {
            data.put(Ca.SecretEntry.KEY.asKey(keyCertName), certAndKey.keyAsBase64String());
            data.put(Ca.SecretEntry.CRT.asKey(keyCertName), certAndKey.certAsBase64String());
        });
        return data;
    }

    /**
     * Constructs a Map containing the provided certificates to be stored in a Kubernetes Secret.
     *
     * @param certificateName Name to use to create identifier for storing the data in the Secret.
     * @param certAndKey Private key and public cert pair to store in the Secret.
     *
     * @return Map of certificate identifier to base64 encoded certificate or key
     */
    public static Map<String, String> buildSecretData(String certificateName, CertAndKey certAndKey) {
        return Map.of(
                Ca.SecretEntry.KEY.asKey(certificateName), certAndKey.keyAsBase64String(),
                Ca.SecretEntry.CRT.asKey(certificateName), certAndKey.certAsBase64String()
        );
    }

    private static byte[] decodeFromSecret(Secret secret, String key) {
        if (secret.getData().get(key) != null && !secret.getData().get(key).isEmpty()) {
            return Util.decodeBytesFromBase64(secret.getData().get(key));
        } else {
            return new byte[]{};
        }
    }

    /**
     * Extracts the KeyStore from the Kubernetes Secret as a CertAndKey
     * @param secret to extract certificate and key from
     * @param keyCertName name of the KeyStore
     * @param caCertGenerationAnnotation Annotation for the CA certificate generation that signed this certificate
     * @return the KeyStore as a CertAndKey including the CA certificate generation that signed this certificate.
     * Returned object has empty truststore and may have empty key, cert or keystore and null store password.
     */
    public static CertAndKey keyStoreCertAndKey(Secret secret, String keyCertName, String caCertGenerationAnnotation) {
        if (secret == null) {
            return null;
        }
        int caCertGeneration = Annotations.intAnnotation(secret, caCertGenerationAnnotation, Ca.INIT_GENERATION);
        return new CertAndKey(decodeFromSecret(secret, Ca.SecretEntry.KEY.asKey(keyCertName)),
                decodeFromSecret(secret, Ca.SecretEntry.CRT.asKey(keyCertName)), caCertGeneration);
    }
}
