/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.Util;

import java.math.BigInteger;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;

/**
 * Certificate utility methods
 */
public class CertUtils {
    /**
     * Generates hash stub of the certificate which is used to track when the certificate changes and rolling update needs to be triggered.
     *
     * @param certSecret    Secrets with the certificate
     * @param key           Key under which the certificate is stored in the Secret
     *
     * @return              Hash stub of the certificate
     */
    public static String getCertificateThumbprint(Secret certSecret, String key) {
        try {
            X509Certificate cert = Ca.cert(certSecret, key);
            return Util.hashStub(cert.getEncoded());
        } catch (CertificateEncodingException e) {
            throw new RuntimeException("Failed to get certificate hashStub of " + key + " from Secret " + certSecret.getMetadata().getName(), e);
        }
    }

    /**
     * Generates the SHA1-hash of the server certificate which is used to track when the certificate changes and rolling update needs to be triggered.
     *
     * @param certSecret    Secrets with the certificate
     * @param podName       Server pod name
     *
     * @return              SHA1-Hash of the certificate or null if certSecret contains no valid X509Certificate
     */
    public static String getCertificateThumbprintForServer(Secret certSecret, String podName) {
        try {
            String hash = null;
            X509Certificate cert = Ca.cert(certSecret, Ca.SecretEntry.getNameForPod(podName, Ca.SecretEntry.CRT));
            if (cert != null) {
                hash = String.format("%040x", new BigInteger(1, Util.getSHA1(cert.getEncoded())));
            }
            return hash;
        } catch (CertificateEncodingException e) {
            throw new RuntimeException("Failed to get certificate thumbprint of " + podName + " from Secret " + certSecret.getMetadata().getName(), e);
        }
    }
}
