/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.SecretCertProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.Labels;

import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.CertificateEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyMap;

/**
 * Certificate utility methods
 */
public class CertUtils {

    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CertUtils.class.getName());

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
            var cert = Ca.cert(certSecret, key);
            return cert == null ? null : String.format("%040x", new BigInteger(1, Util.sha1Digest(cert.getEncoded())));
        } catch (CertificateEncodingException e) {
            throw new RuntimeException("Failed to get certificate thumbprint of " + key + " from Secret " + certSecret.getMetadata().getName(), e);
        }
    }

    /**
     * Builds a clusterCa certificate secret for the different Strimzi components (TO, UO, KE, ...)
     *
     * @param reconciliation                        Reconciliation marker
     * @param clusterCa                             The Cluster CA
     * @param secret                                Kubernetes Secret
     * @param namespace                             Namespace
     * @param secretName                            Name of the Kubernetes secret
     * @param commonName                            Common Name of the certificate
     * @param keyCertName                           Key under which the certificate will be stored in the new Secret
     * @param labels                                Labels
     * @param ownerReference                        Owner reference
     * @param isMaintenanceTimeWindowsSatisfied     Flag whether we are inside a maintenance window or not
     *
     * @return  Newly built Secret
     */
    public static Secret buildTrustedCertificateSecret(Reconciliation reconciliation, ClusterCa clusterCa, Secret secret, String namespace, String secretName,
                                                       String commonName, String keyCertName, Labels labels, OwnerReference ownerReference, boolean isMaintenanceTimeWindowsSatisfied) {
        CertAndKey certAndKey = null;
        boolean shouldBeRegenerated = false;
        List<String> reasons = new ArrayList<>(2);

        if (secret == null) {
            reasons.add("certificate doesn't exist yet");
            shouldBeRegenerated = true;
        } else {
            if (clusterCa.keyCreated() || clusterCa.certRenewed() ||
                    (isMaintenanceTimeWindowsSatisfied && clusterCa.isExpiring(secret, keyCertName + SecretCertProvider.SecretEntry.CRT.getSuffix())) ||
                    clusterCa.hasCaCertGenerationChanged(secret)) {
                reasons.add("certificate needs to be renewed");
                shouldBeRegenerated = true;
            }
        }

        if (shouldBeRegenerated) {
            LOGGER.debugCr(reconciliation, "Certificate for pod {} need to be regenerated because: {}", keyCertName, String.join(", ", reasons));

            try {
                certAndKey = clusterCa.generateSignedCert(commonName, Ca.IO_STRIMZI);
            } catch (IOException e) {
                LOGGER.warnCr(reconciliation, "Error while generating certificates", e);
            }

            LOGGER.debugCr(reconciliation, "End generating certificates");
        } else {
            CertAndKey keyStoreCertAndKey = SecretCertProvider.keyStoreCertAndKey(secret, keyCertName);
            if (keyStoreCertAndKey.keyStore().length != 0 &&
                    keyStoreCertAndKey.storePassword() != null) {
                certAndKey = keyStoreCertAndKey;
            } else {
                try {
                    // coming from an older operator version, the secret exists but without keystore and password
                    certAndKey = clusterCa.addKeyAndCertToKeyStore(commonName,
                            keyStoreCertAndKey.key(),
                            keyStoreCertAndKey.cert());
                } catch (IOException e) {
                    LOGGER.errorCr(reconciliation, "Error generating the keystore for {}", keyCertName, e);
                }
            }
        }
        return ModelUtils.createSecret(secretName, namespace, labels, ownerReference, SecretCertProvider.buildSecretData(Collections.singletonMap(keyCertName, certAndKey)), clusterCa.caCertGenerationFullAnnotation(), emptyMap());
    }
}
