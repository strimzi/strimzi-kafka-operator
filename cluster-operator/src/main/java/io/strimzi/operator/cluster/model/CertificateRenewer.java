/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.common.model.Labels;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * CertificateRenewer is a class which takes the cluster CA cert as well as a secret and ensures the secret passed out
 * is signed by the CA, renewing the certificates if necessary
 */
public class CertificateRenewer {

    private final ClusterCa clusterCa;
    private final String commonName;
    private final String keyCertName;
    private final boolean isMaintenanceTimeWindowsSatisfied;

    protected static final Logger log = LogManager.getLogger(CertificateRenewer.class.getName());

    private CertificateRenewer(ClusterCa clusterCa, String commonName, String keyCertName, boolean isMaintenanceTimeWindowsSatisfied) {
        this.clusterCa = clusterCa;
        this.commonName = commonName;
        this.keyCertName = keyCertName;
        this.isMaintenanceTimeWindowsSatisfied = isMaintenanceTimeWindowsSatisfied;
    }

    /**
     * Pre-configure the CertificateRenewer for generating signed certificates
     *
     * @param clusterCa the cluster CA used to sign any new certificates
     * @param commonName the common-name used in te certificate
     * @param keyCertName the keyCertName used to index data in the secret
     * @param isMaintenanceTimeWindowsSatisfied a boolean of whether the time window is satisfied, if it is and the certificate
     *                                          is expired then a new one will be generated
     *
     * @return a certificate renewer capable of renewing a passed in Secret
     */
    public static CertificateRenewer of(ClusterCa clusterCa, String commonName, String keyCertName, boolean isMaintenanceTimeWindowsSatisfied) {
        return new CertificateRenewer(clusterCa, commonName, keyCertName, isMaintenanceTimeWindowsSatisfied);
    }

    /**
     * Return a new secret signed by the cluster CA
     *
     * @param secret the pre-existing secret, may be null
     * @param secretName the name of the secret to create or modify
     * @param namespace the namespace to write the secret to
     * @param labels the labels te secret should contain
     * @param ownerReference the OwnerReference for the secret
     *
     * @return signed certificate secret
     */
    public Secret signedCertificateSecret(Secret secret, String secretName, String namespace, Labels labels, OwnerReference ownerReference) {
        Map<String, String> signedCertData = new HashMap<>(4);
        CertAndKey certAndKey = null;

        Optional<CertAndKey> regeneratedCert = maybeRegenerateCertificate(secret);

        // TODO ensure empty check is fine
        if (regeneratedCert.isPresent()) {
            certAndKey = regeneratedCert.get();
        } else {
            if (hasKeystoreAndPassword(secret)) {
                certAndKey = getKeystoreAndPassword(secret);
            } else {
                try {
                    // coming from an older operator version, the secret exists but without keystore and password
                    certAndKey = clusterCa.addKeyAndCertToKeyStore(commonName,
                            decodeFromSecret(secret, keyCertName + ".key"),
                            decodeFromSecret(secret, keyCertName + ".crt"));
                } catch (IOException e) {
                    ModelUtils.log.error("Error generating the keystore for {}", keyCertName, e);
                }
            }
        }

        // Assuming exceptions haven't been thrown certAndKey should not be null
        if (certAndKey != null) {
            signedCertData.put(keyCertName + ".key", certAndKey.keyAsBase64String());
            signedCertData.put(keyCertName + ".crt", certAndKey.certAsBase64String());
            signedCertData.put(keyCertName + ".p12", certAndKey.keyStoreAsBase64String());
            signedCertData.put(keyCertName + ".password", certAndKey.storePasswordAsBase64String());
        }

        // return Secret with signed cert data
        return SecretGenerator.create(secretName, namespace, labels, ownerReference, signedCertData);
    }

    private CertAndKey getKeystoreAndPassword(Secret secret) {
        return new CertAndKey(
                decodeFromSecret(secret, keyCertName + ".key"),
                decodeFromSecret(secret, keyCertName + ".crt"),
                null,
                decodeFromSecret(secret, keyCertName + ".p12"),
                new String(decodeFromSecret(secret, keyCertName + ".password"), StandardCharsets.US_ASCII)
        );
    }

    private boolean hasKeystoreAndPassword(Secret secret) {
        return secret.getData().get(keyCertName + ".p12") != null &&
                !secret.getData().get(keyCertName + ".p12").isEmpty() &&
                secret.getData().get(keyCertName + ".password") != null &&
                !secret.getData().get(keyCertName + ".password").isEmpty();
    }

    /**
     * Regenerates certificate if required for any reason
     *
     * @return optional of new certificate and key of the generated certificate
     *         returns empty optional if renewal not required or throws an exception
     */
    private Optional<CertAndKey> maybeRegenerateCertificate(Secret secret) {
        CertAndKey certAndKey = null;
        if (secret == null) {
            log.debug("Certificate for pod {} need to be regenerated because: certificate doesn't exist yet", keyCertName);
            certAndKey = regenerateCertificate();
        } else {
            if (clusterCa.keyCreated() || clusterCa.certRenewed() || (isMaintenanceTimeWindowsSatisfied && clusterCa.isExpiring(secret, keyCertName + ".crt"))) {
                log.debug("Certificate for pod {} need to be regenerated because: certificate needs to be renewed", keyCertName);
                certAndKey = regenerateCertificate();
            }
        }
        return Optional.ofNullable(certAndKey);
    }

    private CertAndKey regenerateCertificate() {
        CertAndKey signedCert = null;
        try {
            signedCert = clusterCa.generateSignedCert(commonName, Ca.IO_STRIMZI);
        } catch (IOException e) {
            log.warn("Error while generating certificates", e);
        }
        log.debug("End generating certificates");
        return signedCert;
    }

    static byte[] decodeFromSecret(Secret secret, String key) {
        return Base64.getDecoder().decode(secret.getData().get(key));
    }
}
