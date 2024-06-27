/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;

import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.CertificateEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
     * @param secret                                Existing Kubernetes certificate Secret containing the certificate to use if present and does not need renewing
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
    public static Secret buildTrustedCertificateSecret(Reconciliation reconciliation, ClusterCa clusterCa, Secret secret, String namespace,
                                                       String secretName, String commonName, String keyCertName,
                                                       Labels labels, OwnerReference ownerReference, boolean isMaintenanceTimeWindowsSatisfied) {
        boolean shouldBeRegenerated = false;
        List<String> reasons = new ArrayList<>(2);

        if (secret == null) {
            reasons.add("certificate doesn't exist yet");
            shouldBeRegenerated = true;
        } else {
            if (clusterCa.keyCreated()
                    || clusterCa.certRenewed()
                    || (isMaintenanceTimeWindowsSatisfied && clusterCa.isExpiring(secret, Ca.SecretEntry.CRT.asKey(keyCertName)))
                    || clusterCa.hasCaCertGenerationChanged(secret)) {
                reasons.add("certificate needs to be renewed");
                shouldBeRegenerated = true;
            }
        }

        CertAndKey certAndKey = null;
        if (shouldBeRegenerated) {
            LOGGER.debugCr(reconciliation, "Certificate for pod {} need to be regenerated because: {}", keyCertName, String.join(", ", reasons));

            try {
                certAndKey = clusterCa.generateSignedCert(commonName, Ca.IO_STRIMZI);
            } catch (IOException e) {
                LOGGER.warnCr(reconciliation, "Error while generating certificates", e);
            }

            LOGGER.debugCr(reconciliation, "End generating certificates");
        } else {
            certAndKey = keyStoreCertAndKey(secret, keyCertName);
        }

        Map<String, String> secretData = certAndKey == null ? Map.of() : buildSecretData(Map.of(keyCertName, certAndKey));

        return ModelUtils.createSecret(secretName, namespace, labels, ownerReference, secretData, Map.ofEntries(clusterCa.caCertGenerationFullAnnotation()), emptyMap());
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
     * @return the KeyStore as a CertAndKey. Returned object has empty truststore and
     * may have empty key, cert or keystore and null store password.
     */
    public static CertAndKey keyStoreCertAndKey(Secret secret, String keyCertName) {
        return new CertAndKey(decodeFromSecret(secret, Ca.SecretEntry.KEY.asKey(keyCertName)),
                decodeFromSecret(secret, Ca.SecretEntry.CRT.asKey(keyCertName)));
    }

    /**
     * Compares two Kubernetes Secrets with certificates and checks whether any value for a key which exists in both Secrets
     * changed. This method is used to evaluate whether rolling update of existing brokers is needed when secrets with
     * certificates change. It separates changes for existing certificates with other changes to the Secret such as
     * added or removed certificates (scale-up or scale-down).
     *
     * @param current   Existing secret
     * @param desired   Desired secret
     *
     * @return  True if there is a key which exists in the data sections of both secrets and which changed.
     */
    public static boolean doExistingCertificatesDiffer(Secret current, Secret desired) {
        Map<String, String> currentData = current.getData();
        Map<String, String> desiredData = desired.getData();

        if (currentData == null) {
            return true;
        } else {
            for (Map.Entry<String, String> entry : currentData.entrySet()) {
                String desiredValue = desiredData.get(entry.getKey());
                if (entry.getValue() != null
                        && desiredValue != null
                        && !entry.getValue().equals(desiredValue)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Creates the Secret Volumes for trusted TLS certificates
     *
     * @param volumeList            List where the volumes will be added
     * @param trustedCertificates   List of trusted certificates for TLS connection
     * @param isOpenShift           Indicates whether we run on OpenShift or not
     */
    public static void createTrustedCertificatesVolumes(List<Volume> volumeList, List<CertSecretSource> trustedCertificates, boolean isOpenShift) {
        createTrustedCertificatesVolumes(volumeList, trustedCertificates, isOpenShift, null);
    }

    /**
     * Creates the Secret Volumes for trusted TLS certificates
     *
     * @param volumeList            List where the volumes will be added
     * @param trustedCertificates   List of trusted certificates for TLS connection
     * @param isOpenShift           Indicates whether we run on OpenShift or not
     * @param prefix                Prefix used for the volume names to distinguish when the same Secret is mounted for
     *                              different purposes (e.g. different cluster connections in MM2)
     */
    public static void createTrustedCertificatesVolumes(List<Volume> volumeList, List<CertSecretSource> trustedCertificates, boolean isOpenShift, String prefix) {
        if (trustedCertificates != null && !trustedCertificates.isEmpty()) {
            for (CertSecretSource certSecretSource : trustedCertificates) {
                maybeAddTrustedCertificateVolume(volumeList, certSecretSource, isOpenShift, prefix);
            }
        }
    }

    /**
     * Creates the Volume used for trusted certificates and adds it to the list. It checks if the Secret where the
     * certificate is stored has been already used or not and makes sure to mount it only once to avoid duplicates and
     * naming conflicts.
     *
     * @param volumeList        List where the volume will be added
     * @param certSecretSource  Represents a certificate inside a Secret
     * @param isOpenShift       Indicates whether we run on OpenShift or not
     * @param prefix            Prefix used to generate the volume name
     */
    private static void maybeAddTrustedCertificateVolume(List<Volume> volumeList, CertSecretSource certSecretSource, boolean isOpenShift, String prefix) {
        String volumeName = trustedCertificateVolumeName(certSecretSource, prefix);

        // skipping if a volume with same name was already added
        if (volumeList.stream().noneMatch(v -> v.getName().equals(volumeName))) {
            volumeList.add(VolumeUtils.createSecretVolume(volumeName, certSecretSource.getSecretName(), isOpenShift));
        }
    }

    /**
     * Creates the volume mounts for Secrets with trusted TLS certificates
     *
     * @param volumeMountList       List where the volume mounts will be added
     * @param trustedCertificates   Trusted certificates for TLS connection
     * @param tlsVolumeMountPath    Path where the TLS certs should be mounted
     */
    public static void createTrustedCertificatesVolumeMounts(List<VolumeMount> volumeMountList, List<CertSecretSource> trustedCertificates, String tlsVolumeMountPath) {
        createTrustedCertificatesVolumeMounts(volumeMountList, trustedCertificates, tlsVolumeMountPath, null);
    }

    /**
     * Creates the volume mounts for Secrets with trusted TLS certificates
     *
     * @param volumeMountList       List where the volume mounts will be added
     * @param trustedCertificates   Trusted certificates for TLS connection
     * @param tlsVolumeMountPath    Path where the TLS certs should be mounted
     * @param prefix                Prefix used for the volume names to distinguish when the same Secret is mounted for
     *                              different purposes (e.g. different cluster connections in MM2)
     */
    public static void createTrustedCertificatesVolumeMounts(List<VolumeMount> volumeMountList, List<CertSecretSource> trustedCertificates, String tlsVolumeMountPath, String prefix) {
        if (trustedCertificates != null && !trustedCertificates.isEmpty()) {
            for (CertSecretSource certSecretSource : trustedCertificates) {
                maybeAddTrustedCertificateVolumeMount(volumeMountList, certSecretSource, tlsVolumeMountPath, prefix);
            }
        }
    }

    /**
     * Creates the VolumeMount used for trusted TLS certificates and adds it to the list. It checks if the Secret where
     * the certificate is stored has been already used or not and makes sure to mount it only once to avoid duplicates
     * and naming conflicts.
     *
     * @param volumeMountList       List where the volume mount will be added
     * @param certSecretSource      Represents a certificate inside a Secret
     * @param tlsVolumeMountPath    Path where the TLS certs should be mounted
     * @param prefix                Prefix used to generate the volume name
     */
    private static void maybeAddTrustedCertificateVolumeMount(List<VolumeMount> volumeMountList, CertSecretSource certSecretSource, String tlsVolumeMountPath, String prefix) {
        String volumeName = trustedCertificateVolumeName(certSecretSource, prefix);

        // skipping if a volume mount with same Secret name was already added
        if (volumeMountList.stream().noneMatch(vm -> vm.getName().equals(volumeName))) {
            volumeMountList.add(VolumeUtils.createVolumeMount(volumeName, tlsVolumeMountPath + certSecretSource.getSecretName()));
        }
    }

    /**
     * Generates the volume name that is used in the Volume definition and in the Volume mounts
     *
     * @param certSecretSource      Represents a certificate inside a Secret
     * @param prefix                Prefix used to generate the volume name

     * @return  The generated volume name
     */
    private static String trustedCertificateVolumeName(CertSecretSource certSecretSource, String prefix)    {
        return prefix != null ? prefix + '-' + certSecretSource.getSecretName() : certSecretSource.getSecretName();
    }

    /**
     * Process the list of trusted certificates into an environment variable that will be used by the container images.
     * The environment variable has the format `secretName/filenameOrPattern;secretName/filenameOrPattern;...`.
     *
     * @param trustedCertificates   List of trusted certificates
     *
     * @return  String to be used in the environment variable
     */
    public static String trustedCertsEnvVar(List<CertSecretSource> trustedCertificates)   {
        if (trustedCertificates != null && !trustedCertificates.isEmpty()) {
            List<String> paths = new ArrayList<>();

            for (CertSecretSource certSecretSource : trustedCertificates) {
                if (certSecretSource.getCertificate() != null)  {
                    paths.add(certSecretSource.getSecretName() + "/" + certSecretSource.getCertificate());
                } else if (certSecretSource.getPattern() != null)   {
                    paths.add(certSecretSource.getSecretName() + "/" + certSecretSource.getPattern());
                } else {
                    throw new InvalidResourceException("Certificate source does not contain the certificate or the pattern.");
                }
            }

            return String.join(";", paths);
        } else {
            return null;
        }
    }
}
