/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.math.BigInteger;
import java.security.cert.CertificateEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Certificate utility methods
 */
public class CertUtils {
    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CertUtils.class.getName());

    private CertUtils() { }

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
        Volume secretVolume = VolumeUtils.createSecretVolume(
            trustedCertificateVolumeName(certSecretSource, prefix),
            certSecretSource.getSecretName(),
            isOpenShift);

        // skipping if a volume with same name was already added
        if (volumeList.stream().noneMatch(v -> v.getName().equals(secretVolume.getName()))) {
            volumeList.add(secretVolume);
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
        VolumeMount secretVolumeMount = VolumeUtils.createVolumeMount(
            trustedCertificateVolumeName(certSecretSource, prefix),
            tlsVolumeMountPath + certSecretSource.getSecretName());

        // skipping if a volume mount with same Secret name was already added
        if (volumeMountList.stream().noneMatch(vm -> vm.getName().equals(secretVolumeMount.getName()))) {
            volumeMountList.add(secretVolumeMount);
        }
    }

    /**
     * Adds a prefix to the Secret name used in Volume and VolumeMounts definitions.
     * The returned string may exceed the maximum resource name length.
     *
     * @param certSecretSource      Represents a certificate inside a Secret
     * @param prefix                Prefix used to generate the volume name
     *
     * @return  The prefixed secret name, or the secret name if the prefix is null.
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
