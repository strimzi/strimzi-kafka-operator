/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.certmanager.api.model.v1.Certificate;
import io.fabric8.certmanager.api.model.v1.CertificateBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.Labels;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * cert-manager utility methods
 */
public class CertManagerUtils {
    private static final String CERT_MANAGER_SECRET_SUFFIX = "-cm";
    /**
     * Get the name of the Secret managed by cert-manager, given a Strimzi managed Secret
     *
     * @param strimziSecretName Name of the Secret managed by Strimzi
     * @return Secret name to use for cert-manager managed Secret
     */
    public static String certManagerSecretName(String strimziSecretName) {
        return strimziSecretName + CERT_MANAGER_SECRET_SUFFIX;
    }

    /**
     * Get the name of the Secret managed by Strimzi, given a cert-manager managed Secret
     *
     * @param certManagerSecretName Name of the Secret managed by cert-manager
     * @return Secret name to use for Strimzi managed Secret
     */
    public static String strimziSecretName(String certManagerSecretName) {
        if (!matchesCertManagerSecretNaming(certManagerSecretName)) {
            throw new RuntimeException("Supplied Secret does not match expected format for cert-manager Secret name");
        }
        return certManagerSecretName.substring(0, certManagerSecretName.length() - CERT_MANAGER_SECRET_SUFFIX.length());
    }

    /**
     * Returns whether the supplied Secret name has the same format as a Secret created by cert-manager
     *
     * @param secretName Secret name to check
     * @return Whether the Secret name matches the format of a Secret created by cert-manager
     */
    public static boolean matchesCertManagerSecretNaming(String secretName) {
        return secretName.endsWith(CERT_MANAGER_SECRET_SUFFIX);
    }

    /**
     * Build Certificate object to give to cert-manager to generate certificate
     *
     * @param namespace             Namespace for the Certificate
     * @param name                  Name for the Certificate
     * @param initialCertificate    Certificate to use as a base
     * @param labels                Labels for Certificate
     * @param ownerReference        Owner reference for Certificate
     * @return Certificate object
     */
    public static Certificate buildCertManagerCertificate(String namespace,
                                                          String name, Certificate initialCertificate,
                                                          Labels labels, OwnerReference ownerReference) {
        String secretName = certManagerSecretName(name);
        if (ownerReference == null) {
            return new CertificateBuilder(initialCertificate)
                    .withNewMetadata()
                        .withName(name)
                        .withNamespace(namespace)
                    .endMetadata()
                    .editSpec()
                        .withSecretName(secretName)
                        .withNewSecretTemplate()
                            .withLabels(labels.toMap())
                        .endSecretTemplate()
                    .endSpec()
                    .build();
        } else {
            return new CertificateBuilder(initialCertificate)
                    .withNewMetadata()
                        .withName(name)
                        .withNamespace(namespace)
                            .withOwnerReferences(ownerReference)
                    .endMetadata()
                    .editSpec()
                        .withSecretName(secretName)
                        .withNewSecretTemplate()
                            .withLabels(labels.toMap())
                        .endSecretTemplate()
                    .endSpec()
                    .build();
        }
    }

    /**
     * Builds a clusterCa certificate Secret for the different Strimzi components (TO, UO, KE, ...).
     * Used when cert-manager has issued the CA Secret.
     *
     * @param clusterCa         The Cluster CA
     * @param clientsCa         The Clients CA. If this is not null the Clients CA generation is added
     * @param certManagerSecret cert-manager Secret containing the Cluster CA
     * @param namespace         Namespace for the Secret
     * @param secretName        Name of the Secret
     * @param keyCertName       Key under which the certificate will be stored in the new Secret
     * @param labels            Labels
     * @param ownerReference    Owner reference
     * @return Newly built Secret
     */
    public static Secret buildTrustedCertificateSecretFromCertManager(ClusterCa clusterCa, ClientsCa clientsCa, Secret certManagerSecret, String namespace,
                                                                      String secretName, String keyCertName, Labels labels,
                                                                      OwnerReference ownerReference) {
        String certHash = CertUtils.getCertificateThumbprint(certManagerSecret, "tls.crt");
        Objects.requireNonNull(certHash);

        Map<String, String> annotations = new HashMap<>();
        annotations.put(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, certHash);
        Map.Entry<String, String> clusterCaGeneration = clusterCa.caCertGenerationFullAnnotation();
        annotations.put(clusterCaGeneration.getKey(), clusterCaGeneration.getValue());

        if (clientsCa != null) {
            Map.Entry<String, String> clientsCaGeneration = clientsCa.caCertGenerationFullAnnotation();
            annotations.put(clientsCaGeneration.getKey(), clientsCaGeneration.getValue());
        }

        return ModelUtils.createSecret(secretName, namespace, labels, ownerReference,
                Map.of(
                        Ca.SecretEntry.CRT.asKey(keyCertName), certManagerSecret.getData().get("tls.crt"),
                        Ca.SecretEntry.KEY.asKey(keyCertName), certManagerSecret.getData().get("tls.key")
                ),
                annotations,
                Map.of());
    }

    /**
     * Checks if the hashes of certs in a cert Secret have changed
     * @param existingCertSecret    Existing cert Secret
     * @param newCertSecret         New cert Secret
     * @return Whether the cert has been updated in the new Secret
     */
    public static boolean certManagerCertUpdated(Secret existingCertSecret, Secret newCertSecret) {
        String existingCertHash = Annotations.stringAnnotation(existingCertSecret, Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, null);
        String newCertHash = Annotations.stringAnnotation(newCertSecret, Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, null);
        if (existingCertHash == null || newCertHash == null) {
            throw new RuntimeException(String.format("Failed to find server-cert-hash annotation for Secret %s/%s", existingCertSecret.getMetadata().getNamespace(), existingCertSecret.getMetadata().getName()));
        }
        return !existingCertHash.equals(newCertHash);
    }
}
