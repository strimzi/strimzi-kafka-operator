/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.ca;

import io.fabric8.certmanager.api.model.v1.Certificate;
import io.fabric8.certmanager.api.model.v1.CertificateBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.certmanager.IssuerRef;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.StrimziSubject;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.kubernetes.CertManagerCertificateOperator;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;

import java.math.BigInteger;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import static io.strimzi.operator.common.ca.CertificateUtils.getCertificateThumbprint;

/**
 * A Certificate Authority managed by cert-manager
 */
@SuppressWarnings("checkstyle:CyclomaticComplexity")
public class CertManagerCa extends Ca {
    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CertManagerCa.class);

    private static final String CERT_MANAGER_SECRET_SUFFIX = "-cm";
    private final CertManagerCertificateOperator certManagerCertificateOperator;
    private final SecretOperator secretOperator;
    private final OwnerReference ownerReference;
    protected final IssuerRef issuerRef;

    /**
     * Constructs the CA object
     *
     * @param reconciliation                 Reconciliation marker
     * @param caRole                         Ca role
     * @param caCertSecret                   Kubernetes Secret where the CA public key is stored
     * @param caConfig                       Certificate Authority configuration
     * @param certManagerCertificateOperator cert-manager Certificate operator
     * @param secretOperator                 Secret operator
     * @param ownerReference                 Owner reference for Kubernetes resources
     * @param issuerRef                      Reference to issuer for issuing certificates through other services like cert-manager
     */
    public CertManagerCa(Reconciliation reconciliation,
                         CaRole caRole,
                         Secret caCertSecret,
                         CaConfig caConfig,
                         CertManagerCertificateOperator certManagerCertificateOperator,
                         SecretOperator secretOperator,
                         OwnerReference ownerReference,
                         IssuerRef issuerRef) {
        super(reconciliation, caRole, caCertSecret, null, caConfig);
        this.certManagerCertificateOperator = certManagerCertificateOperator;
        this.secretOperator = secretOperator;
        this.ownerReference = ownerReference;
        this.issuerRef = issuerRef;
    }

    @Override
    protected int initCaKeyGeneration(Secret caKeySecret, Secret caCertSecret) {
        if (caCertSecret != null) {
            return Annotations.intAnnotation(caCertSecret, ANNO_STRIMZI_IO_CA_KEY_GENERATION, INIT_GENERATION);
        }
        return INIT_GENERATION;
    }

    @Override
    public CompletionStage<CertAndKey> maybeCopyOrGenerateServerCerts(Reconciliation reconciliation, String componentName, StrimziSubject subject, CertAndKey existingCertAndKey, boolean isMaintenanceTimeWindowsSatisfied, boolean includeCaChain) {
        return maybeCopyOrGenerateCert(componentName, subject, existingCertAndKey);
    }

    @Override
    public CompletionStage<CertAndKey> maybeCopyOrGenerateClientCert(Reconciliation reconciliation, String componentName, CertAndKey existingCertAndKey, boolean isMaintenanceTimeWindowsSatisfied) {
        Subject subject = CertificateUtils.getSubject(componentName, Ca.IO_STRIMZI);
        return maybeCopyOrGenerateCert(componentName, subject, existingCertAndKey);
    }

    /**
     * Maybe update CA data and cert and key generations
     * <p>
     * Store the new data if it doesn't exist already, otherwise check if the certificate has changed
     * and update the data and generations accordingly.
     *
     * @param newCaCertAsBase64     New CA cert.
     * @param existingCaCertHash    Existing CA cert hash to determine if the cert has changed.
     * @param endEntityCertificate  End entity certificate to use for cert path validation.
     */
    public void maybeUpdateCa(String newCaCertAsBase64, String existingCaCertHash, X509Certificate endEntityCertificate) {
        renewalType = shouldUpdateCa(newCaCertAsBase64, existingCaCertHash, endEntityCertificate);
        Map<String, String> updatedCertData;
        switch (renewalType) {
            case NOOP -> updatedCertData = new HashMap<>(caCertData);
            case CREATE -> {
                // No data, so we add it
                updatedCertData = new HashMap<>();
                updatedCertData.put(CA_CRT, newCaCertAsBase64);
            }
            case RENEW_CERT -> {
                updatedCertData = new HashMap<>();
                updatedCertData.put(CA_CRT, newCaCertAsBase64);
                ++caCertGeneration;
            }
            case REPLACE_KEY -> {
                String notAfterDate = DATE_TIME_FORMATTER.format(currentCaCertX509().getNotAfter().toInstant().atZone(ZoneId.of("Z")));
                updatedCertData = new HashMap<>();
                updatedCertData.put(Ca.SecretEntry.CRT.asKey("ca-" + notAfterDate), caCertData.get(CA_CRT));
                updatedCertData.put(CA_CRT, newCaCertAsBase64);
                ++caCertGeneration;
                ++caKeyGeneration;
            }
            default -> throw new RuntimeException("Unsupported renewal type: " + renewalType);
        }
        caCertData = updatedCertData;
    }

    private RenewalType shouldUpdateCa(String newCaCertAsBase64, String existingCaCertHash, X509Certificate endEntityCertificate) {
        if (caCertData.isEmpty()) {
            return RenewalType.CREATE;
        }

        X509Certificate x509CaCert;
        String newCaCertHash;
        try {
            x509CaCert = CertificateUtils.x509Certificate(Util.decodeBytesFromBase64(newCaCertAsBase64));
            newCaCertHash = String.format("%040x", new BigInteger(1, Util.sha1Digest(x509CaCert.getEncoded())));
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }

        if (!existingCaCertHash.equals(newCaCertHash)) {
            if (CaRole.CLIENTS_CA.equals(caRole)) {
                // For clients CA we treat both renewal and key replacement the same so no trust check is needed
                return RenewalType.RENEW_CERT;
            }
            if (endEntityCertificate == null) {
                // Cluster operator certificate is missing, so no cert path validation to perform
                // Don't update - wait for operator cert to be available
                LOGGER.warnCr(reconciliation, "Cluster CA cert has changed, but operator certificate is missing - cannot determine if key changed. Will retry in next reconciliation.");
                return  RenewalType.NOOP;
            }
            if (CertificateUtils.certIsTrusted(reconciliation, List.of(endEntityCertificate), x509CaCert)) {
                // No key replacement
                return RenewalType.RENEW_CERT;
            } else {
                // key replacement
                return RenewalType.REPLACE_KEY;
            }
        } else {
            return RenewalType.NOOP;
        }
    }

    @Override
    protected boolean removeCerts(Map<String, String> newData, Predicate<Map.Entry<String, String>> predicate) {
        Iterator<Map.Entry<String, String>> iter = newData.entrySet().iterator();
        List<String> removed = new ArrayList<>();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            boolean remove = predicate.test(entry);
            if (remove) {
                String certName = entry.getKey();
                LOGGER.debugCr(reconciliation, "Removing data.{} from Secret",
                        certName.replace(".", "\\."));
                iter.remove();
                removed.add(certName);
            }
        }
        return !removed.isEmpty();
    }

    @Override
    public void maybeDeleteOldCerts() {
        deleteOldCerts();
    }

    CompletionStage<CertAndKey> maybeCopyOrGenerateCert(String entityName, StrimziSubject subject, CertAndKey existingCert) {
        return generateSignedCert(entityName, subject)
                .thenApply(newCertAndKey -> {
                    if (existingCert == null) {
                        return newCertAndKey;
                    } else if (certManagerCertUpdated(existingCert, newCertAndKey)) {
                        if (CertificateUtils.certIsTrusted(reconciliation, CertificateUtils.extractCertChain(entityName, newCertAndKey.cert()), currentCaCertX509())) {
                            LOGGER.infoCr(reconciliation, "New certificate for {}/{}", reconciliation.namespace(), entityName);
                            return newCertAndKey;
                        } else {
                            LOGGER.infoCr(reconciliation, "New certificate for {}/{}, but not trusted yet so keeping existing certificate.", reconciliation.namespace(), entityName);
                            return existingCert;
                        }
                    } else {
                        // Certificate has not changed
                        return existingCert;
                    }
                });
    }

    /**
     * Generates a certificate signed by cert-manager CA
     *
     * @param entityName            Name of the component the Certificate is for
     * @param subject               Subject for Certificate
     * @return CompletionStage with CertAndKey
     */
    private CompletionStage<CertAndKey> generateSignedCert(String entityName, StrimziSubject subject) {
        Certificate certificate = buildCertificateResource(entityName, subject, caConfig.getValidityDays(), caConfig.getRenewalDays());
        return certManagerCertificateOperator.reconcile(reconciliation, reconciliation.namespace(), entityName, certificate)
                .thenCompose(v -> certManagerCertificateOperator.waitForReady(reconciliation, reconciliation.namespace(), entityName))
                .thenCompose(v -> secretOperator.getAsync(reconciliation.namespace(), certManagerSecretName(entityName)))
                .thenCompose(certSecret -> {
                    if (certSecret == null) {
                        return CompletableFuture.failedFuture(new RuntimeException("cert-manager Certificate '" + entityName + "' is Ready but its Secret '" + certManagerSecretName(entityName) +
                                "' is not yet available. Failing reconciliation to wait for the secret to become available."));
                    }
                    if (certSecret.getData() == null || certSecret.getData().get("tls.crt") == null || certSecret.getData().get("tls.key") == null) {
                        return CompletableFuture.failedFuture(new RuntimeException(new RuntimeException("cert-manager Certificate '" + entityName + "' is Ready but no certificate data is provided")));
                    }
                    CertAndKey updatedCert = new CertAndKey(Util.decodeBytesFromBase64(certSecret.getData().get("tls.key")),
                            Util.decodeBytesFromBase64(certSecret.getData().get("tls.crt")), this.caCertGeneration);

                    // Check the subject of the certificate is correct, otherwise fail the reconciliation to wait for the certificate to be issued with correct dns
                    Collection<String> desiredAltNames = subject.subjectAltNames().values();
                    Collection<String> updatedCertAltNames = CertificateUtils.getSubjectAltNames(reconciliation, updatedCert.cert());
                    if (updatedCertAltNames != null && desiredAltNames.containsAll(updatedCertAltNames) && updatedCertAltNames.containsAll(desiredAltNames))   {
                        return CompletableFuture.completedFuture(updatedCert);
                    } else {
                        String message = "Certificate from cert-manager does not contain correct subject. Failing reconciliation to wait for new certificate to be issued.";
                        LOGGER.debugCr(reconciliation, message);
                        return CompletableFuture.failedFuture(new RuntimeException(message));
                    }
                });
    }

    /**
     * Build Certificate object to give to cert-manager to generate certificate
     *
     * @param entityName            Name of the component the Certificate is for
     * @param subject               Subject for Certificate
     * @param validityDays          Validity days for Certificate
     * @param renewalDays           Renewal days for certificate
     * @return Certificate object
     */
    private Certificate buildCertificateResource(String entityName, StrimziSubject subject, int validityDays, int renewalDays) {
        String secretName = certManagerSecretName(entityName);
        CertificateBuilder certificateBuilder = new CertificateBuilder()
                .withNewMetadata()
                    .withName(entityName)
                    .withNamespace(reconciliation.namespace())
                .endMetadata()
                .withNewSpec()
                .withCommonName(subject.commonName())
                .withNewPrivateKey()
                    .withAlgorithm("RSA")
                    .withEncoding("PKCS8")
                    .withSize(2048)
                .endPrivateKey()
                .withDuration(convertToFabric8Duration(validityDays))
                .withRenewBefore(convertToFabric8Duration(renewalDays))
                .withIsCA(false)
                .withNewSubject()
                    .withOrganizations(subject.organizationName())
                .endSubject()
                .withDnsNames(new ArrayList<>(subject.dnsNames()))
                .withIpAddresses(new ArrayList<>(subject.ipAddresses()))
                .withNewIssuerRef()
                    .withName(issuerRef.getName())
                    .withKind(issuerRef.getKind().toValue())
                    .withGroup(issuerRef.getGroup())
                .endIssuerRef()
                .withSecretName(secretName)
                .endSpec();
        if (ownerReference != null) {
            certificateBuilder.editMetadata().withOwnerReferences(ownerReference).endMetadata();
        }
        return certificateBuilder.build();
    }

    /**
     * Checks if two certs are the same by comparing hashes
     * @param existingCertAndKey    Existing cert
     * @param newCertAndKey         New cert
     * @return Whether the cert has been updated in the new Secret
     */
    private static boolean certManagerCertUpdated(CertAndKey existingCertAndKey, CertAndKey newCertAndKey) {
        try {
            String existingCertHash = getCertificateThumbprint(CertificateUtils.x509Certificate(existingCertAndKey.cert()));
            String newCertHash = getCertificateThumbprint(CertificateUtils.x509Certificate(newCertAndKey.cert()));
            return !existingCertHash.equals(newCertHash);
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Convert an int to a fabric8 Duration.
     * <p>
     * Since the constructor only takes a java.time.Duration the checkstyle
     * warning for qualified class names needs to be suppressed.
     *
     * @param days int of days
     * @return fabric8 duration representing the days
     */
    @SuppressWarnings("NoFullyQualifiedClassNames")
    /*test*/ static io.fabric8.kubernetes.api.model.Duration convertToFabric8Duration(int days) {
        return new io.fabric8.kubernetes.api.model.Duration(Duration.ofDays(days));
    }

    /**
     * Get the name of the Secret managed by cert-manager, given a Strimzi managed Secret
     *
     * @param strimziSecretName Name of the Secret managed by Strimzi
     * @return Secret name to use for cert-manager managed Secret
     */
    private static String certManagerSecretName(String strimziSecretName) {
        return strimziSecretName + CERT_MANAGER_SECRET_SUFFIX;
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
     * Get the name of the Secret managed by Strimzi, given a cert-manager managed Secret
     *
     * @param certManagerSecretName Name of the Secret managed by cert-manager
     * @return Secret name to use for Strimzi managed Secret
     */
    public static String mapToStrimziSecretName(String certManagerSecretName) {
        if (!matchesCertManagerSecretNaming(certManagerSecretName)) {
            throw new RuntimeException("Supplied Secret does not match expected format for cert-manager Secret name");
        }
        return certManagerSecretName.substring(0, certManagerSecretName.length() - CERT_MANAGER_SECRET_SUFFIX.length());
    }
}
