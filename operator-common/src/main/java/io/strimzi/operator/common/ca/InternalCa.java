/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.ca;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertIssuer;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.PasswordGenerator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * A Certificate Authority which can renew its own (self-signed) certificates, and generate signed certificates
 */
@SuppressWarnings("checkstyle:CyclomaticComplexity")
public class InternalCa extends Ca {

    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(InternalCa.class);

    /**
     * Key for storing the CA PKCS21 store in a Kubernetes Secret
     */
    public static final String CA_STORE = SecretEntry.P12_KEYSTORE.asKey(CA_SECRET_PREFIX);

    /**
     * Key for storing the PKCS12 store password in a Kubernetes Secret
     */
    public static final String CA_STORE_PASSWORD = SecretEntry.P12_KEYSTORE_PASSWORD.asKey(CA_SECRET_PREFIX);

    private final PasswordGenerator passwordGenerator;
    private final CertIssuer certIssuer;

    /**
     * Constructs the CA object
     *
     * @param reconciliation    Reconciliation marker
     * @param caRole            Ca Role
     * @param certIssuer        Certificate manager instance
     * @param passwordGenerator Password generator instance
     * @param caCertSecret      Kubernetes Secret where the CA public key is stored
     * @param caKeySecret       Kubernetes Secret where the CA private key is stored
     * @param caConfig          Certificate Authority configuration
     */
    public InternalCa(Reconciliation reconciliation,
                      CaRole caRole, CertIssuer certIssuer,
                      PasswordGenerator passwordGenerator,
                      Secret caCertSecret,
                      Secret caKeySecret,
                      CaConfig caConfig) {
        super(reconciliation, caRole, caCertSecret, caKeySecret, caConfig);
        this.certIssuer = certIssuer;
        this.passwordGenerator = passwordGenerator;
    }

    @Override
    protected int initCaKeyGeneration(Secret caKeySecret, Secret caCertSecret) {
        if (caKeySecret != null) {
            return Annotations.intAnnotation(caKeySecret, ANNO_STRIMZI_IO_CA_KEY_GENERATION, INIT_GENERATION);
        }
        return INIT_GENERATION;
    }

    @Override
    public CompletionStage<CertAndKey> maybeCopyOrGenerateServerCerts(Reconciliation reconciliation,
                                                                  String podName,
                                                                  Subject subject,
                                                                  CertAndKey existingCertAndKey,
                                                                  boolean isMaintenanceTimeWindowsSatisfied,
                                                                  boolean includeCaChain) {
        List<String> reasons = new ArrayList<>();

        if (existingCertAndKey == null) {
            reasons.add("certificate doesn't exist yet for pod");
        } else if (hasCaCertGenerationChanged(existingCertAndKey.caCertGeneration(), podName)) {
            reasons.add("certificate for pod has old cert generation");
        } else {
            // A certificate for this node already exists, so we will try to reuse it
            LOGGER.debugCr(reconciliation, "certificate for node {} already exists", podName);

            if (certSubjectChanged(reconciliation, existingCertAndKey, subject, podName))   {
                reasons.add("DNS names changed");
            }

            if (isExpiring(existingCertAndKey.cert()) && isMaintenanceTimeWindowsSatisfied)  {
                reasons.add("certificate is expiring");
            }


            // In Strimzi 0.48 we moved to using the PEM certificates directly instead of PKCS12 in the Kafka brokers.
            // But that (unintentionally) removed the full CA chain from the server certificates. We added them back
            // in Strimzi 0.50. But this logic is needed to actually roll out the updated Secrets with the full CA chain.
            // For more details, see https://github.com/strimzi/strimzi-kafka-operator/issues/12364.
            //
            // After some time - after multiple Strimzi releases, once the CA chains are added in all clusters, we
            // should be able to remove this logic again.
            if (includeCaChain && !includesCaChain(existingCertAndKey.cert(), currentCaCertBytes())) {
                reasons.add("CA chain added");
            }
        }

        CertAndKey certAndKey;
        if (!reasons.isEmpty())  {
            LOGGER.infoCr(reconciliation, "Certificate for pod {} needs to be regenerated because: {}", podName, String.join(", ", reasons));
            try {
                certAndKey = generateSignedCert(subject, includeCaChain);
            } catch (IOException e) {
                LOGGER.errorCr(reconciliation, "Error while generating certificates", e);
                return CompletableFuture.failedStage(new RuntimeException("Failed to prepare certificate for" + podName, e));
            }
        }  else {
            certAndKey = existingCertAndKey;
        }

        return CompletableFuture.completedFuture(certAndKey);
    }

    /**
     * Checks whether subject alternate names changed and certificate needs a renewal
     *
     * @param certAndKey        Current certificate
     * @param desiredSubject    Desired subject alternate names
     * @param podName           Name of the pod to which this certificate belongs (used for log messages)
     *
     * @return  True if the subjects are different, false otherwise
     */
    /* test */
    static boolean certSubjectChanged(Reconciliation reconciliation, CertAndKey certAndKey, Subject desiredSubject, String podName)    {
        Collection<String> desiredAltNames = desiredSubject.subjectAltNames().values();
        Collection<String> currentAltNames = getSubjectAltNames(reconciliation, certAndKey.cert());

        if (currentAltNames != null && desiredAltNames.containsAll(currentAltNames) && currentAltNames.containsAll(desiredAltNames))   {
            LOGGER.traceCr(reconciliation, "Alternate subjects match. No need to refresh cert for pod {}.", podName);
            return false;
        } else {
            LOGGER.infoCr(reconciliation, "Alternate subjects for pod {} differ", podName);
            LOGGER.infoCr(reconciliation, "Current alternate subjects: {}", currentAltNames);
            LOGGER.infoCr(reconciliation, "Desired alternate subjects: {}", desiredAltNames);
            return true;
        }
    }

    /**
     * Extracts the alternate subject names out of existing certificate
     *
     * @param certificate   Existing X509 certificate as a byte array
     *
     * @return  List of certificate Subject Alternate Names
     */
    private static List<String> getSubjectAltNames(Reconciliation reconciliation, byte[] certificate) {
        List<String> subjectAltNames = new ArrayList<>();

        try {
            X509Certificate cert = CertificateUtils.x509Certificate(certificate);
            Collection<List<?>> altNames = cert.getSubjectAlternativeNames();
            if (altNames != null) {
                subjectAltNames = altNames.stream()
                        .filter(name -> name.get(1) instanceof String)
                        .map(item -> (String) item.get(1))
                        .collect(Collectors.toList());
            }
        } catch (CertificateException | RuntimeException e) {
            LOGGER.debugCr(reconciliation, "Failed to parse existing certificate", e);
        }

        return subjectAltNames;
    }

    /**
     * Checks if the CA chain is contained at the end of the certificate.
     *
     * @param cert      The server certificate as a byte array
     * @param caChain   The CA chain as a byte array
     *
     * @return  True if the CA chain is included at the end of the certificate, false otherwise.
     */
    /* test */ public static boolean includesCaChain(byte[] cert, byte[] caChain) {
        if (cert == null || caChain == null || cert.length < caChain.length) {
            // The CA chain is definitely not included
            return false;
        } else {
            return Arrays.equals(Arrays.copyOfRange(cert, cert.length - caChain.length, cert.length), caChain);
        }
    }

    /**
     * It checks if the current CA certificate generation is changed compared to the one
     * that signed the CertAndKey.
     */
    private boolean hasCaCertGenerationChanged(int certAndKeyCaCertGeneration, String podName) {
        LOGGER.debugOp("Pod {} generation anno = {}, current CA generation = {}", podName, certAndKeyCaCertGeneration, caCertGeneration());
        return certAndKeyCaCertGeneration != caCertGeneration;
    }

    @Override
    public CompletionStage<CertAndKey> maybeCopyOrGenerateClientCert(
            Reconciliation reconciliation,
            String commonName,
            CertAndKey existingCertAndKey,
            boolean isMaintenanceTimeWindowsSatisfied
    ) {
        List<String> reasons = new ArrayList<>();

        if (existingCertAndKey == null) {
            reasons.add("certificate doesn't exist yet");
        } else if (hasCaCertGenerationChanged(existingCertAndKey.caCertGeneration(), commonName)) {
            reasons.add("certificate has old cert generation");
        } else {
            // Certificate exists and CA generation matches - check if renewal is needed
            if (isExpiring(existingCertAndKey.cert()) && isMaintenanceTimeWindowsSatisfied) {
                reasons.add("certificate is expiring");
            }
        }

        CertAndKey certAndKey = null;
        if (!reasons.isEmpty()) {
            LOGGER.infoCr(reconciliation, "Certificate for component {} needs to be regenerated because: {}", commonName, String.join(", ", reasons));

            try {
                String org = caRole.equals(CaRole.CLIENTS_CA) ? null : Ca.IO_STRIMZI;
                Subject subject = CertificateUtils.getSubject(commonName, org);
                certAndKey = generateSignedCert(subject);
            } catch (IOException e) {
                LOGGER.warnCr(reconciliation, "Error while generating certificates", e);
            }

            LOGGER.debugCr(reconciliation, "End generating certificates");
        } else {
            certAndKey = existingCertAndKey;
        }

        return CompletableFuture.completedFuture(certAndKey);
    }

    private static void delete(Reconciliation reconciliation, File file) {
        if (file != null && !file.delete()) {
            LOGGER.warnCr(reconciliation, "{} cannot be deleted", file.getName());
        }
    }

    /**
     * Generates a PKCS12 keystore for an existing key and certificate.
     *
     * @param alias                     Alias under which it should be stored in the PKCS12 store
     * @param key                       Private key
     * @param cert                      Public key
     * @param caCertGeneration          Generation number of the CA that signed this certificate
     *
     * @return  CertAndKey with the certificate and PKCS12 store
     *
     * @throws IOException  Throws an IOException if something fails when working with the files
     */
    public CertAndKey generatePkcs12Store(String alias, byte[] key, byte[] cert, int caCertGeneration) throws IOException {
        try {
            File keyFile = Files.createTempFile("tls", "key").toFile();
            File certFile = Files.createTempFile("tls", "cert").toFile();
            File keyStoreFile = Files.createTempFile("tls", "p12").toFile();

            try {
                Files.write(keyFile.toPath(), key);
                Files.write(certFile.toPath(), cert);

                String keyStorePassword = passwordGenerator.generate();
                certIssuer.addKeyAndCertToKeyStore(keyFile, certFile, alias, keyStoreFile, keyStorePassword);

                return new CertAndKey(
                        Files.readAllBytes(keyFile.toPath()),
                        Files.readAllBytes(certFile.toPath()),
                        null,
                        Files.readAllBytes(keyStoreFile.toPath()),
                        keyStorePassword,
                        caCertGeneration);
            } finally {
                delete(reconciliation, keyFile);
                delete(reconciliation, certFile);
                delete(reconciliation, keyStoreFile);
            }
        } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Generates a certificate signed by this CA
     *
     * @param subject The subject of the certificate to be generated.
     * @param csrFile Certificate sign request file
     * @param keyFile Key file
     * @param certFile Certificate file
     * @param keyStoreFile Keystore file
     * @param includeCaChain Whether include CA chain
     * @return The CertAndKey
     */
    public CertAndKey generateSignedCert(Subject subject,
                                         File csrFile, File keyFile, File certFile, File keyStoreFile, boolean includeCaChain) {
        LOGGER.infoCr(reconciliation, "Generating certificate {}, signed by CA {}", subject, caRole.caCommonName());

        try {
            byte[] caCertBytes = currentCaCertBytes();
            certIssuer.generateCsr(keyFile, csrFile, subject);
            certIssuer.generateCert(csrFile, currentCaKey(), caCertBytes,
                    certFile, subject, caConfig.getValidityDays());

            byte[] certChain;
            if (includeCaChain) {
                byte[] leafCert = Files.readAllBytes(certFile.toPath());
                certChain = new byte[leafCert.length + caCertBytes.length];
                System.arraycopy(leafCert, 0, certChain, 0, leafCert.length);
                System.arraycopy(caCertBytes, 0, certChain, leafCert.length, caCertBytes.length);
            } else {
                certChain = Files.readAllBytes(certFile.toPath());
            }

            if (caConfig.isGeneratePkcs12Stores()) {
                String keyStorePassword = passwordGenerator.generate();
                certIssuer.addKeyAndCertToKeyStore(keyFile, certFile, subject.commonName(), keyStoreFile, keyStorePassword);

                return new CertAndKey(
                        Files.readAllBytes(keyFile.toPath()),
                        certChain,
                        null,
                        Files.readAllBytes(keyStoreFile.toPath()),
                        keyStorePassword,
                        caCertGeneration);
            } else {
                return new CertAndKey(
                        Files.readAllBytes(keyFile.toPath()),
                        certChain,
                        null,
                        null,
                        null,
                        caCertGeneration);
            }
        } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
    }


    private CertAndKey generateSignedCert(Subject subject) throws IOException {
        return generateSignedCert(subject, false);
    }


    private CertAndKey generateSignedCert(Subject subject, boolean includeCaChain) throws IOException {
        File csrFile = Files.createTempFile("tls", "csr").toFile();
        File keyFile = Files.createTempFile("tls", "key").toFile();
        File certFile = Files.createTempFile("tls", "cert").toFile();
        File keyStoreFile = Files.createTempFile("tls", "p12").toFile();

        CertAndKey result = generateSignedCert(subject, csrFile, keyFile, certFile, keyStoreFile, includeCaChain);

        delete(reconciliation, csrFile);
        delete(reconciliation, keyFile);
        delete(reconciliation, certFile);
        delete(reconciliation, keyStoreFile);
        return result;
    }

    /**
     * Returns whether the certificate is expiring or not
     *
     * @param secret  Secret with the certificate
     * @param certKey   Key under which is the certificate stored
     * @return  True when the certificate should be renewed. False otherwise.
     */
    public boolean isExpiring(Secret secret, String certKey)  {
        X509Certificate currentCert = CertificateUtils.cert(secret, certKey);
        return certNeedsRenewal(currentCert);
    }

    /**
     * Returns whether the certificate is expiring or not
     *
     * @param certificate Byte array with the certificate
     *
     * @return  True when the certificate should be renewed. False otherwise.
     */
    public boolean isExpiring(byte[] certificate)  {
        try {
            X509Certificate currentCert = CertificateUtils.x509Certificate(certificate);
            return certNeedsRenewal(currentCert);
        } catch (CertificateException e) {
            LOGGER.errorCr(reconciliation, "Failed to parse existing certificate", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Create the CA data if they don't exist, otherwise if within the renewal period then either renew
     * the CA cert or replace the CA cert and key, according to the configured policy. After calling this method
     * {@link #certsRemoved()} will return whether expired secrets were removed from the Secret.
     *
     * @param maintenanceWindowSatisfied Flag indicating whether we are in the maintenance window
     * @param forceReplace Flag indicating whether to do a force replace
     * @param forceRenew Flag indicating whether to do a force renew
     */
    public void createRenewOrReplace(boolean maintenanceWindowSatisfied, boolean forceReplace, boolean forceRenew) {
        X509Certificate currentCert = currentCaCertX509();
        Map<String, String> certData;
        Map<String, String> keyData;
        this.renewalType = shouldCreateOrRenew(currentCert, maintenanceWindowSatisfied, forceReplace, forceRenew);
        LOGGER.debugCr(reconciliation, "{} renewalType {}", caRole.caCommonName(), renewalType);

        switch (renewalType) {
            case CREATE -> {
                keyData = new HashMap<>(1);
                certData = new HashMap<>(3);
                generateCaKeyAndCert(nextCaSubject(caKeyGeneration), keyData, certData);
            }
            case REPLACE_KEY -> {
                keyData = new HashMap<>(1);
                certData = new HashMap<>(caCertData);
                if (certData.containsKey(CA_CRT)) {
                    String notAfterDate = DATE_TIME_FORMATTER.format(currentCert.getNotAfter().toInstant().atZone(ZoneId.of("Z")));

                    if (caConfig.isGeneratePkcs12Stores()) {
                        addCertCaToTrustStore("ca-" + notAfterDate + SecretEntry.CRT.suffix, certData);
                    }

                    certData.put("ca-" + notAfterDate + SecretEntry.CRT.suffix, certData.remove(CA_CRT));
                }
                ++caCertGeneration;
                generateCaKeyAndCert(nextCaSubject(++caKeyGeneration), keyData, certData);
            }
            case RENEW_CERT -> {
                keyData = new HashMap<>(caKeyData);
                certData = new HashMap<>(3);
                ++caCertGeneration;
                renewCaCert(nextCaSubject(caKeyGeneration), certData);
            }
            default -> {
                keyData = new HashMap<>(caKeyData);
                certData = new HashMap<>(caCertData);

                if (caConfig.isGeneratePkcs12Stores() && !certData.containsKey(CA_STORE)) {
                    // If we are generating PKCS12 stores, and it is missing in the Secret, we add it
                    addCertCaToTrustStore(CA_CRT, certData);
                } else if (!caConfig.isGeneratePkcs12Stores() && certData.containsKey(CA_STORE)) {
                    // If we are not generating PKCS12 stores, and it is already present in the Secret, we will remove it
                    certData.remove(CA_STORE);
                    certData.remove(CA_STORE_PASSWORD);
                }
            }
        }

        if (removeCerts(certData, this::removeExpiredCert)) {
            LOGGER.infoCr(reconciliation, "{}: Expired CA certificates removed", caRole.caCommonName());
            this.caCertsRemoved = true;
        }

        if (renewalType != RenewalType.NOOP && renewalType != RenewalType.POSTPONED) {
            LOGGER.debugCr(reconciliation, "{}: {}", caRole.caCommonName(), renewalType.postDescription(caRole.caName()));
        }
        caCertData = certData;
        caKeyData = keyData;
    }

    private Subject nextCaSubject(int version) {
        return new Subject.Builder()
        // Key replacements does not work if both old and new CA certs have the same subject DN, so include the
        // key generation in the DN so the certificates appear distinct during CA key replacement.
            .withCommonName(caRole.caCommonName() + " v" + version)
            .withOrganizationName(IO_STRIMZI).build();
    }

    private RenewalType shouldCreateOrRenew(X509Certificate currentCert, boolean maintenanceWindowSatisfied, boolean forceReplace, boolean forceRenew) {
        String reason = null;
        RenewalType renewalType = RenewalType.NOOP;
        if (caKeyData.get(CA_KEY) == null) {
            reason = "CA key secret for " + caRole.caName()  + " is missing or lacking data." + CA_KEY.replace(".", "\\.");
            renewalType = RenewalType.CREATE;
        } else if (this.caCertData.get(CA_CRT) == null) {
            reason = "CA certificate secret for " + caRole.caName()  + " is missing or lacking data." + CA_CRT.replace(".", "\\.");
            renewalType = RenewalType.RENEW_CERT;
        } else if (forceRenew) {
            reason = "CA certificate secret for " + caRole.caName()  + " is annotated with " + Annotations.ANNO_STRIMZI_IO_FORCE_RENEW;

            if (maintenanceWindowSatisfied) {
                renewalType = RenewalType.RENEW_CERT;
            } else {
                renewalType = RenewalType.POSTPONED;
            }
        } else if (forceReplace) {
            reason = "CA key secret for " + caRole.caName()  + " is annotated with " + Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE;

            if (maintenanceWindowSatisfied) {
                renewalType = RenewalType.REPLACE_KEY;
            } else {
                renewalType = RenewalType.POSTPONED;
            }
        } else if (currentCert != null
                && certNeedsRenewal(currentCert)) {
            reason = "Within renewal period for CA certificate (expires on " + currentCert.getNotAfter() + ")";

            if (maintenanceWindowSatisfied) {
                renewalType = switch (caConfig.getCertificateExpirationPolicy()) {
                    case REPLACE_KEY -> RenewalType.REPLACE_KEY;
                    case RENEW_CERTIFICATE -> RenewalType.RENEW_CERT;
                };
            } else {
                renewalType = RenewalType.POSTPONED;
            }
        }

        switch (renewalType) {
            case NOOP ->
                    LOGGER.debugCr(reconciliation, "{}: {}", caRole.caCommonName(), renewalType.preDescription(caRole.caName()));
            case REPLACE_KEY, RENEW_CERT, CREATE ->
                    LOGGER.debugCr(reconciliation, "{}: {}: {}", caRole.caCommonName(), renewalType.preDescription(caRole.caName()), reason);
            case POSTPONED ->
                    LOGGER.warnCr(reconciliation, "{}: {}: {}", caRole.caCommonName(), renewalType.preDescription(caRole.caName()), reason);
        }

        return renewalType;
    }

    /**
     * Remove old certificates that are stored in the CA Secret matching the "ca-YYYY-MM-DDTHH-MM-SSZ.crt" naming pattern.
     * NOTE: mostly used when a CA certificate is renewed by replacing the key
     */
    @Override
    public void maybeDeleteOldCerts() {
        // the operator doesn't have to touch Secret provided by the user with his own custom CA certificate
        if (caConfig.isGenerateCa()) {
            deleteOldCerts();
        }
    }

    /**
     * Checks if the renewal or replacement was postponed.
     *
     * @return Returns true if the renewal or replacement was postponed
     */
    public boolean postponed() {
        return renewalType.equals(RenewalType.POSTPONED);
    }

    /**
     * Predicate used to remove expired certificates that are stored in the CA Secret
     *
     * @param entry entry in the CA Secret data section to check
     * @return if the certificate is expired and has to be removed
     */
    private boolean removeExpiredCert(Map.Entry<String, String> entry) {
        boolean remove = false;
        String certName = entry.getKey();
        String certText = entry.getValue();
        try {
            X509Certificate cert = CertificateUtils.x509Certificate(Util.decodeBytesFromBase64(certText));
            Instant expiryDate = cert.getNotAfter().toInstant();
            remove = expiryDate.isBefore(clock.instant());
            if (remove) {
                LOGGER.infoCr(reconciliation, "The certificate (data.{}) in Secret expired {}; removing it",
                        certName.replace(".", "\\."), expiryDate);
            }
        } catch (CertificateException e) {
            // doesn't remove stores and related password
            if (!SecretEntry.P12_KEYSTORE.matchesType(certName) && !SecretEntry.P12_KEYSTORE_PASSWORD.matchesType(certName)) {
                remove = true;
                LOGGER.debugCr(reconciliation, "The certificate (data.{}) in Secret is not an X.509 certificate; removing it",
                        certName.replace(".", "\\."));
            }
        }
        return remove;
    }

    @Override
    public boolean removeCerts(Map<String, String> newData, Predicate<Map.Entry<String, String>> predicate) {
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
        if (removed.isEmpty()) {
            return false;
        } else {
            if (caConfig.isGeneratePkcs12Stores()) {
                // the certificates removed from the Secret data have to be removed from the store as well
                try {
                    File trustStoreFile = Files.createTempFile("tls", "-truststore").toFile();
                    Files.write(trustStoreFile.toPath(), Util.decodeBytesFromBase64(newData.get(CA_STORE)));
                    try {
                        String trustStorePassword = Util.decodeFromBase64(newData.get(CA_STORE_PASSWORD));
                        certIssuer.deleteFromTrustStore(removed, trustStoreFile, trustStorePassword);
                        newData.put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile.toPath())));
                    } finally {
                        delete(reconciliation, trustStoreFile);
                    }
                } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
            }

            return true;
        }
    }

    private boolean certNeedsRenewal(X509Certificate cert)  {
        Instant notAfter = cert.getNotAfter().toInstant();
        Instant renewalPeriodBegin = notAfter.minus(caConfig.getRenewalDays(), ChronoUnit.DAYS);
        LOGGER.traceCr(reconciliation, "Certificate {} expires on {} renewal period begins on {}", cert.getSubjectX500Principal(), notAfter, renewalPeriodBegin);
        return this.clock.instant().isAfter(renewalPeriodBegin);
    }


    private void addCertCaToTrustStore(String alias, Map<String, String> certData) {
        try {
            File certFile = Files.createTempFile("tls", "-cert").toFile();
            Files.write(certFile.toPath(), Util.decodeBytesFromBase64(certData.get(CA_CRT)));
            try {
                File trustStoreFile = Files.createTempFile("tls", "-truststore").toFile();
                if (certData.containsKey(CA_STORE)) {
                    Files.write(trustStoreFile.toPath(), Util.decodeBytesFromBase64(certData.get(CA_STORE)));
                }
                try {
                    String trustStorePassword = certData.containsKey(CA_STORE_PASSWORD) ?
                            Util.decodeFromBase64(certData.get(CA_STORE_PASSWORD)) :
                            passwordGenerator.generate();
                    certIssuer.addCertToTrustStore(certFile, alias, trustStoreFile, trustStorePassword);
                    certData.put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile.toPath())));
                    certData.put(CA_STORE_PASSWORD, Base64.getEncoder().encodeToString(trustStorePassword.getBytes(StandardCharsets.US_ASCII)));
                } finally {
                    delete(reconciliation, trustStoreFile);
                }
            } finally {
                delete(reconciliation, certFile);
            }

        } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private void generateCaKeyAndCert(Subject subject, Map<String, String> keyData, Map<String, String> certData) {
        try {
            LOGGER.infoCr(reconciliation, "Generating CA with subject={}", subject);

            File keyFile = Files.createTempFile("tls", subject.commonName() + "-key").toFile();
            File certFile = Files.createTempFile("tls", subject.commonName() + "-cert").toFile();

            try {
                certIssuer.generateSelfSignedCert(keyFile, certFile, subject, caConfig.getValidityDays());
                CertAndKey ca;

                if (caConfig.isGeneratePkcs12Stores()) {
                    File trustStoreFile = Files.createTempFile("tls", subject.commonName() + "-truststore").toFile();
                    String trustStorePassword;
                    // if secret already contains the truststore, we have to reuse it without changing password
                    if (certData.containsKey(CA_STORE)) {
                        Files.write(trustStoreFile.toPath(), Util.decodeBytesFromBase64(certData.get(CA_STORE)));
                        trustStorePassword = Util.decodeFromBase64(certData.get(CA_STORE_PASSWORD));
                    } else {
                        trustStorePassword = passwordGenerator.generate();
                    }
                    try {
                        certIssuer.addCertToTrustStore(certFile, CA_CRT, trustStoreFile, trustStorePassword);
                        ca = new CertAndKey(
                                Files.readAllBytes(keyFile.toPath()),
                                Files.readAllBytes(certFile.toPath()),
                                Files.readAllBytes(trustStoreFile.toPath()),
                                null,
                                trustStorePassword);

                        certData.put(CA_STORE, ca.trustStoreAsBase64String());
                        certData.put(CA_STORE_PASSWORD, ca.storePasswordAsBase64String());
                    } finally {
                        delete(reconciliation, trustStoreFile);
                    }
                } else {
                    ca = new CertAndKey(
                            Files.readAllBytes(keyFile.toPath()),
                            Files.readAllBytes(certFile.toPath()),
                            null,
                            null,
                            null);
                }

                certData.put(CA_CRT, ca.certAsBase64String());
                keyData.put(CA_KEY, ca.keyAsBase64String());
            } finally {
                delete(reconciliation, certFile);
                delete(reconciliation, keyFile);
            }
        } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private void renewCaCert(Subject subject, Map<String, String> certData) {
        try {
            LOGGER.infoCr(reconciliation, "Renewing CA with subject={}", subject);

            byte[] bytes = Util.decodeBytesFromBase64(caKeyData.get(CA_KEY));
            File keyFile = Files.createTempFile("tls", subject.commonName() + "-key").toFile();
            Files.write(keyFile.toPath(), bytes);
            File certFile = Files.createTempFile("tls", subject.commonName() + "-cert").toFile();

            try {
                certIssuer.renewSelfSignedCert(keyFile, certFile, subject, caConfig.getValidityDays());
                CertAndKey ca;

                if (caConfig.isGeneratePkcs12Stores()) {
                    File trustStoreFile = Files.createTempFile("tls", subject.commonName() + "-truststore").toFile();
                    try {
                        String trustStorePassword = passwordGenerator.generate();
                        certIssuer.addCertToTrustStore(certFile, CA_CRT, trustStoreFile, trustStorePassword);
                        ca = new CertAndKey(
                                bytes,
                                Files.readAllBytes(certFile.toPath()),
                                Files.readAllBytes(trustStoreFile.toPath()),
                                null,
                                trustStorePassword);
                        certData.put(CA_STORE, ca.trustStoreAsBase64String());
                        certData.put(CA_STORE_PASSWORD, ca.storePasswordAsBase64String());
                    } finally {
                        delete(reconciliation, trustStoreFile);
                    }
                } else {
                    ca = new CertAndKey(
                            bytes,
                            Files.readAllBytes(certFile.toPath()),
                            null,
                            null,
                            null);
                }

                certData.put(CA_CRT, ca.certAsBase64String());
            } finally {
                delete(reconciliation, keyFile);
                delete(reconciliation, certFile);
            }
        } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
