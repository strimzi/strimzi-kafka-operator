/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.common.CertificateExpirationPolicy;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/**
 * A Certificate Authority which can renew its own (self-signed) certificates, and generate signed certificates
 */
@SuppressWarnings("checkstyle:CyclomaticComplexity")
public abstract class Ca {

    /**
     * A certificate entry in a Kubernetes Secret. Used to construct the keys in the Secret data where certificates are stored.
     */
    public enum SecretEntry {
        /**
         * A 64-bit encoded X509 Certificate
         */
        CRT(".crt"),
        /**
         * Entity private key
         */
        KEY(".key"),
        /**
         * Entity certificate and key as a P12 keystore
         */
        P12_KEYSTORE(".p12"),
        /**
         * P12 keystore password
         */
        P12_KEYSTORE_PASSWORD(".password");

        final String suffix;

        SecretEntry(String suffix) {
            this.suffix = suffix;
        }

        /**
         * Build the Kubernetes Secret key to use with this type of SecretEntry.
         *
         * @param prefix to use for the certificate Secret key
         * @return a certificate Secret key with the provided prefix and the suffix of this type of SecretEntry
         */
        public String asKey(String prefix) {
            return prefix + suffix;
        }

        /**
         *
         * @param key to check the type of
         * @return whether this key has the correct suffix for the entry
         */
        private boolean matchesType(String key) {
            return key.endsWith(suffix);
        }

    }

    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(Ca.class);

    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .appendLiteral('T')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral('-')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral('-')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .optionalStart()
            .appendOffsetId()
            .toFormatter().withChronology(IsoChronology.INSTANCE);

    private static final String CA_SECRET_PREFIX = "ca";

    /**
     * Key for storing the CA private key in a Kubernetes Secret
     */
    public static final String CA_KEY = SecretEntry.KEY.asKey(CA_SECRET_PREFIX);

    /**
     * Key for storing the CA public key in a Kubernetes Secret
     */
    public static final String CA_CRT = SecretEntry.CRT.asKey(CA_SECRET_PREFIX);

    /**
     * Key for storing the CA PKCS21 store in a Kubernetes Secret
     */
    public static final String CA_STORE = SecretEntry.P12_KEYSTORE.asKey(CA_SECRET_PREFIX);

    /**
     * Key for storing the PKCS12 store password in a Kubernetes Secret
     */
    public static final String CA_STORE_PASSWORD = SecretEntry.P12_KEYSTORE_PASSWORD.asKey(CA_SECRET_PREFIX);

    /**
     * Organization used in the generated CAs
     */
    public static final String IO_STRIMZI = "io.strimzi";

    /**
     * Annotation for tracking the CA key generation used by Kubernetes resources
     */
    public static final String ANNO_STRIMZI_IO_CA_KEY_GENERATION = Annotations.STRIMZI_DOMAIN + "ca-key-generation";

    /**
     * Annotation for tracking the CA certificate generation used by Kubernetes resources
     */
    public static final String ANNO_STRIMZI_IO_CA_CERT_GENERATION = Annotations.STRIMZI_DOMAIN + "ca-cert-generation";

    /**
     * Annotation for tracking the Cluster CA generation used by Kubernetes resources
     */
    public static final String ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION = Annotations.STRIMZI_DOMAIN + "cluster-ca-cert-generation";

    /**
     * Annotation for tracking the Clients CA generation used by Kubernetes resources
     */
    public static final String ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION = Annotations.STRIMZI_DOMAIN + "clients-ca-cert-generation";

    /**
     * Annotation for tracking the Cluster CA key generation used by Kubernetes resources
     */
    public static final String ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION = Annotations.STRIMZI_DOMAIN + "cluster-ca-key-generation";

    /**
     * Initial generation used for the CAs
     */
    public static final int INIT_GENERATION = 0;

    private final PasswordGenerator passwordGenerator;
    protected final Reconciliation reconciliation;
    private Clock clock;

    /**
     * Enum describing whether an event related to a certificate renewal is happening or not.
     */
    public enum RenewalType {
        /**
         * No changes to the CA, no renewals are happening.
         */
        NOOP() {
            @Override
            public String preDescription(String keySecretName, String certSecretName) {
                return "noop";
            }
            @Override
            public String postDescription(String keySecretName, String certSecretName) {
                return "noop";
            }
        },
        /**
         * Renewal should be done, but was currently postponed because of the maintenance window configuration
         */
        POSTPONED() {
            @Override
            public String preDescription(String keySecretName, String certSecretName) {
                return "CA operation was postponed and will be done in the next maintenance window";
            }
            @Override
            public String postDescription(String keySecretName, String certSecretName) {
                return "postponed";
            }
        },
        /**
         * New CA is being created
         */
        CREATE() {
            @Override
            public String preDescription(String keySecretName, String certSecretName) {
                return "CA key (in " + keySecretName + ") and certificate (in " + certSecretName + ") needs to be created";
            }
            @Override
            public String postDescription(String keySecretName, String certSecretName) {
                return "CA key (in " + keySecretName + ") and certificate (in " + certSecretName + ") created";
            }
        },
        /**
         * CA is being renewed (new public key s generated using the same private key)
         */
        RENEW_CERT() {
            @Override
            public String preDescription(String keySecretName, String certSecretName) {
                return "CA certificate (in " + certSecretName + ") needs to be renewed";
            }
            @Override
            public String postDescription(String keySecretName, String certSecretName) {
                return "CA certificate (in " + certSecretName + ") renewed";
            }
        },
        /**
         * CA is being renewed including new private key
         */
        REPLACE_KEY() {
            @Override
            public String preDescription(String keySecretName, String certSecretName) {
                return "CA key (in " + keySecretName + ") needs to be replaced";
            }
            @Override
            public String postDescription(String keySecretName, String certSecretName) {
                return "CA key (in " + keySecretName + ") replaced";
            }
        };

        RenewalType() {
        }

        /**
         * Pre-renewal description which is used to log what is going to happen.
         *
         * @param keySecretName     Name of the Secret
         * @param certSecretName    Key in the Secret
         *
         * @return  String with the description
         */
        public abstract String preDescription(String keySecretName, String certSecretName);

        /**
         * Post-renewal description which is used to log what was just done.
         *
         * @param keySecretName     Name of the Secret
         * @param certSecretName    Key in the Secret
         *
         * @return  String with the description
         */
        public abstract String postDescription(String keySecretName, String certSecretName);
    }

    protected final String commonName;
    protected final CertManager certManager;
    protected final int validityDays;
    protected final int renewalDays;
    protected final boolean generateCa;
    protected String caCertSecretName;
    protected Secret caCertSecret;
    protected String caKeySecretName;
    protected Secret caKeySecret;
    protected RenewalType renewalType;
    protected boolean caCertsRemoved;
    protected final CertificateExpirationPolicy policy;

    /**
     * Constructs the CA object
     *
     * @param reconciliation        Reconciliation marker
     * @param certManager           Certificate manager instance
     * @param passwordGenerator     Password generator instance
     * @param commonName            Common name which should be used by this CA
     * @param caCertSecretName      Name of the Kubernetes Secret where the CA public key wil be stored
     * @param caCertSecret          Kubernetes Secret where the CA public key will be stored
     * @param caKeySecretName       Name of the Kubernetes Secret where the CA private key wil be stored
     * @param caKeySecret           Kubernetes Secret where the CA private key will be stored
     * @param validityDays          Number of days for which the CA certificate should be value
     * @param renewalDays           Number of day before expiration, when the certificate should be renewed
     * @param generateCa            Flag indicating whether the CA should be generated by Strimzi or not
     * @param policy                Policy defining the behavior when the CA expires (renewal or completely replacing the CA)
     */
    public Ca(Reconciliation reconciliation, CertManager certManager, PasswordGenerator passwordGenerator, String commonName,
              String caCertSecretName, Secret caCertSecret,
              String caKeySecretName, Secret caKeySecret,
              int validityDays, int renewalDays, boolean generateCa, CertificateExpirationPolicy policy) {
        this.reconciliation = reconciliation;
        this.commonName = commonName;
        this.caCertSecret = caCertSecret;
        this.caCertSecretName = caCertSecretName;
        this.caKeySecret = caKeySecret;
        this.caKeySecretName = caKeySecretName;
        this.certManager = certManager;
        this.passwordGenerator = passwordGenerator;
        this.validityDays = validityDays;
        this.renewalDays = renewalDays;
        this.generateCa = generateCa;
        this.policy = policy == null ? CertificateExpirationPolicy.RENEW_CERTIFICATE : policy;
        this.renewalType = RenewalType.NOOP;
        this.clock = Clock.systemUTC();
    }

    /**
     * Sets the clock to some specific value. This method is useful in testing. But it has to be public because of how
     * the Ca class is shared and inherited between different modules.
     *
     * @param clock     Clock instance that should be used to determine time
     */
    public void setClock(Clock clock) {
        this.clock = clock;
    }

    /**
     * Extracts the CA generation from the CA
     *
     * @return CA generation or the initial generation if no generation is set
     */
    public int caCertGeneration() {
        return Annotations.intAnnotation(caCertSecret(), ANNO_STRIMZI_IO_CA_CERT_GENERATION, INIT_GENERATION);
    }

    /**
     * Extracts the CA key generation from the CA
     *
     * @return CA key generation or the initial generation if no generation is set
     */
    public int caKeyGeneration() {
        return Annotations.intAnnotation(caKeySecret(), ANNO_STRIMZI_IO_CA_KEY_GENERATION, INIT_GENERATION);
    }

    protected static void delete(Reconciliation reconciliation, File file) {
        if (!file.delete()) {
            LOGGER.warnCr(reconciliation, "{} cannot be deleted", file.getName());
        }
    }

    /**
     * Returns the given {@code cert} and {@code key} values from the given {@code Secret} as a {@code CertAndKey},
     * or null if the given {@code secret} is null.
     * An exception is thrown if the given {@code secret} is non-null, but does not contain the given
     * entries in its {@code data}.
     *
     * @param secret The secret.
     * @param key The key.
     * @param cert The cert.
     * @param keyStore The keyStore.
     * @param keyStorePassword The store password.
     * @return The CertAndKey.
     */
    public static CertAndKey asCertAndKey(Secret secret, String key, String cert, String keyStore, String keyStorePassword) {
        if (secret == null || secret.getData() == null) {
            return null;
        } else {
            String keyData = secret.getData().get(key);
            if (keyData == null) {
                throw new RuntimeException("The Secret " + secret.getMetadata().getNamespace() + "/" + secret.getMetadata().getName() + " is missing the key " + key);
            }
            String certData = secret.getData().get(cert);
            if (certData == null) {
                throw new RuntimeException("The Secret " + secret.getMetadata().getNamespace() + "/" + secret.getMetadata().getName() + " is missing the key " + cert);
            }
            return new CertAndKey(
                    Util.decodeBytesFromBase64(keyData),
                    Util.decodeBytesFromBase64(certData),
                    null,
                    Util.decodeBytesFromBase64(secret.getData().get(keyStore)),
                    Util.decodeFromBase64(secret.getData().get(keyStorePassword)));
        }
    }

    /**
     * Adds a certificate into a PKCS12 keystore
     *
     * @param alias     Alias under which it should be stored in the PKCS12 store
     * @param key       Private key
     * @param cert      Public key
     *
     * @return  PKCS12 store with the certificate
     *
     * @throws IOException  Throws an IOException if something fails when working with the files
     */
    public CertAndKey addKeyAndCertToKeyStore(String alias, byte[] key, byte[] cert) throws IOException {
        File keyFile = Files.createTempFile("tls", "key").toFile();
        File certFile = Files.createTempFile("tls", "cert").toFile();
        File keyStoreFile = Files.createTempFile("tls", "p12").toFile();

        Files.write(keyFile.toPath(), key);
        Files.write(certFile.toPath(), cert);

        String keyStorePassword = passwordGenerator.generate();
        certManager.addKeyAndCertToKeyStore(keyFile, certFile, alias, keyStoreFile, keyStorePassword);

        CertAndKey result = new CertAndKey(
                Files.readAllBytes(keyFile.toPath()),
                Files.readAllBytes(certFile.toPath()),
                null,
                Files.readAllBytes(keyStoreFile.toPath()),
                keyStorePassword);

        delete(reconciliation, keyFile);
        delete(reconciliation, certFile);
        delete(reconciliation, keyStoreFile);

        return result;
    }

    protected CertAndKey generateSignedCert(Subject subject,
                                           File csrFile, File keyFile, File certFile, File keyStoreFile) throws IOException {
        LOGGER.infoCr(reconciliation, "Generating certificate {}, signed by CA {}", subject, this);

        certManager.generateCsr(keyFile, csrFile, subject);
        certManager.generateCert(csrFile, currentCaKey(), currentCaCertBytes(),
                certFile, subject, validityDays);

        String keyStorePassword = passwordGenerator.generate();
        certManager.addKeyAndCertToKeyStore(keyFile, certFile, subject.commonName(), keyStoreFile, keyStorePassword);

        return new CertAndKey(
                Files.readAllBytes(keyFile.toPath()),
                Files.readAllBytes(certFile.toPath()),
                null,
                Files.readAllBytes(keyStoreFile.toPath()),
                keyStorePassword);
    }

    /**
     * Generates a certificate signed by this CA
     *
     * @param commonName The CN of the certificate to be generated.
     * @return The CertAndKey
     * @throws IOException If the cert could not be generated.
     */
    public CertAndKey generateSignedCert(String commonName) throws IOException {
        return generateSignedCert(commonName, null);
    }

    /**
     * Generates a certificate signed by this CA
     *
     * @param commonName The CN of the certificate to be generated.
     * @param organization The O of the certificate to be generated. May be null.
     * @return The CertAndKey
     * @throws IOException If the cert could not be generated.
     */
    public CertAndKey generateSignedCert(String commonName, String organization) throws IOException {
        File csrFile = Files.createTempFile("tls", "csr").toFile();
        File keyFile = Files.createTempFile("tls", "key").toFile();
        File certFile = Files.createTempFile("tls", "cert").toFile();
        File keyStoreFile = Files.createTempFile("tls", "p12").toFile();

        Subject.Builder subject = new Subject.Builder();

        if (organization != null) {
            subject.withOrganizationName(organization);
        }

        subject.withCommonName(commonName);

        CertAndKey result = generateSignedCert(subject.build(),
                csrFile, keyFile, certFile, keyStoreFile);

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
        boolean isExpiring = false;

        try {
            X509Certificate currentCert = cert(secret, certKey);
            isExpiring = certNeedsRenewal(currentCert);
        } catch (RuntimeException e) {
            // TODO: We should mock the certificates properly so that this doesn't fail in tests (not now => long term :-o)
            LOGGER.debugCr(reconciliation, "Failed to parse existing certificate", e);
        }

        return isExpiring;
    }

    /**
     * Create the CA {@code Secrets} if they don't exist, otherwise if within the renewal period then either renew the CA cert
     * or replace the CA cert and key, according to the configured policy.
     * After calling this method {@link #certRenewed()} and {@link #certsRemoved()}
     * will return whether the certificate was renewed and whether expired secrets were removed from the Secret.
     * @param namespace The namespace containing the cluster.
     * @param clusterName The name of the cluster.
     * @param labels The labels of the {@code Secrets} created.
     * @param additionalLabels The additional labels of the {@code Secrets} created.
     * @param additionalAnnotations The additional annotations of the {@code Secrets} created.
     * @param ownerRef The owner of the {@code Secrets} created.
     * @param maintenanceWindowSatisfied Flag indicating whether we are in the maintenance window
     */
    public void createRenewOrReplace(String namespace, String clusterName, Map<String, String> labels, Map<String, String> additionalLabels, Map<String, String> additionalAnnotations, OwnerReference ownerRef, boolean maintenanceWindowSatisfied) {
        X509Certificate currentCert = cert(caCertSecret, CA_CRT);
        Map<String, String> certData;
        Map<String, String> keyData;
        int caCertGeneration = certGeneration();
        int caKeyGeneration = keyGeneration();
        if (!generateCa) {
            certData = caCertSecret != null ? caCertSecret.getData() : emptyMap();
            keyData = caKeySecret != null ? singletonMap(CA_KEY, caKeySecret.getData().get(CA_KEY)) : emptyMap();
            renewalType = hasCaCertGenerationChanged() ? RenewalType.REPLACE_KEY : RenewalType.NOOP;
            caCertsRemoved = false;
        } else {
            this.renewalType = shouldCreateOrRenew(currentCert, namespace, clusterName, maintenanceWindowSatisfied);
            LOGGER.debugCr(reconciliation, "{} renewalType {}", this, renewalType);
            switch (renewalType) {
                case CREATE:
                    keyData = new HashMap<>(1);
                    certData = new HashMap<>(3);
                    generateCaKeyAndCert(nextCaSubject(caKeyGeneration), keyData, certData);
                    break;
                case REPLACE_KEY:
                    keyData = new HashMap<>(1);
                    certData = new HashMap<>(caCertSecret.getData());
                    if (certData.containsKey(CA_CRT)) {
                        String notAfterDate = DATE_TIME_FORMATTER.format(currentCert.getNotAfter().toInstant().atZone(ZoneId.of("Z")));
                        addCertCaToTrustStore("ca-" + notAfterDate + SecretEntry.CRT.suffix, certData);
                        certData.put("ca-" + notAfterDate + SecretEntry.CRT.suffix, certData.remove(CA_CRT));
                    }
                    ++caCertGeneration;
                    generateCaKeyAndCert(nextCaSubject(++caKeyGeneration), keyData, certData);
                    break;
                case RENEW_CERT:
                    keyData = caKeySecret.getData();
                    certData = new HashMap<>(3);
                    ++caCertGeneration;
                    renewCaCert(nextCaSubject(caKeyGeneration), certData);
                    break;
                default:
                    keyData = caKeySecret.getData();
                    certData = caCertSecret.getData();
                    // coming from an older version, the secret could not have the CA truststore
                    if (!certData.containsKey(CA_STORE)) {
                        addCertCaToTrustStore(CA_CRT, certData);
                    }
            }
            this.caCertsRemoved = removeCerts(certData, this::removeExpiredCert) > 0;
        }

        if (caCertsRemoved) {
            LOGGER.infoCr(reconciliation, "{}: Expired CA certificates removed", this);
        }
        if (renewalType != RenewalType.NOOP && renewalType != RenewalType.POSTPONED) {
            LOGGER.debugCr(reconciliation, "{}: {}", this, renewalType.postDescription(caKeySecretName, caCertSecretName));
        }

        // cluster CA certificate annotation handling
        Map<String, String> certAnnotations = new HashMap<>(2);
        certAnnotations.put(ANNO_STRIMZI_IO_CA_CERT_GENERATION, String.valueOf(caCertGeneration));

        if (renewalType.equals(RenewalType.POSTPONED)
                && this.caCertSecret.getMetadata() != null
                && Annotations.hasAnnotation(caCertSecret, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW))   {
            certAnnotations.put(Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, Annotations.stringAnnotation(caCertSecret, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, "false"));
        }

        Map<String, String> keyAnnotations = new HashMap<>(2);
        keyAnnotations.put(ANNO_STRIMZI_IO_CA_KEY_GENERATION, String.valueOf(caKeyGeneration));

        if (renewalType.equals(RenewalType.POSTPONED)
                && this.caKeySecret.getMetadata() != null
                && Annotations.hasAnnotation(caKeySecret, Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE))   {
            keyAnnotations.put(Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE, Annotations.stringAnnotation(caKeySecret, Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE, "false"));
        }

        caCertSecret = createCaSecret(namespace, caCertSecretName, certData, Util.mergeLabelsOrAnnotations(labels, additionalLabels),
                Util.mergeLabelsOrAnnotations(certAnnotations, additionalAnnotations), ownerRef);

        caKeySecret = createCaSecret(namespace, caKeySecretName, keyData, labels,
                keyAnnotations, ownerRef);
    }

    /**
     * Create a Kubernetes secret containing the provided secret data section
     *
     * @param namespace Namespace
     * @param name Secret name
     * @param data Map with secret data / files
     * @param labels Labels to add to the Secret
     * @param annotations annotations to add to the Secret
     * @param ownerReference owner of the Secret
     * @return the Secret
     */
    private static Secret createCaSecret(String namespace, String name, Map<String, String> data,
                               Map<String, String> labels, Map<String, String> annotations, OwnerReference ownerReference) {
        List<OwnerReference> or = ownerReference != null ? singletonList(ownerReference) : emptyList();
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .withAnnotations(annotations)
                    .withOwnerReferences(or)
                .endMetadata()
                .withType("Opaque")
                .withData(data)
                .build();
    }

    private Subject nextCaSubject(int version) {
        return new Subject.Builder()
        // Key replacements does not work if both old and new CA certs have the same subject DN, so include the
        // key generation in the DN so the certificates appear distinct during CA key replacement.
            .withCommonName(commonName + " v" + version)
            .withOrganizationName(IO_STRIMZI).build();
    }

    private RenewalType shouldCreateOrRenew(X509Certificate currentCert, String namespace, String clusterName, boolean maintenanceWindowSatisfied) {
        String reason = null;
        RenewalType renewalType = RenewalType.NOOP;
        if (caKeySecret == null
                || caKeySecret.getData() == null
                || caKeySecret.getData().get(CA_KEY) == null) {
            reason = "CA key secret " + caKeySecretName + " is missing or lacking data." + CA_KEY.replace(".", "\\.");
            renewalType = RenewalType.CREATE;
        } else if (this.caCertSecret == null
                || this.caCertSecret.getData() == null
                || this.caCertSecret.getData().get(CA_CRT) == null) {
            reason = "CA certificate secret " + caCertSecretName + " is missing or lacking data." + CA_CRT.replace(".", "\\.");
            renewalType = RenewalType.RENEW_CERT;
        } else if (this.caCertSecret.getMetadata() != null
                && Annotations.booleanAnnotation(this.caCertSecret, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, false)) {
            reason = "CA certificate secret " + caCertSecretName + " is annotated with " + Annotations.ANNO_STRIMZI_IO_FORCE_RENEW;

            if (maintenanceWindowSatisfied) {
                renewalType = RenewalType.RENEW_CERT;
            } else {
                renewalType = RenewalType.POSTPONED;
            }
        } else if (this.caKeySecret.getMetadata() != null
                && Annotations.booleanAnnotation(this.caKeySecret, Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE, false)) {
            reason = "CA key secret " + caKeySecretName + " is annotated with " + Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE;

            if (maintenanceWindowSatisfied) {
                renewalType = RenewalType.REPLACE_KEY;
            } else {
                renewalType = RenewalType.POSTPONED;
            }
        } else if (currentCert != null
                && certNeedsRenewal(currentCert)) {
            reason = "Within renewal period for CA certificate (expires on " + currentCert.getNotAfter() + ")";

            if (maintenanceWindowSatisfied) {
                switch (policy) {
                    case REPLACE_KEY:
                        renewalType = RenewalType.REPLACE_KEY;
                        break;
                    case RENEW_CERTIFICATE:
                        renewalType = RenewalType.RENEW_CERT;
                        break;
                }
            } else {
                renewalType = RenewalType.POSTPONED;
            }
        }

        logRenewalState(currentCert, namespace, clusterName, renewalType, reason);
        return renewalType;
    }

    private void logRenewalState(X509Certificate currentCert, String namespace, String clusterName, RenewalType renewalType, String reason) {
        switch (renewalType) {
            case REPLACE_KEY:
            case RENEW_CERT:
            case CREATE:
                if (generateCa) {
                    LOGGER.debugCr(reconciliation, "{}: {}: {}", this, renewalType.preDescription(caKeySecretName, caCertSecretName), reason);
                } else {
                    LOGGER.warnCr(reconciliation, "{}: {}: {}", this, renewalType.preDescription(caKeySecretName, caCertSecretName), reason);
                }
                break;
            case POSTPONED:
                LOGGER.warnCr(reconciliation, "{}: {}: {}", this, renewalType.preDescription(caKeySecretName, caCertSecretName), reason);
                break;
            case NOOP:
                LOGGER.debugCr(reconciliation, "{}: The CA certificate in secret {} already exists and does not need renewing", this, caCertSecretName);
                break;
        }
        if (!generateCa) {
            if (renewalType.equals(RenewalType.RENEW_CERT)) {
                LOGGER.warnCr(reconciliation, "The certificate (data.{}) in Secret {} in namespace {} needs to be renewed " +
                                "and it is not configured to automatically renew. This needs to be manually updated before that date. " +
                                "Alternatively, configure Kafka.spec.tlsCertificates.generateCertificateAuthority=true in the Kafka resource with name {} in namespace {}.",
                        CA_CRT.replace(".", "\\."), this.caCertSecretName, namespace,
                        currentCert.getNotAfter());
            } else if (renewalType.equals(RenewalType.REPLACE_KEY)) {
                LOGGER.warnCr(reconciliation, "The private key (data.{}) in Secret {} in namespace {} needs to be renewed " +
                                "and it is not configured to automatically renew. This needs to be manually updated before that date. " +
                                "Alternatively, configure Kafka.spec.tlsCertificates.generateCertificateAuthority=true in the Kafka resource with name {} in namespace {}.",
                        CA_KEY.replace(".", "\\."), this.caKeySecretName, namespace,
                        currentCert.getNotAfter());
            } else if (caCertSecret == null) {
                LOGGER.warnCr(reconciliation, "The certificate (data.{}) in Secret {} and the private key (data.{}) in Secret {} in namespace {} " +
                                "needs to be configured with a Base64 encoded PEM-format certificate. " +
                                "Alternatively, configure Kafka.spec.tlsCertificates.generateCertificateAuthority=true in the Kafka resource with name {} in namespace {}.",
                        CA_CRT.replace(".", "\\."), this.caCertSecretName,
                        CA_KEY.replace(".", "\\."), this.caKeySecretName, namespace,
                        clusterName, namespace);
            }
        }
    }

    /**
     * @return the CA cert secret, which contains both the current CA cert and also previous, still valid certs.
     */
    public Secret caCertSecret() {
        return caCertSecret;
    }

    /**
     * @return the CA key secret, which contains the current CA private key.
     */
    public Secret caKeySecret() {
        return caKeySecret;
    }

    /**
     * @return The current CA certificate as bytes.
     */
    public byte[] currentCaCertBytes() {
        return Util.decodeBytesFromBase64(caCertSecret().getData().get(CA_CRT));
    }

    /**
     * @return The base64 encoded bytes of the current CA certificate.
     */
    public String currentCaCertBase64() {
        return caCertSecret().getData().get(CA_CRT);
    }

    /**
     * @return The current CA key as bytes.
     */
    public byte[] currentCaKey() {
        return Util.decodeBytesFromBase64(caKeySecret().getData().get(CA_KEY));
    }

    /**
     * True if the last call to {@link #createRenewOrReplace(String, String, Map, Map, Map, OwnerReference, boolean)}
     * resulted in expired certificates being removed from the CA {@code Secret}.
     * @return Whether any expired certificates were removed.
     */
    public boolean certsRemoved() {
        return this.caCertsRemoved;
    }

    /**
     * True if the last call to {@link #createRenewOrReplace(String, String, Map, Map, Map, OwnerReference, boolean)}
     * resulted in a renewed CA certificate.
     * @return Whether the certificate was renewed.
     */
    public boolean certRenewed() {
        return renewalType.equals(RenewalType.RENEW_CERT) || renewalType.equals(RenewalType.REPLACE_KEY);
    }

    /**
     * True if the last call to {@link #createRenewOrReplace(String, String, Map, Map, Map, OwnerReference, boolean)}
     * resulted in a replaced CA key.
     * @return Whether the key was replaced.
     */
    public boolean keyReplaced() {
        return renewalType.equals(RenewalType.REPLACE_KEY);
    }

    /**
     * @return  Returns true if the key was newly created
     */
    public boolean keyCreated() {
        return renewalType.equals(RenewalType.CREATE);
    }

    /**
     * @return the generation of the current CA certificate
     */
    public int certGeneration() {
        if (caCertSecret != null) {
            if (!Annotations.hasAnnotation(caCertSecret, ANNO_STRIMZI_IO_CA_CERT_GENERATION)) {
                LOGGER.warnOp("Secret {}/{} is missing generation annotation {}",
                        caCertSecret.getMetadata().getNamespace(), caCertSecret.getMetadata().getName(), ANNO_STRIMZI_IO_CA_CERT_GENERATION);
            }
            return caCertGeneration();
        }
        return INIT_GENERATION;
    }

    /**
     * @return the generation of the current CA certificate as an annotation
     */
    public Map.Entry<String, String> caCertGenerationFullAnnotation() {
        return Map.entry(caCertGenerationAnnotation(), String.valueOf(certGeneration()));
    }

    /**
     * @return the generation of the current CA key
     */
    public int keyGeneration() {
        if (caKeySecret != null) {
            if (!Annotations.hasAnnotation(caKeySecret, ANNO_STRIMZI_IO_CA_KEY_GENERATION)) {
                LOGGER.warnOp("Secret {}/{} is missing generation annotation {}",
                        caKeySecret.getMetadata().getNamespace(), caKeySecret.getMetadata().getName(), ANNO_STRIMZI_IO_CA_KEY_GENERATION);
            }
            return caKeyGeneration();
        }
        return INIT_GENERATION;
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
            X509Certificate cert = x509Certificate(Util.decodeBytesFromBase64(certText));
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

    /**
     * Remove certificates from the CA related Secret and store which match the provided predicate
     *
     * @param newData data section of the CA Secret containing certificates
     * @param predicate predicate to match for removing a certificate
     * @return the number of removed certificates
     */
    protected int removeCerts(Map<String, String> newData, Predicate<Map.Entry<String, String>> predicate) {
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

        if (removed.size() > 0) {
            // the certificates removed from the Secret data has tobe removed from the store as well
            try {
                File trustStoreFile = Files.createTempFile("tls", "-truststore").toFile();
                Files.write(trustStoreFile.toPath(), Util.decodeBytesFromBase64(newData.get(CA_STORE)));
                try {
                    String trustStorePassword = Util.decodeFromBase64(newData.get(CA_STORE_PASSWORD));
                    certManager.deleteFromTrustStore(removed, trustStoreFile, trustStorePassword);
                    newData.put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile.toPath())));
                } finally {
                    delete(reconciliation, trustStoreFile);
                }
            } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        return removed.size();
    }

    private boolean certNeedsRenewal(X509Certificate cert)  {
        Instant notAfter = cert.getNotAfter().toInstant();
        Instant renewalPeriodBegin = notAfter.minus(renewalDays, ChronoUnit.DAYS);
        LOGGER.traceCr(reconciliation, "Certificate {} expires on {} renewal period begins on {}", cert.getSubjectX500Principal(), notAfter, renewalPeriodBegin);
        return this.clock.instant().isAfter(renewalPeriodBegin);
    }

    /**
     * Extracts X509 certificate from a Kubernetes Secret
     *
     * @param secret    Kubernetes Secret with the certificate
     * @param key       Key under which the certificate is stored in the Secret
     *
     * @return  An X509Certificate instance with the certificate
     */
    public static X509Certificate cert(Secret secret, String key)  {
        if (secret == null || secret.getData() == null || secret.getData().get(key) == null) {
            return null;
        }
        byte[] bytes = Util.decodeBytesFromBase64(secret.getData().get(key));
        try {
            return x509Certificate(bytes);
        } catch (CertificateException e) {
            throw new RuntimeException("Failed to decode certificate in data." + key.replace(".", "\\.") + " of Secret " + secret.getMetadata().getName(), e);
        }
    }

    /**
     * Returns set of all public keys (all .crt records) from a secret
     *
     * @param secret    Kubernetes Secret with certificates
     *
     * @return          Set with X509Certificate instances
     */
    public static Set<X509Certificate> certs(Secret secret)  {
        if (secret == null || secret.getData() == null) {
            return Set.of();
        } else {
            return secret
                    .getData()
                    .entrySet()
                    .stream()
                    .filter(record -> SecretEntry.CRT.matchesType(record.getKey()))
                    .map(record -> {
                        byte[] bytes = Util.decodeBytesFromBase64(record.getValue());
                        try {
                            return x509Certificate(bytes);
                        } catch (CertificateException e) {
                            throw new RuntimeException("Failed to decode certificate in data." + record.getKey().replace(".", "\\.") + " of Secret " + secret.getMetadata().getName(), e);
                        }
                    })
                    .collect(Collectors.toSet());
        }
    }

    /**
     * Creates X509Certificate instance from a byte array containing a certificate.
     *
     * @param bytes     Bytes with the X509 certificate
     *
     * @throws CertificateException     Thrown when the creation of the X509Certificate instance fails. Typically, this
     *                                  would happen because the bytes do not contain a valid X509 certificate.
     *
     * @return  X509Certificate instance created based on the Certificate bytes
     */
    public static X509Certificate x509Certificate(byte[] bytes) throws CertificateException {
        CertificateFactory factory = certificateFactory();
        return x509Certificate(factory, bytes);
    }

    static X509Certificate x509Certificate(CertificateFactory factory, byte[] bytes) throws CertificateException {
        Certificate certificate = factory.generateCertificate(new ByteArrayInputStream(bytes));
        if (certificate instanceof X509Certificate) {
            return (X509Certificate) certificate;
        } else {
            throw new CertificateException("Not an X509Certificate: " + certificate);
        }
    }

    static CertificateFactory certificateFactory() {
        CertificateFactory factory;
        try {
            factory = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            throw new RuntimeException("No security provider with support for X.509 certificates", e);
        }
        return factory;
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
                    certManager.addCertToTrustStore(certFile, alias, trustStoreFile, trustStorePassword);
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
            try {
                File certFile = Files.createTempFile("tls", subject.commonName() + "-cert").toFile();
                try {
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
                        certManager.generateSelfSignedCert(keyFile, certFile, subject, validityDays);
                        certManager.addCertToTrustStore(certFile, CA_CRT, trustStoreFile, trustStorePassword);
                        CertAndKey ca = new CertAndKey(
                                Files.readAllBytes(keyFile.toPath()),
                                Files.readAllBytes(certFile.toPath()),
                                Files.readAllBytes(trustStoreFile.toPath()),
                                null,
                                trustStorePassword);
                        certData.put(CA_CRT, ca.certAsBase64String());
                        keyData.put(CA_KEY, ca.keyAsBase64String());
                        certData.put(CA_STORE, ca.trustStoreAsBase64String());
                        certData.put(CA_STORE_PASSWORD, ca.storePasswordAsBase64String());
                    } finally {
                        delete(reconciliation, trustStoreFile);
                    }
                } finally {
                    delete(reconciliation, certFile);
                }
            } finally {
                delete(reconciliation, keyFile);
            }
        } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private void renewCaCert(Subject subject, Map<String, String> certData) {
        try {
            LOGGER.infoCr(reconciliation, "Renewing CA with subject={}", subject);

            byte[] bytes = Util.decodeBytesFromBase64(caKeySecret.getData().get(CA_KEY));
            File keyFile = Files.createTempFile("tls", subject.commonName() + "-key").toFile();
            try {
                Files.write(keyFile.toPath(), bytes);
                File certFile = Files.createTempFile("tls", subject.commonName() + "-cert").toFile();
                try {
                    File trustStoreFile = Files.createTempFile("tls", subject.commonName() + "-truststore").toFile();
                    try {
                        String trustStorePassword = passwordGenerator.generate();
                        certManager.renewSelfSignedCert(keyFile, certFile, subject, validityDays);
                        certManager.addCertToTrustStore(certFile, CA_CRT, trustStoreFile, trustStorePassword);
                        CertAndKey ca = new CertAndKey(
                                bytes,
                                Files.readAllBytes(certFile.toPath()),
                                Files.readAllBytes(trustStoreFile.toPath()),
                                null,
                                trustStorePassword);
                        certData.put(CA_CRT, ca.certAsBase64String());
                        certData.put(CA_STORE, ca.trustStoreAsBase64String());
                        certData.put(CA_STORE_PASSWORD, ca.storePasswordAsBase64String());
                    } finally {
                        delete(reconciliation, trustStoreFile);
                    }
                } finally {
                    delete(reconciliation, certFile);
                }
            } finally {
                delete(reconciliation, keyFile);
            }
        } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return the name of the annotation bringing the generation of the specific CA certificate type (cluster or clients)
     *         on the Secrets containing certificates signed by that CA (i.e. ZooKeeper nodes, Kafka brokers, ...)
     */
    protected abstract String caCertGenerationAnnotation();

    /**
     * @return if the current (cluster or clients) CA certificate generation is changed compared to the one
     *         brought on Secrets containing certificates signed by that CA (i.e. ZooKeeper nodes, Kafka brokers, ...)
     */
    protected abstract boolean hasCaCertGenerationChanged();

    /**
     * It checks if the current (cluster or clients) CA certificate generation is changed compared to the one
     * brought by the corresponding annotation on the provided Secret (i.e. ZooKeeper nodes, Kafka brokers, ...)
     *
     * @param secret Secret containing certificates signed by the current (clients or cluster) CA
     * @return if the current (cluster or clients) CA certificate generation is changed compared to the one
     *         brought by the corresponding annotation on the provided Secret
     */
    public boolean hasCaCertGenerationChanged(Secret secret) {
        if (secret != null) {
            String caCertGenerationAnno = Annotations.stringAnnotation(secret, caCertGenerationAnnotation(), null);
            int currentCaCertGeneration = certGeneration();
            LOGGER.debugOp("Secret {}/{} generation anno = {}, current CA generation = {}",
                    secret.getMetadata().getNamespace(), secret.getMetadata().getName(), caCertGenerationAnno, currentCaCertGeneration);
            return caCertGenerationAnno != null && Integer.parseInt(caCertGenerationAnno) != currentCaCertGeneration;
        }
        return false;
    }


    /**
     * Generates the expiration date as epoch of the CA certificate.
     * @return  Epoch representation of the expiration date of the certificate
     * @throws  RuntimeException if the certificate cannot be decoded or the cert does not exist
     */
    public long getCertificateExpirationDateEpoch() {
        var cert = cert(caCertSecret, CA_CRT);
        if (cert == null) {
            throw new RuntimeException(CA_CRT + " does not exist in the secret " + caCertSecret);
        }
        return cert.getNotAfter().getTime();
    }
}
