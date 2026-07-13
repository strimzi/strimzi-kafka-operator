/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.ca;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

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
         * Checks whether the key has the desired suffix based on the entry.
         *
         * @param key   The key that will be checked whether it matches
         *
         * @return  True if the key matches. False otherwise.
         */
        public boolean matchesType(String key) {
            return key.endsWith(suffix);
        }
    }

    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(Ca.class);

    /**
     * DateTimeFormatter used for renaming old certificates
     */
    public static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
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

    protected static final String CA_SECRET_PREFIX = "ca";

    /**
     * Key for storing the CA private key in a Kubernetes Secret
     */
    public static final String CA_KEY = SecretEntry.KEY.asKey(CA_SECRET_PREFIX);

    /**
     * Key for storing the CA public key in a Kubernetes Secret
     */
    public static final String CA_CRT = SecretEntry.CRT.asKey(CA_SECRET_PREFIX);

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
     * Pattern used for the old CA certificate during CA renewal. This pattern is used to recognize this certificate
     * and delete it when it is not needed anymore.
     */
    private static final Pattern OLD_CA_CERT_PATTERN = Pattern.compile("^ca-\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}Z.crt$");

    /**
     * Initial generation used for the CAs
     */
    public static final int INIT_GENERATION = 0;

    protected final Reconciliation reconciliation;
    Clock clock;

    /**
     * Ca Role
     */
    public enum CaRole {
        /**
         * Cluster Ca
         */
        CLUSTER_CA,
        /**
         * Clients Ca
         */
        CLIENTS_CA;

        /**
         * Get Ca name based on the role
         *
         * @return Ca name
         */
        public String caName() {
            return switch (this) {
                case CLUSTER_CA -> "Cluster CA";
                case CLIENTS_CA -> "Clients CA";
            };
        }

        /**
         * Get Ca common name based on the role
         *
         * @return Ca common name
         */
        public String caCommonName() {
            return switch (this) {
                case CLUSTER_CA -> "cluster-ca";
                case CLIENTS_CA -> "clients-ca";
            };
        }
    }

    /**
     * Enum describing whether an event related to a certificate renewal is happening or not.
     */
    public enum RenewalType {
        /**
         * No changes to the CA, no renewals are happening.
         */
        NOOP() {
            @Override
            public String preDescription(String caName) {
                return "CA key and certificate (in " + caName + " Secrets) already exist and do not need replacing or renewing";
            }
            @Override
            public String postDescription(String caName) {
                return "noop";
            }
        },
        /**
         * Renewal should be done, but was currently postponed because of the maintenance window configuration
         */
        POSTPONED() {
            @Override
            public String preDescription(String caName) {
                return "CA operation was postponed and will be done in the next maintenance window";
            }
            @Override
            public String postDescription(String caName) {
                return "postponed";
            }
        },
        /**
         * New CA is being created
         */
        CREATE() {
            @Override
            public String preDescription(String caName) {
                return "CA key and certificate (in " + caName + " Secrets) needs to be created";
            }
            @Override
            public String postDescription(String caName) {
                return "CA key and certificate (in " + caName + " Secrets) created";
            }
        },
        /**
         * CA is being renewed (new public key s generated using the same private key)
         */
        RENEW_CERT() {
            @Override
            public String preDescription(String caName) {
                return "CA certificate (in " + caName + " Secret) needs to be renewed";
            }
            @Override
            public String postDescription(String caName) {
                return "CA certificate (in " + caName + " Secret) renewed";
            }
        },
        /**
         * CA is being renewed including new private key
         */
        REPLACE_KEY() {
            @Override
            public String preDescription(String caName) {
                return "CA key (in " + caName + " Secret) needs to be replaced";
            }
            @Override
            public String postDescription(String caName) {
                return "CA key (in " + caName + " Secret) replaced";
            }
        };

        RenewalType() {
        }

        /**
         * Pre-renewal description which is used to log what is going to happen.
         *
         * @param caName The name of the CA being renewed
         *
         * @return  String with the description
         */
        public abstract String preDescription(String caName);

        /**
         * Post-renewal description which is used to log what was just done.
         *
         * @param caName The name of the CA being renewed
         *
         * @return  String with the description
         */
        public abstract String postDescription(String caName);
    }

    protected int caCertGeneration;
    protected int caKeyGeneration;
    protected Map<String, String> caCertData;
    protected Map<String, String> caKeyData;
    protected RenewalType renewalType;
    protected boolean caCertsRemoved;
    protected final CaConfig caConfig;
    protected final CaRole caRole;

    /**
     * Constructs the CA object
     *
     * @param reconciliation Reconciliation marker
     * @param caRole         Ca Role
     * @param caCertSecret   Kubernetes Secret where the CA public key is stored
     * @param caKeySecret    Kubernetes Secret where the CA private key is stored
     * @param caConfig       Certificate Authority configuration
     */
    public Ca(Reconciliation reconciliation,
              CaRole caRole,
              Secret caCertSecret,
              Secret caKeySecret,
              CaConfig caConfig) {
        this.reconciliation = reconciliation;
        this.caRole = caRole;
        this.caConfig = caConfig;
        this.caCertGeneration = initCaCertGeneration(caCertSecret);
        this.caCertData = caCertSecret == null ? new HashMap<>() : caCertSecret.getData();
        this.caKeyGeneration = initCaKeyGeneration(caKeySecret, caCertSecret);
        this.caKeyData = caKeySecret == null ? new HashMap<>() : Map.of(CA_KEY, caKeySecret.getData().get(CA_KEY));
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
     * Extracts the CA generation from the CA cert Secret
     *
     * @param caCertSecret Secret to extract the CA cert from
     * @return CA generation or the initial generation if no generation is set
     */
    protected int initCaCertGeneration(Secret caCertSecret) {
        if (caCertSecret != null) {
            if (!Annotations.hasAnnotation(caCertSecret, ANNO_STRIMZI_IO_CA_CERT_GENERATION)) {
                LOGGER.warnOp("Secret {}/{} is missing generation annotation {}",
                        caCertSecret.getMetadata().getNamespace(), caCertSecret.getMetadata().getName(), ANNO_STRIMZI_IO_CA_CERT_GENERATION);
            }
            return Annotations.intAnnotation(caCertSecret, ANNO_STRIMZI_IO_CA_CERT_GENERATION, INIT_GENERATION);
        }
        return INIT_GENERATION;
    }

    /**
     * Extracts the CA key generation from the CA cert or CA key Secret
     *
     * @param caKeySecret CA key Secret
     * @param caCertSecret CA cert Secret
     * @return CA key generation
     */
    protected abstract int initCaKeyGeneration(Secret caKeySecret, Secret caCertSecret);

    /**
     * Gets the CA certificate data, which contains both the current CA cert and also previous, still valid certs.
     *
     * @return the CA cert data, which contains both the current CA cert and also previous, still valid certs.
     */
    public Map<String, String> caCertData() {
        return caCertData;
    }

    /**
     * Gets the CA key data, which contains the current CA private key.
     *
     * @return the CA key data, which contains the current CA private key.
     */
    public Map<String, String> caKeyData() {
        return caKeyData;
    }

    /**
     * Gets the current CA certificate as bytes.
     *
     * @return The current CA certificate as bytes.
     */
    public byte[] currentCaCertBytes() {
        return Util.decodeBytesFromBase64(caCertData.get(CA_CRT));
    }

    /**
     * Gets the base64 encoded bytes of the current CA certificate.
     *
     * @return The base64 encoded bytes of the current CA certificate.
     */
    public String currentCaCertBase64() {
        return caCertData.get(CA_CRT);
    }

    /**
     * Gets the current CA certificate as an X509Certificate.
     *
     * @return The current CA certificate as an X509Certificate.
     */
    public X509Certificate currentCaCertX509() {
        if (caCertData.get(CA_CRT) != null) {
            try {
                return CertificateUtils.x509Certificate(currentCaCertBytes());
            } catch (CertificateException e) {
                throw new RuntimeException("Failed to decode "  + CA_CRT + " in Secret for " + caRole.caName(), e);
            }
        } else {
            return null;
        }
    }

    /**
     * Returns the certificates that a client authenticating against the CA should trust. When a chain of multiple CAs
     * is used, only the last certificate from the chain should be included.
     *
     * @return  Certificates that clients authenticating against this CA should trust
     */
    public String trustedCaCerts() {
        return caCertData.entrySet().stream()
                .filter(e -> e.getKey().endsWith(SecretEntry.CRT.suffix) && e.getValue() != null && !e.getValue().isBlank())
                .map(e -> {
                    try {
                        return CertificateUtils.x509CertificateToPem(CertificateUtils.x509Certificate(Util.decodeBytesFromBase64(e.getValue())));
                    } catch (CertificateException ex) {
                        throw new RuntimeException("Failed to decode " + e.getKey() + " in Secret for " + caRole.caName(), ex);
                    }
                })
                .collect(Collectors.joining(System.lineSeparator()));
    }

    /**
     * Gets the current CA key as bytes.
     *
     * @return The current CA key as bytes.
     */
    public byte[] currentCaKey() {
        return Util.decodeBytesFromBase64(caKeyData.get(CA_KEY));
    }

    /**
     * True if the expired certificates being removed from the CA {@code Secret}.
     * @return Whether any expired certificates were removed.
     */
    public boolean certsRemoved() {
        return this.caCertsRemoved;
    }

    /**
     * True if renewed CA certificate.
     * @return Whether the certificate was renewed.
     */
    public boolean certRenewed() {
        return renewalType.equals(RenewalType.RENEW_CERT) || renewalType.equals(RenewalType.REPLACE_KEY);
    }

    /**
     * True if replaced CA key.
     * @return Whether the key was replaced.
     */
    public boolean keyReplaced() {
        return renewalType.equals(RenewalType.REPLACE_KEY);
    }

    /**
     * Checks if the key was newly created.
     *
     * @return  Returns true if the key was newly created
     */
    public boolean keyCreated() {
        return renewalType.equals(RenewalType.CREATE);
    }

    /**
     * Gets the generation of the current CA certificate.
     *
     * @return the generation of the current CA certificate
     */
    public int caCertGeneration() {
        return caCertGeneration;
    }

    /**
     * Gets the generation of the current CA key.
     *
     * @return the generation of the current CA key
     */
    public int caKeyGeneration() {
        return caKeyGeneration;
    }


    /**
     * Generates or reuses a server certificate signed by this Cluster CA.
     * Used for Kafka brokers and Cruise Control.
     *
     * @param reconciliation                        Reconciliation marker
     * @param commonName                            Common Name for the certificate
     * @param subject                               Subject for the certificate
     * @param existingCertAndKey                    Existing certificate (or null if none exists)
     * @param isMaintenanceTimeWindowsSatisfied     Whether we are in a maintenance window
     * @param includeCaChain                        Whether to include CA chain
     *
     *
     * @return CertAndKey object containing the public and private key
     **/
    public abstract CompletionStage<CertAndKey> maybeCopyOrGenerateServerCerts(
            Reconciliation reconciliation,
            String commonName,
            Subject subject,
            CertAndKey existingCertAndKey,
            boolean isMaintenanceTimeWindowsSatisfied,
            boolean includeCaChain
    );

    /**
     * Generates or reuses a client certificate signed by this Cluster CA.
     * Used for components that only act as clients, like Entity Operators and Kafka Exporter.
     *
     * @param reconciliation                        Reconciliation marker
     * @param commonName                            Common Name for the certificate
     * @param existingCertAndKey                    Existing certificate (or null if none exists)
     * @param isMaintenanceTimeWindowsSatisfied     Whether we are in a maintenance window
     *
     * @return CertAndKey object containing the certificate and key with CA generation set
     */
    public abstract CompletionStage<CertAndKey> maybeCopyOrGenerateClientCert(
            Reconciliation reconciliation,
            String commonName,
            CertAndKey existingCertAndKey,
            boolean isMaintenanceTimeWindowsSatisfied
    );

    /**
     * Remove certificates from the CA related Secret and store which match the provided predicate
     *
     * @param newData data section of the CA Secret containing certificates
     * @param predicate predicate to match for removing a certificate
     * @return boolean indicating whether any certs were removed
     */

    protected abstract boolean removeCerts(Map<String, String> newData, Predicate<Map.Entry<String, String>> predicate);

    /**
     * Gets the name of the annotation bringing the generation of the specific CA certificate type.
     *
     * @return the name of the annotation bringing the generation of the specific CA certificate type (cluster or clients)
     *         on the Secrets containing certificates signed by that CA (i.e. Kafka brokers, ...)
     */
    public String caCertGenerationAnnotation() {
        if (caRole.equals(CaRole.CLIENTS_CA)) {
            return  ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION;
        } else {
            return  ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION;
        }
    }

    /**
     * It checks if the current (cluster or clients) CA certificate generation is changed compared to the one
     * brought by the corresponding annotation on the provided Resource (i.e. Secret containing Kafka broker certificates, Kafka Pods presenting certificates...)
     *
     * @param resource Resource (Secret or Pod) containing or presenting certificates signed by the current (clients or cluster) CA
     * @return if the current (cluster or clients) CA certificate generation is changed compared to the one
     *         brought by the corresponding annotation on the provided Resource
     */
    public boolean hasCaCertGenerationChanged(HasMetadata resource) {
        if (resource != null && Annotations.hasAnnotation(resource, caCertGenerationAnnotation())) {
            int caCertGenerationAnno = Annotations.intAnnotation(resource, caCertGenerationAnnotation(), INIT_GENERATION);
            LOGGER.debugOp("{} {}/{} generation anno = {}, current CA generation = {}", resource.getKind(),
                    resource.getMetadata().getNamespace(), resource.getMetadata().getName(), caCertGenerationAnno, caCertGeneration);
            return caCertGenerationAnno != caCertGeneration;
        }
        return false;
    }

    /**
     * Generates the expiration date as epoch of the CA certificate.
     * @return  Epoch representation of the expiration date of the certificate
     * @throws  RuntimeException if the certificate cannot be decoded or the cert does not exist
     */
    public long getCertificateExpirationDateEpoch() {
        var cert = currentCaCertX509();
        if (cert == null) {
            throw new RuntimeException(CA_CRT + " does not exist in the secret for " + caRole.caName());
        }
        return cert.getNotAfter().getTime();
    }

    /**
     * Remove old certificates that are stored in the CA Secret.
     */
    public abstract void maybeDeleteOldCerts();

    /**
     * Remove old certificates that are stored in the CA Secret matching the "ca-YYYY-MM-DDTHH-MM-SSZ.crt" naming pattern.
     * NOTE: mostly used when a CA certificate is renewed by replacing the key
     */
    public void deleteOldCerts() {
        if (removeCerts(caCertData(), entry -> OLD_CA_CERT_PATTERN.matcher(entry.getKey()).matches())) {
            LOGGER.infoCr(reconciliation, "{}: Old CA certificates removed", caRole.caCommonName());
            this.caCertsRemoved = true;
        }
    }
}
