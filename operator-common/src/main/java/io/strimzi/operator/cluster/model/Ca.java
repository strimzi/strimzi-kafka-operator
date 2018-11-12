/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.SecretCertProvider;
import io.strimzi.certs.Subject;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

/**
 * A Certificate Authority which can renew its own (self-signed) certificates, and generate signed certificates
 */
public abstract class Ca {

    protected static final Logger log = LogManager.getLogger(Ca.class);

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
    public static final String CA_KEY = "ca.key";
    public static final String CA_CRT = "ca.crt";
    public static final String IO_STRIMZI = "io.strimzi";
    public static final String ANNO_STRIMZI_IO_FORCE_RENEW = "strimzi.io/force-renew";
    public static final String ANNO_STRIMZI_IO_CA_CERT_GENERATION = "strimzi.io/ca-cert-generation";
    private static final int INIT_GENERATION = 0;

    /**
     * Set the {@code strimzi.io/force-renew} annotation on the given {@code caCert} if the given {@code caKey} has
     * the given {@code key}.
     *
     * This is used to force certificate renewal when upgrading from a Strimzi 0.6.0 Secret.
     */
    protected static Secret forceRenewal(Secret caCert, Secret caKey, String key) {
        if (caCert != null && caKey != null && caKey.getData() != null && caKey.getData().containsKey(key)) {
            caCert = new SecretBuilder(caCert).editMetadata().addToAnnotations(ANNO_STRIMZI_IO_FORCE_RENEW, "true").endMetadata().build();
        }
        return caCert;
    }

    protected final String commonName;
    protected final CertManager certManager;
    protected final int validityDays;
    protected final int renewalDays;
    private final boolean generateCa;
    protected String caCertSecretName;
    private Secret caCertSecret;
    protected String caKeySecretName;
    private Secret caKeySecret;
    private boolean caRenewed;
    private boolean certsRemoved;

    public Ca(CertManager certManager, String commonName,
              String caCertSecretName, Secret caCertSecret,
              String caKeySecretName, Secret caKeySecret,
              int validityDays, int renewalDays, boolean generateCa) {
        this.commonName = commonName;
        this.caCertSecret = caCertSecret;
        this.caCertSecretName = caCertSecretName;
        this.caKeySecret = caKeySecret;
        this.caKeySecretName = caKeySecretName;
        this.certManager = certManager;
        this.validityDays = validityDays;
        this.renewalDays = renewalDays;
        this.generateCa = generateCa;
    }

    private static void delete(File brokerCsrFile) {
        if (!brokerCsrFile.delete()) {
            log.warn("{} cannot be deleted", brokerCsrFile.getName());
        }
    }

    private static CertAndKey asCertAndKey(Secret secret, String key, String cert) {
        Base64.Decoder decoder = Base64.getDecoder();
        return secret == null ? null : new CertAndKey(
                decoder.decode(secret.getData().get(key)),
                decoder.decode(secret.getData().get(cert)));
    }

    private CertAndKey generateSignedCert(Subject subject,
                                            File csrFile, File keyFile, File certFile) throws IOException {
        log.debug("Generating certificate {} with SAN {}, signed by CA {}", subject, subject.subjectAltNames(), this);

        certManager.generateCsr(keyFile, csrFile, subject);
        certManager.generateCert(csrFile, currentCaKey(), currentCaCertBytes(),
                certFile, subject, validityDays);

        return new CertAndKey(Files.readAllBytes(keyFile.toPath()), Files.readAllBytes(certFile.toPath()));
    }

    /**
     * Generates a certificate signed by this CA
     */
    public CertAndKey generateSignedCert(String commonName) throws IOException {
        return generateSignedCert(commonName, null);
    }

    /**
     * Generates a certificate signed by this CA
     */
    public CertAndKey generateSignedCert(String commonName, String organization) throws IOException {
        File csrFile = File.createTempFile("tls", "csr");
        File keyFile = File.createTempFile("tls", "key");
        File certFile = File.createTempFile("tls", "cert");

        Subject subject = new Subject();

        if (organization != null) {
            subject.setOrganizationName(organization);
        }

        subject.setCommonName(commonName);

        CertAndKey result = generateSignedCert(subject,
                csrFile, keyFile, certFile);

        delete(csrFile);
        delete(keyFile);
        delete(certFile);
        return result;
    }

    /**
     * Copy already existing certificates from provided Secret based on number of effective replicas
     * and maybe generate new ones for new replicas (i.e. scale-up).
     */
    protected Map<String, CertAndKey> maybeCopyOrGenerateCerts(
           int replicas,
           Function<Integer, Subject> subjectFn,
           Secret secret,
           Function<Integer, String> podNameFn) throws IOException {
        int replicasInSecret = secret == null || this.certRenewed() ? 0 : secret.getData().size() / 2;

        File brokerCsrFile = File.createTempFile("tls", "broker-csr");
        File brokerKeyFile = File.createTempFile("tls", "broker-key");
        File brokerCertFile = File.createTempFile("tls", "broker-cert");

        Map<String, CertAndKey> certs = new HashMap<>();
        // copying the minimum number of certificates already existing in the secret
        // scale up -> it will copy all certificates
        // scale down -> it will copy just the requested number of replicas
        for (int i = 0; i < Math.min(replicasInSecret, replicas); i++) {
            String podName = podNameFn.apply(i);
            log.debug("Certificate for {} already exists", podName);

            Subject subject = subjectFn.apply(i);
            Collection<String> desiredSbjAltNames = subject.subjectAltNames().values();
            Collection<String> currentSbjAltNames = getSubjectAltNames(asCertAndKey(secret, podName + ".key", podName + ".crt").cert());

            if (currentSbjAltNames != null && desiredSbjAltNames.containsAll(currentSbjAltNames) && currentSbjAltNames.containsAll(desiredSbjAltNames))   {
                log.trace("Alternate subjects match. No need to refresh cert for pod {}.", podName);

                certs.put(
                        podName,
                        asCertAndKey(secret, podName + ".key", podName + ".crt"));
            } else {
                if (log.isTraceEnabled()) {
                    if (currentSbjAltNames != null) {
                        log.trace("Current alternate subjects for pod {}: {}", podName, String.join(", ", currentSbjAltNames));
                    } else {
                        log.trace("Current certificate for pod {} has no alternate subjects", podName);
                    }
                    log.trace("Desired alternate subjects for pod {}: {}", podName, String.join(", ", desiredSbjAltNames));
                }

                log.debug("Alternate subjects do not match. Certificate needs to be refreshed for pod {}.", podName);

                CertAndKey k = generateSignedCert(subject,
                        brokerCsrFile, brokerKeyFile, brokerCertFile);
                certs.put(podName, k);
            }
        }

        // generate the missing number of certificates
        // scale up -> generate new certificates for added replicas
        // scale down -> does nothing
        for (int i = replicasInSecret; i < replicas; i++) {
            String podName = podNameFn.apply(i);
            log.debug("Certificate for {} to generate", podName);
            CertAndKey k = generateSignedCert(subjectFn.apply(i),
                    brokerCsrFile, brokerKeyFile, brokerCertFile);
            certs.put(podName, k);
        }
        delete(brokerCsrFile);
        delete(brokerKeyFile);
        delete(brokerCertFile);

        return certs;
    }

    /**
     * Extracts the alternate subject names out of existing certificate
     *
     * @param certificate Existing X509 certificate as a byte array
     * @return
     */
    protected List<String> getSubjectAltNames(byte[] certificate) {
        List<String> subjectAltNames = null;

        try {
            X509Certificate cert = x509Certificate("Parsing certificate SANs", certificate);
            Collection<List<?>> altNames = cert.getSubjectAlternativeNames();
            subjectAltNames = altNames.stream()
                    .filter(name -> name.get(1) instanceof String)
                    .map(item -> (String) item.get(1))
                    .collect(Collectors.toList());
        } catch (CertificateException | RuntimeException e) {
            // TODO: We should mock the certificates properly so that this doesn't fail in tests (not now => long term :-o)
            log.debug("Failed to parse existing certificate", e);
        }

        return subjectAltNames;
    }

    /**
     * Replaces the CA secret if it is within the renewal period.
     * After calling this method {@link #certRenewed()} and {@link #certsRemoved()}
     * will return whether the certificate was renewed and whether expired secrets were removed from the Secret.
     */
    public void createOrRenew(String namespace, String clusterName, Map<String, String> labels, OwnerReference ownerRef) {
        X509Certificate currentCert = cert(caCertSecret, CA_CRT);
        Map<String, String> certData;
        Map<String, String> keyData;
        if (!generateCa) {
            certData = caCertSecret != null ? caCertSecret.getData() : emptyMap();
            keyData = caKeySecret != null ? singletonMap(CA_KEY, caKeySecret.getData().get(CA_KEY)) : emptyMap();
            certsRemoved = false;
        } else {
            boolean shouldCreateOrRenew = shouldCreateOrRenew(currentCert, namespace, clusterName);
            if (shouldCreateOrRenew) {
                try {
                    Map<String, String>[] newData = createOrRenewCert(currentCert);
                    certData = newData[0];
                    keyData = newData[1];
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                certData = caCertSecret.getData();
                keyData = caKeySecret.getData();
            }
            this.certsRemoved = removeExpiredCerts(certData) > 0;
        }
        SecretCertProvider secretCertProvider = new SecretCertProvider();

        if (certsRemoved) {
            log.info("{}: Expired CA certificates removed", this);
        }
        if (caRenewed) {
            log.info("{}: Certificates renewed", this);
        }

        // cluster CA certificate generation annotation handling
        int caCertGeneration = INIT_GENERATION;
        if (caCertSecret != null && caCertSecret.getData().get(CA_CRT) != null) {
            String caCertGenerationAnnotation = caCertSecret.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_CA_CERT_GENERATION);
            if (caCertGenerationAnnotation != null) {
                caCertGeneration = Integer.parseInt(caCertGenerationAnnotation);
                if (caRenewed) {
                    caCertGeneration++;
                }
            }
        }

        // TODO: handling cluster CA key generation annotation

        caCertSecret = secretCertProvider.createSecret(namespace, caCertSecretName, certData, labels,
                Collections.singletonMap(ANNO_STRIMZI_IO_CA_CERT_GENERATION, String.valueOf(caCertGeneration)), ownerRef);
        caKeySecret = secretCertProvider.createSecret(namespace, caKeySecretName, keyData, labels,
                Collections.emptyMap(), ownerRef);
    }

    private boolean shouldCreateOrRenew(X509Certificate currentCert, String namespace, String clusterName) {
        String reason = null;
        boolean result = false;
        if (this.caCertSecret == null
                || this.caCertSecret.getData().get(CA_CRT) == null) {
            reason = "CA certificate secret " + caCertSecretName + " is missing or lacking the key " + CA_CRT;
            result = true;
            this.caRenewed = this.caCertSecret != null;
        } else if (caKeySecret == null
                || caKeySecret.getData().get(CA_KEY) == null) {
            reason = "CA key secret " + caKeySecretName + " is missing or lacking the key " + CA_KEY;
            result = true;
            this.caRenewed = caKeySecret != null;
        } else if (this.caCertSecret.getMetadata() != null
                && this.caCertSecret.getMetadata().getAnnotations() != null
                && "true".equals(this.caCertSecret.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_FORCE_RENEW))) {
            reason = "CA certificate secret " + caCertSecretName + " is annotated with " + ANNO_STRIMZI_IO_FORCE_RENEW;
            result = true;
            this.caRenewed = true;
        } else if (currentCert != null && certNeedsRenewal(currentCert)) {
            reason = "Within renewal period for CA certificate (expires on " + currentCert.getNotAfter() + ")";
            result = true;
            this.caRenewed = true;
        }

        if (this.caRenewed) {
            log.log(!generateCa ? Level.WARN : Level.INFO,
                    "{}: CA certificate in secret {} needs to be renewed: {}", this, caCertSecretName, reason);
        } else {
            log.debug("{}: The CA certificate in secret {} already exists and does not need renewing", this, caCertSecretName);
        }
        if (!generateCa) {
            if (caRenewed) {
                log.warn("The {} certificate in Secret {} in namespace {} needs to be renewed " +
                                "and it is not configured to automatically renew. This needs to be manually updated before that date. " +
                                "Alternatively, configure Kafka.spec.tlsCertificates.generateCertificateAuthority=true in the Kafka resource with name {} in namespace {}.",
                        CA_CRT, this.caCertSecretName, namespace, currentCert.getNotAfter());
            } else if (caCertSecret == null) {
                log.warn("The certificate (data.{}) in Secret {} and the private key (data.{}) in Secret {} in namespace {} " +
                                "needs to be configured with a Base64 encoded PEM-format certificate. " +
                                "Alternatively, configure Kafka.spec.tlsCertificates.generateCertificateAuthority=true in the Kafka resource with name {} in namespace {}.",
                        CA_CRT, this.caCertSecretName,
                        CA_KEY, this.caKeySecretName, namespace,
                        clusterName, namespace);
            }
        }
        return result;
    }

    /**
     * Gets the CA cert secret, which contains both the current CA cert and also previous, still valid certs.
     */
    public Secret caCertSecret() {
        return caCertSecret;
    }

    /**
     * Gets the CA key secret, which contains the current CA private key.
     */
    public Secret caKeySecret() {
        return caKeySecret;
    }

    public byte[] currentCaCertBytes() {
        Base64.Decoder decoder = Base64.getDecoder();
        return decoder.decode(caCertSecret().getData().get(CA_CRT));
    }

    public String currentCaCertBase64() {
        return caCertSecret().getData().get(CA_CRT);
    }

    public byte[] currentCaKey() {
        Base64.Decoder decoder = Base64.getDecoder();
        return decoder.decode(caKeySecret().getData().get(CA_KEY));
    }

    /**
     * True if the last call to {@link #createOrRenew(String, String, Map, OwnerReference)}
     * resulted in expired certificates being removed from the CA Secret.
     */
    public boolean certsRemoved() {
        return this.certsRemoved;
    }

    /**
     * True if the last call to {@link #createOrRenew(String, String, Map, OwnerReference)}
     * resulted in a renewed CA certificate.
     */
    public boolean certRenewed() {
        return this.caRenewed;
    }

    private int removeExpiredCerts(Map<String, String> newData) {
        int removed = 0;
        Iterator<String> iter = newData.keySet().iterator();
        while (iter.hasNext()) {
            Pattern pattern = Pattern.compile("ca-" + "([0-9T:-]{19}).(crt|key)");
            String key = iter.next();
            Matcher matcher = pattern.matcher(key);

            if (matcher.matches()) {
                String date = matcher.group(1) + "Z";
                Instant parse = DATE_TIME_FORMATTER.parse(date, Instant::from);
                if (parse.isBefore(Instant.now())) {
                    log.debug("Removing {} from Secret because it has expired", key);
                    iter.remove();
                    removed++;
                }
            }
        }
        return removed;
    }

    private Map<String, String>[] createOrRenewCert(X509Certificate currentCert) throws IOException {
        log.debug("Generating new certificate {} to be stored in {}", CA_CRT, caCertSecretName);
        CertAndKey ca = currentCert == null ? generateCa(commonName) : renewCa(commonName);
        Map<String, String> certData = new HashMap<>();
        Map<String, String> keyData = new HashMap<>();
        // Add the generated certificate as the current certificate
        certData.put(CA_CRT, ca.certAsBase64String());
        keyData.put(CA_KEY, ca.keyAsBase64String());

        log.debug("End generating certificate {} to be stored in {}", CA_CRT, caCertSecretName);
        return new Map[]{certData, keyData};
    }

    private boolean certNeedsRenewal(X509Certificate cert)  {
        Date notAfter = cert.getNotAfter();
        log.trace("Certificate {} expires on {}", cert.getSubjectDN(), notAfter);
        long msTillExpired = notAfter.getTime() - System.currentTimeMillis();
        return msTillExpired < renewalDays * 24L * 60L * 60L * 1000L;
    }

    static X509Certificate cert(Secret secret, String key)  {
        if (secret == null || secret.getData() == null || secret.getData().get(key) == null) {
            return null;
        }
        Base64.Decoder decoder = Base64.getDecoder();
        byte[] bytes = decoder.decode(secret.getData().get(key));
        return x509Certificate("Certificate in key " + key + " of Secret " + secret.getMetadata().getName(), bytes);
    }

    static X509Certificate x509Certificate(String logCtx, byte[] bytes) {
        CertificateFactory factory = certificateFactory();
        return x509Certificate(logCtx, factory, bytes);
    }

    static X509Certificate x509Certificate(String logCtx, CertificateFactory factory, byte[] bytes) {
        try {
            Certificate certificate = factory.generateCertificate(new ByteArrayInputStream(bytes));
            if (certificate instanceof X509Certificate) {
                return (X509Certificate) certificate;
            } else {
                throw new RuntimeException(logCtx + " is not an X509Certificate");
            }
        } catch (CertificateException e) {
            throw new RuntimeException(logCtx + " could not be parsed");
        }
    }

    static CertificateFactory certificateFactory() {
        CertificateFactory factory = null;
        try {
            factory = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            throw new RuntimeException("No security provider with support for X.509 certificates", e);
        }
        return factory;
    }

    private CertAndKey generateCa(String commonName) throws IOException {
        log.debug("Generating CA with cn={}, org={}", commonName, IO_STRIMZI);
        File keyFile = File.createTempFile("tls", commonName + "-key");
        try {
            File certFile = File.createTempFile("tls", commonName + "-cert");
            try {

                Subject sbj = new Subject();
                sbj.setOrganizationName(IO_STRIMZI);
                sbj.setCommonName(commonName);

                certManager.generateSelfSignedCert(keyFile, certFile, sbj, validityDays);
                return new CertAndKey(Files.readAllBytes(keyFile.toPath()), Files.readAllBytes(certFile.toPath()));
            } finally {
                delete(certFile);
            }
        } finally {
            delete(keyFile);
        }
    }

    private CertAndKey renewCa(String commonName) throws IOException {
        log.debug("Renewing CA with cn={}, org={}", commonName, IO_STRIMZI);

        Base64.Decoder decoder = Base64.getDecoder();
        byte[] bytes = decoder.decode(caKeySecret.getData().get(CA_KEY));
        File keyFile = File.createTempFile("ererver", "dffbvd");
        try {
            Files.write(keyFile.toPath(), bytes);
            File certFile = File.createTempFile("tls", commonName + "-cert");
            try {

                Subject sbj = new Subject();
                sbj.setOrganizationName(IO_STRIMZI);
                sbj.setCommonName(commonName);

                certManager.renewSelfSignedCert(keyFile, certFile, sbj, validityDays);
                return new CertAndKey(bytes, Files.readAllBytes(certFile.toPath()));
            } finally {
                delete(certFile);
            }
        } finally {
            delete(keyFile);
        }
    }
}
