/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.SecretCertProvider;
import io.strimzi.certs.Subject;
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
import java.time.ZoneId;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    protected final String commonName;
    protected final CertManager certManager;
    protected final int validityDays;
    protected final int renewalDays;
    private final boolean generateCa;
    protected String caCertSecretName;
    private Secret caCertSecret;
    protected String caKeySecretName;
    private Secret caKeySecret;
    private boolean needsRenewal;
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

        Map<String, CertAndKey> certs = new HashMap<>();
        // copying the minimum number of certificates already existing in the secret
        // scale up -> it will copy all certificates
        // scale down -> it will copy just the requested number of replicas
        for (int i = 0; i < Math.min(replicasInSecret, replicas); i++) {

            String podName = podNameFn.apply(i);
            log.debug("Certificate for {} already exists", podName);
            certs.put(
                    podName,
                    asCertAndKey(secret, podName + ".key", podName + ".crt"));
        }

        File brokerCsrFile = File.createTempFile("tls", "broker-csr");
        File brokerKeyFile = File.createTempFile("tls", "broker-key");
        File brokerCertFile = File.createTempFile("tls", "broker-cert");

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
     * Replaces the CA secret if it is within the renewal period.
     * After calling this method {@link #certRenewed()} and {@link #certsRemoved()}
     * will return whether the certificate was renewed and whether expired secrets were removed from the Secret.
     */
    public void createOrRenew(String namespace, String clusterName, Map<String, String> labels, OwnerReference ownerRef) {
        X509Certificate currentCert = cert(caCertSecret, CA_CRT);
        this.needsRenewal = currentCert != null && certNeedsRenewal(currentCert);
        if (needsRenewal) {
            log.debug("{}: CA certificate in secret {} needs to be renewed", this, caCertSecretName);
        }
        Map<String, String> certData;
        Map<String, String> keyData;
        if (!generateCa) {
            certData = checkProvidedCert(namespace, clusterName, caCertSecret, currentCert);
            keyData = caKeySecret != null ? singletonMap(CA_KEY, caKeySecret.getData().get(CA_KEY)) : emptyMap();
            certsRemoved = false;
        } else {
            if (caCertSecret == null
                    || caCertSecret.getData().get(CA_CRT) == null
                    || caKeySecret == null
                    || caKeySecret.getData().get(CA_KEY) == null
                    || needsRenewal) {
                try {
                    Map<String, String>[] newData = createOrRenewCert(currentCert);
                    certData = newData[0];
                    keyData = newData[1];
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                log.debug("{}: The CA certificate in secret {} already exists and does not need renewing", this, caCertSecretName);
                certData = caCertSecret.getData();
                keyData = caKeySecret.getData();
            }
            this.certsRemoved = removeExpiredCerts(certData) > 0;
        }
        SecretCertProvider secretCertProvider = new SecretCertProvider();

        if (certsRemoved) {
            log.info("{}: Expired CA certificates removed", this);
        }
        if (needsRenewal) {
            log.info("{}: Certificates renewed", this);
        }

        caCertSecret = secretCertProvider.createSecret(namespace, caCertSecretName, certData, labels, ownerRef);
        caKeySecret = secretCertProvider.createSecret(namespace, caKeySecretName, keyData, labels, ownerRef);
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
        return this.needsRenewal;
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
        CertAndKey ca = generateCa(commonName);
        Map<String, String> certData = new HashMap<>();
        Map<String, String> keyData = new HashMap<>();
        if (currentCert != null) {
            // Add the current certificate as an old certificate (with date in UTC)
            String notAfterDate = DATE_TIME_FORMATTER.format(currentCert.getNotAfter().toInstant().atZone(ZoneId.of("Z")));
            // TODO factor out this naming scheme
            certData.put("ca-" + notAfterDate + ".crt", caCertSecret.getData().get(CA_CRT));
        }
        // Add the generated certificate as the current certificate
        certData.put(CA_CRT, ca.certAsBase64String());
        keyData.put(CA_KEY, ca.keyAsBase64String());

        log.debug("End generating certificate {} to be stored in {}", CA_CRT, caCertSecretName);
        return new Map[]{certData, keyData};
    }

    private Map<String, String> checkProvidedCert(String namespace, String clusterName,
                                                  Secret clusterCa, X509Certificate currentCert) {
        Map<String, String> newData;
        if (needsRenewal) {
            log.warn("The {} certificate in Secret {} in namespace {} will expire on {} " +
                            "and it is not configured to automatically renew. This needs to be manually updated before that date. " +
                            "Alternatively, configure Kafka.spec.tlsCertificates.generateCertificateAuthority=true in the Kafka resource with name {} in namespace {}.",
                    CA_CRT, caCertSecretName, namespace, currentCert.getNotAfter());
        } else if (clusterCa == null) {
            log.warn("The certificate (data.{}) in Secret {} and the private key (data.{}) in Secret {} in namespace {} " +
                            "needs to be configured with a Base64 encoded PEM-format certificate. " +
                            "Alternatively, configure Kafka.spec.tlsCertificates.generateCertificateAuthority=true in the Kafka resource with name {} in namespace {}.",
                    CA_CRT, this.caCertSecretName,
                    CA_KEY, this.caKeySecretName, namespace,
                    clusterName, namespace);
        }
        newData = clusterCa != null ? clusterCa.getData() : emptyMap();
        return newData;
    }

    private boolean certNeedsRenewal(X509Certificate cert)  {
        Date notAfter = cert.getNotAfter();
        log.trace("Certificate {} expires on {}", cert.getSubjectDN(), notAfter);
        long msTillExpired = notAfter.getTime() - System.currentTimeMillis();
        return msTillExpired < renewalDays * 24L * 60L * 60L * 1000L;
    }

    static X509Certificate cert(Secret secret, String key)  {
        if (secret == null || secret.getData().get(key) == null) {
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
}
