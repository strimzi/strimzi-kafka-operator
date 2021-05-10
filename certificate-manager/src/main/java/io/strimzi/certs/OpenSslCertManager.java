/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An OpenSSL based certificate manager.
 * @see "Chapter 11 of 'Bulletproof SSL and TLS' by Ivan Ristic."
 * @see "The man page for <code>config(5)</code>."
 * @see "The man page for <code>openssl-ca(1)</code>."
 * @see "The man page for <code>openssl-req(1)</code>."
 */
public class OpenSslCertManager implements CertManager {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4)
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .appendOffsetId().toFormatter();
    public static final int MAXIMUM_CN_LENGTH = 64;

    private static final Logger LOGGER = LogManager.getLogger(OpenSslCertManager.class);
    public static final ZoneId UTC = ZoneId.of("UTC");

    public OpenSslCertManager() {}

    void checkValidity(ZonedDateTime notBefore, ZonedDateTime notAfter) {
        Objects.requireNonNull(notBefore);
        Objects.requireNonNull(notAfter);
        if (!notBefore.isBefore(notAfter)) {
            throw new IllegalArgumentException("Invalid notBefore and notAfter: " + notBefore + " must be before " + notAfter);
        }
    }

    static void delete(Path fileOrDir) throws IOException {
        if (fileOrDir != null)
            if (Files.isDirectory(fileOrDir)) {
                Files.walk(fileOrDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                log.debug("File could not be deleted: {}", fileOrDir);
                            }
                        });
            } else {
                if (!Files.deleteIfExists(fileOrDir)) {
                    log.debug("File not deleted, because it did not exist: {}", fileOrDir);
                }
            }
    }

    private Path createDefaultConfig() throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("openssl.conf")) {
            Path openSslConf = Files.createTempFile(null, null);
            Files.copy(is, openSslConf, StandardCopyOption.REPLACE_EXISTING);
            return openSslConf;
        }
    }

    /**
     * Add basic constraints and subject alt names section to the provided openssl configuration file
     *
     * @param sbj subject information
     * @return openssl configuration file with subject alt names added
     * @throws IOException
     */
    private Path buildConfigFile(Subject sbj, boolean isCa) throws IOException {
        Path sna = createDefaultConfig();
        try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(sna.toFile(), true), StandardCharsets.UTF_8))) {
            if (isCa) {
                out.append("basicConstraints = critical,CA:true,pathlen:0\n");
            }
            if (sbj != null) {
                if (sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {
                    out.append("subjectAltName = @alt_names\n" +
                            "\n" +
                            "[alt_names]\n");
                    boolean newline = false;
                    for (Map.Entry<String, String> entry : sbj.subjectAltNames().entrySet()) {
                        if (newline) {
                            out.append("\n");
                        }
                        out.append(entry.getKey()).append(" = ").append(entry.getValue());
                        newline = true;
                    }
                }
            }
        }
        return sna;
    }

    @Override
    public void generateSelfSignedCert(File keyFile, File certFile, Subject sbj, int days) throws IOException {
        Instant now = Instant.now();
        ZonedDateTime notBefore = now.atZone(UTC);
        ZonedDateTime notAfter = now.plus(days, ChronoUnit.DAYS).atZone(UTC);
        generateRootCaCert(keyFile, certFile, sbj, notBefore, notAfter, 0);
    }

    public void generateRootCaCert(File keyFile, File certFile, Subject subject,
                                   ZonedDateTime notBefore, ZonedDateTime notAfter, int pathLength) throws IOException {
        // Preconditions
        Objects.requireNonNull(keyFile);
        Objects.requireNonNull(certFile);
        Objects.requireNonNull(subject);
        checkValidity(notBefore, notAfter);
        if (pathLength < 0) {
            throw new IllegalArgumentException("pathLength cannot be negative: " + pathLength);
        }
        if (subject.subjectAltNames() != null && !subject.subjectAltNames().isEmpty()) {
            throw new IllegalArgumentException("CA certificates should not have Subject Alternative Names");
        }

        // Generate a CSR for the key
        Path tmpKey = null;
        Path defaultConfig = null;
        Path csrFile = null;
        Path newCertsDir = null;
        Path database = null;
        Path attr = null;

        try {
            tmpKey = Files.createTempFile(null, null);
            // Generate a key pair
            new OpensslArgs("openssl", "genrsa")
                    .optArg("-out", tmpKey)
                    .opt("4096")
                    .exec();

            csrFile = Files.createTempFile(null, null);
            new OpensslArgs("openssl", "req")
                    .opt("-new")
                    .optArg("-key", tmpKey)
                    .optArg("-out", csrFile)
                    .optArg("-subj", subject)
                    .exec();

            // Generate a self signed cert for the CA
            database = Files.createTempFile(null, null);
            attr = Files.createFile(new File(database.toString() + ".attr").toPath());
            newCertsDir = Files.createTempDirectory(null);
            defaultConfig = createDefaultConfig();
            new OpensslArgs("openssl", "ca")
                    .opt("-utf8").opt("-batch").opt("-notext")
                    .opt("-selfsign")
                    .optArg("-in", csrFile)
                    .optArg("-out", certFile)
                    .optArg("-keyfile", tmpKey)
                    .optArg("-startdate", notBefore)
                    .optArg("-enddate", notAfter)
                    .optArg("-subj", subject)
                    .optArg("-config", defaultConfig)
                    .database(database, attr)
                    .newCertsDir(newCertsDir)
                    .basicConstraints("critical,CA:true,pathlen:" + pathLength)
                    .keyUsage("critical,keyCertSign,cRLSign")
                    .exec(false);

            // The key will be in pkcs#1 format (bracketed by BEGIN/END RSA PRIVATE KEY)
            // Convert it to pkcs#8 format (bracketed by BEGIN/END PRIVATE KEY)
            new OpensslArgs("openssl", "pkcs8")
                    .opt("-topk8").opt("-nocrypt")
                    .optArg("-in", tmpKey)
                    .optArg("-out", keyFile)
                    .exec();
        } finally {
            delete(tmpKey);
            delete(database);
            delete(attr);
            delete(newCertsDir);
            delete(csrFile);
            delete(defaultConfig);
        }
    }

    public void generateIntermediateCaCert(File issuerCaKeyFile, File issuerCaCertFile,
                                           Subject subject,
                                           File subjectKeyFile, File subjectCertFile,
                                           ZonedDateTime notBefore, ZonedDateTime notAfter, int pathLength) throws IOException {
        // Preconditions
        Objects.requireNonNull(issuerCaKeyFile);
        Objects.requireNonNull(issuerCaCertFile);
        Objects.requireNonNull(subject);
        Objects.requireNonNull(subjectKeyFile);
        Objects.requireNonNull(subjectCertFile);
        checkValidity(notBefore, notAfter);
        if (pathLength < 0) {
            throw new IllegalArgumentException("pathLength cannot be negative: " + pathLength);
        }
        if (subject.subjectAltNames() != null && !subject.subjectAltNames().isEmpty()) {
            throw new IllegalArgumentException("CA certificates should not have Subject Alternative Names");
        }

        // Generate a CSR for the key
        Path tmpKey = null;
        Path defaultConfig = null;
        Path csrFile = null;
        Path newCertsDir = null;
        Path database = null;
        Path attr = null;

        try {
            tmpKey = Files.createTempFile(null, null);
            // Generate a key pair
            new OpensslArgs("openssl", "genrsa")
                    .optArg("-out", tmpKey)
                    .opt("4096")
                    .exec();

            csrFile = Files.createTempFile(null, null);
            defaultConfig = buildConfigFile(subject, true);
            new OpensslArgs("openssl", "req")
                    .opt("-new")
                    .optArg("-config", defaultConfig, true)
                    .optArg("-key", tmpKey)
                    .optArg("-out", csrFile)
                    .optArg("-subj", subject)
                    .exec();

            // Generate a self signed cert for the CA
            database = Files.createTempFile(null, null);
            attr = Files.createFile(new File(database.toString() + ".attr").toPath());
            newCertsDir = Files.createTempDirectory(null);
            defaultConfig = createDefaultConfig();
            new OpensslArgs("openssl", "ca")
                    .opt("-utf8").opt("-batch").opt("-notext")
                    .optArg("-in", csrFile)
                    .optArg("-out", subjectCertFile)
                    .optArg("-startdate", notBefore)
                    .optArg("-enddate", notAfter)
                    .optArg("-subj", subject)
                    .optArg("-config", defaultConfig)
                    .optArg("-cert", issuerCaCertFile)
                    .optArg("-keyfile", issuerCaKeyFile)
                    .database(database, attr)
                    .newCertsDir(newCertsDir)
                    .basicConstraints("critical,CA:true,pathlen:" + pathLength)
                    .keyUsage("critical,keyCertSign,cRLSign")
                    .exec(false);

            // The key will be in pkcs#1 format (bracketed by BEGIN/END RSA PRIVATE KEY)
            // Convert it to pkcs#8 format (bracketed by BEGIN/END PRIVATE KEY)
            new OpensslArgs("openssl", "pkcs8")
                    .opt("-topk8").opt("-nocrypt")
                    .optArg("-in", tmpKey)
                    .optArg("-out", subjectKeyFile)
                    .exec();
        } finally {
            delete(tmpKey);
            delete(database);
            delete(attr);
            delete(newCertsDir);
            delete(csrFile);
            delete(defaultConfig);
        }
    }

    @Override
    public void addCertToTrustStore(File certFile, String certAlias, File trustStoreFile, String trustStorePassword)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        // Preconditions
        Objects.requireNonNull(certFile);
        Objects.requireNonNull(certAlias);
        Objects.requireNonNull(trustStoreFile);
        Objects.requireNonNull(trustStorePassword);

        FileInputStream isTrustStore = null;
        try {
            // check if the truststore file is empty or not, for loading its content eventually
            // the KeyStore class is able to create an empty store if the input stream is null
            if (trustStoreFile.length() > 0) {
                isTrustStore = new FileInputStream(trustStoreFile);
            }

            try (FileInputStream isCertificate = new FileInputStream(certFile)) {

                CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
                X509Certificate certificate = (X509Certificate) certFactory.generateCertificate(isCertificate);

                KeyStore trustStore = KeyStore.getInstance("PKCS12");
                trustStore.load(isTrustStore, trustStorePassword.toCharArray());
                trustStore.setEntry(certAlias, new KeyStore.TrustedCertificateEntry(certificate), null);

                try (FileOutputStream osTrustStore = new FileOutputStream(trustStoreFile)) {
                    trustStore.store(osTrustStore, trustStorePassword.toCharArray());
                }
            }
        } finally {
            if (isTrustStore != null) {
                isTrustStore.close();
            }
        }
    }

    @Override
    public void addKeyAndCertToKeyStore(File keyFile, File certFile, String alias, File keyStoreFile, String keyStorePassword) throws IOException {
        new OpensslArgs("openssl", "pkcs12")
                .opt("-export")
                .optArg("-in", certFile)
                .optArg("-inkey", keyFile)
                .optArg("-name", alias)
                .optArg("-out", keyStoreFile)
                .optArg("-passout", "pass:" + keyStorePassword)
                .exec();
    }

    @Override
    public void deleteFromTrustStore(List<String> aliases, File trustStoreFile, String trustStorePassword)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        // Preconditions
        Objects.requireNonNull(aliases);
        Objects.requireNonNull(trustStoreFile);
        Objects.requireNonNull(trustStorePassword);

        try (FileInputStream isTrustStore = new FileInputStream(trustStoreFile)) {
            KeyStore trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(isTrustStore, trustStorePassword.toCharArray());
            for (String alias : aliases) {
                trustStore.deleteEntry(alias);
            }
            try (FileOutputStream osTrustStore = new FileOutputStream(trustStoreFile)) {
                trustStore.store(osTrustStore, trustStorePassword.toCharArray());
            }
        }
    }

    @Override
    public void renewSelfSignedCert(File keyFile, File certFile, Subject subject, int days) throws IOException {
        // Preconditions
        Objects.requireNonNull(keyFile);
        Objects.requireNonNull(certFile);
        Objects.requireNonNull(subject);
        if (days <= 0) {
            throw new IllegalArgumentException("Invalid validityDays " + days);
        }

        // See https://serverfault.com/a/501513
        Path sna = null;
        Path csrFile = null;
        try {
            csrFile = Files.createTempFile(null, null);

            OpensslArgs args = new OpensslArgs("openssl", "req")
                    .opt("-new")
                    .opt("-batch")
                    .optArg("-out", csrFile)
                    .optArg("-key", keyFile);
            if (subject != null) {
                if (subject.subjectAltNames() != null && subject.subjectAltNames().size() > 0) {
                    sna = buildConfigFile(subject, true);
                    args.optArg("-config", sna, true).optArg("-extensions", "v3_req");
                }
                args.optArg("-subj", subject);
            }

            args.exec();
            delete(sna);

            // subject alt names need to be in an openssl configuration file
            sna = buildConfigFile(subject, true);
            new OpensslArgs("openssl", "x509")
                    .opt("-req")
                    .optArg("-days", String.valueOf(days))
                    .optArg("-in", csrFile)
                    .optArg("-signkey", keyFile)
                    .optArg("-out", certFile)
                    .optArg("-extfile", sna, true)
                    .optArg("-extensions", "v3_req")
                    .exec();
        } finally {
            delete(sna);
            delete(csrFile);
        }
    }

    @Override
    public void generateCsr(File keyFile, File csrFile, Subject subject) throws IOException {
        Objects.requireNonNull(keyFile);
        Objects.requireNonNull(csrFile);
        Objects.requireNonNull(subject);

        OpensslArgs cmd = new OpensslArgs("openssl", "req")
                .opt("-new").opt("-batch").opt("-nodes")
                .optArg("-keyout", keyFile)
                .optArg("-out", csrFile);

        Path sna = null;
        try {
            if (subject != null) {

                if (subject.subjectAltNames() != null && subject.subjectAltNames().size() > 0) {

                    // subject alt names need to be in an openssl configuration file
                    sna = buildConfigFile(subject, false);
                    cmd.optArg("-config", sna, true).optArg("-extensions", "v3_req");
                }

                cmd.optArg("-subj", subject);
            }

            cmd.exec();
        } finally {
            delete(sna);
        }
    }

    @Override
    public void generateCert(File csrFile, File caKey, File caCert, File crtFile, Subject sbj, int days) throws IOException {
        Instant now = Instant.now();
        ZonedDateTime notBefore = now.atZone(UTC);
        ZonedDateTime notAfter = now.plus(days, ChronoUnit.DAYS).atZone(UTC);
        generateCert(csrFile, caKey, caCert, crtFile, sbj, notBefore, notAfter);
    }

    public void generateCert(File csrFile, File caKey, File caCert, File crtFile, Subject sbj, ZonedDateTime notBefore, ZonedDateTime notAfter) throws IOException {
        // Preconditions
        Objects.requireNonNull(csrFile);
        Objects.requireNonNull(caKey);
        Objects.requireNonNull(caCert);
        Objects.requireNonNull(crtFile);
        Objects.requireNonNull(sbj);
        checkValidity(notBefore, notAfter);

        Path defaultConfig = null;
        Path database = null;
        Path attr = null;
        Path newCertsDir = null;
        Path sna = null;
        try {
            defaultConfig = createDefaultConfig();
            database = Files.createTempFile(null, null);
            attr = Files.createFile(new File(database.toString() + ".attr").toPath());
            newCertsDir = Files.createTempDirectory(null);
            OpensslArgs cmd = new OpensslArgs("openssl", "ca")
                    .opt("-utf8").opt("-batch").opt("-notext")
                    .optArg("-in", csrFile)
                    .optArg("-out", crtFile)
                    .optArg("-cert", caCert)
                    .optArg("-keyfile", caKey)
                    .optArg("-startdate", notBefore)
                    .optArg("-enddate", notAfter)
                    .optArg("-config", defaultConfig, true);

            if (sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {
                cmd.optArg("-extensions", "v3_req");
                // subject alt names need to be in an openssl configuration file
                sna = buildConfigFile(sbj, false);
                cmd.optArg("-extfile", sna, true);
            }

            cmd.database(database, attr).newCertsDir(newCertsDir);
            cmd.exec(false);
        } finally {
            delete(database);
            delete(attr);
            delete(newCertsDir);
            delete(defaultConfig);
            delete(sna);
        }

        // We need to remove CA serial file
        Path path = Paths.get(caCert.getPath().replaceAll(".[a-zA-Z0-9]+$", ".srl"));
        delete(path);
    }


    @Override
    public void generateCert(File csrFile, byte[] caKey, byte[] caCert, File crtFile, Subject sbj, int days) throws IOException {
        Path caKeyFile = null;
        Path caCertFile = null;
        try {
            caKeyFile = Files.write(Files.createTempFile(null, null), caKey);
            caCertFile = Files.write(Files.createTempFile(null, null), caCert);
            generateCert(csrFile, caKeyFile.toFile(), caCertFile.toFile(), crtFile, sbj, days);
        } finally {
            delete(caKeyFile);
            delete(caCertFile);
        }
    }

    /**
     * Helper for building arg lists and environments.
     * The environment is used so that the config file can be parameterised for things like basic constraints.
     * But it's still necessary to use dynamically generated configs for specifying SANs
     * (see {@link OpenSslCertManager#buildConfigFile(Subject, boolean)}).
     */
    private static class OpensslArgs {
        ProcessBuilder pb = new ProcessBuilder();
        public OpensslArgs(String binary, String command) {
            pb.command().add(binary);
            pb.command().add(command);
        }
        public OpensslArgs optArg(String opt, File file) throws IOException {
            return optArg(opt, file, false);
        }
        public OpensslArgs optArg(String opt, File file, boolean mayLog) throws IOException {
            if (mayLog && log.isTraceEnabled()) {
                log.trace("Contents of {} for option {} is:\n{}", file, opt, Files.readString(file.toPath()));
            }
            opt(opt);
            pb.command().add(file.getAbsolutePath());
            return this;
        }
        public OpensslArgs optArg(String opt, Path file) throws IOException {
            return optArg(opt, file.toFile(), false);
        }
        public OpensslArgs optArg(String opt, Path file, boolean mayLog) throws IOException {
            return optArg(opt, file.toFile(), mayLog);
        }
        public OpensslArgs optArg(String opt, ZonedDateTime dateTime) {
            opt(opt);
            pb.command().add(DATE_TIME_FORMATTER.format(dateTime));
            return this;
        }
        public OpensslArgs opt(String option) {
            pb.command().add(option);
            return this;
        }

        public OpensslArgs optArg(String opt, Subject subject) {
            opt(opt);
            pb.command().add(subject.toString());
            return this;
        }

        public OpensslArgs optArg(String opt, String s) {
            opt(opt);
            pb.command().add(s);
            return this;
        }

        public OpensslArgs basicConstraints(String basicConstraints) {
            pb.environment().put("STRIMZI_basicConstraints", basicConstraints);
            return this;
        }
        public OpensslArgs keyUsage(String keyUsage) {
            pb.environment().put("STRIMZI_keyUsage", keyUsage);
            return this;
        }
        public OpensslArgs database(Path database, Path attr) throws IOException {
            // Some versions of openssl require the presence of a index.txt.attr file
            // https://serverfault.com/questions/857131/odd-error-while-using-openssl
            Files.writeString(attr, "unique_subject = no\n");
            pb.environment().put("STRIMZI_database", database != null ? database.toFile().getAbsolutePath() : "STRIMZI_database");
            return this;
        }
        public OpensslArgs newCertsDir(Path newCertsDir) {
            pb.environment().put("STRIMZI_new_certs_dir", newCertsDir != null ? newCertsDir.toFile().getAbsolutePath() : "STRIMZI_new_certs_dir");
            return this;
        }

        public void exec() throws IOException {
            exec(true);
        }

        public void exec(boolean failOnNonZero) throws IOException {

            if (!pb.environment().containsKey("STRIMZI_basicConstraints")) {
                basicConstraints("critical,CA:false");
            }
            if (!pb.environment().containsKey("STRIMZI_keyUsage")) {
                keyUsage("critical,digitalSignature,keyEncipherment");
            }
            if (!pb.environment().containsKey("STRIMZI_database")) {
                pb.environment().put("STRIMZI_database", "/dev/null");
            }
            if (!pb.environment().containsKey("STRIMZI_new_certs_dir")) {
                pb.environment().put("STRIMZI_new_certs_dir", "/dev/null");
            }

            Path out = null;
            try {
                out = Files.createTempFile(null, null);
                pb.redirectErrorStream(true)
                        .redirectOutput(out.toFile());

                log.debug("Running command {}", pb.command());

                Process proc = pb.start();

                OutputStream outputStream = proc.getOutputStream();
                // close subprocess' stdin
                outputStream.close();

                int result = proc.waitFor();

                if (failOnNonZero && result != 0) {
                    String output = Files.readString(out, Charset.defaultCharset());
                    if (!log.isDebugEnabled()) {
                        // Include the command if we've not logged it already
                        log.error("Got result {} from command {} with output\n{}", result, pb.command(), output);
                    } else {
                        log.error("Got result {} with output\n{}", result, output);
                    }
                    throw new RuntimeException("openssl status code " + result);
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Got output\n{}", Files.readString(out, Charset.defaultCharset()));
                    }
                    log.debug("Got result {}", result);
                }

            } catch (InterruptedException ignored) {
            } finally {
                delete(out);
            }

        }
    }

}
