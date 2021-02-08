/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * An OpenSSL based certificates manager
 */
public class OpenSslCertManager implements CertManager {
    public static final int MAXIMUM_CN_LENGTH = 64;

    private static final Logger log = LogManager.getLogger(OpenSslCertManager.class);

    public OpenSslCertManager() {}

    @Override
    public void generateSelfSignedCert(File keyFile, File certFile, int days) throws IOException {
        generateSelfSignedCert(keyFile, certFile, null, days);
    }

    @Override
    public void generateSelfSignedCert(File keyFile, File certFile, Subject sbj, int days) throws IOException {

        List<String> cmd = new ArrayList<>(asList("openssl", "req", "-x509", "-new", "-days", String.valueOf(days), "-batch", "-nodes",
                "-out", certFile.getAbsolutePath(), "-keyout", keyFile.getAbsolutePath()));

        File sna = null;
        File openSslConf = null;
        if (sbj != null) {

            if (sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {

                // subject alt names need to be in an openssl configuration file
                InputStream is = getClass().getClassLoader().getResourceAsStream("openssl.conf");
                openSslConf = File.createTempFile("openssl-", ".conf");
                Files.copy(is, openSslConf.toPath(), StandardCopyOption.REPLACE_EXISTING);

                sna = addSubjectAltNames(openSslConf, sbj);
                cmd.addAll(asList("-config", sna.toPath().toString(), "-extensions", "v3_req"));
            }

            cmd.addAll(asList("-subj", sbj.toString()));
        }

        exec(cmd);

        if (sna != null) {
            if (!sna.delete()) {
                log.warn("{} cannot be deleted", sna.getName());
            }
        }
        if (openSslConf != null) {
            if (!openSslConf.delete()) {
                log.warn("{} cannot be deleted", openSslConf.getName());
            }
        }
    }

    @Override
    public void addCertToTrustStore(File certFile, String certAlias, File trustStoreFile, String trustStorePassword)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {

        try {
            FileInputStream isTrustStore = null;
            try {
                // check if the truststore file is empty or not, for loading its content eventually
                // the KeyStore class is able to create an empty store if the input stream is null
                if (trustStoreFile.length() > 0) {
                    isTrustStore = new FileInputStream(trustStoreFile);
                }

                FileInputStream isCertificate = null;
                try {
                    isCertificate = new FileInputStream(certFile);

                    CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
                    X509Certificate certificate = (X509Certificate) certFactory.generateCertificate(isCertificate);

                    KeyStore trustStore = KeyStore.getInstance("PKCS12");
                    trustStore.load(isTrustStore, trustStorePassword.toCharArray());
                    trustStore.setEntry(certAlias, new KeyStore.TrustedCertificateEntry(certificate), null);

                    FileOutputStream osTrustStore = null;
                    try {
                        osTrustStore = new FileOutputStream(trustStoreFile);
                        trustStore.store(osTrustStore, trustStorePassword.toCharArray());
                    } finally {
                        if (osTrustStore != null) {
                            osTrustStore.close();
                        }
                    }
                } finally {
                    if (isCertificate != null) {
                        isCertificate.close();
                    }
                }
            } finally {
                if (isTrustStore != null) {
                    isTrustStore.close();
                }
            }
        } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException e) {
            throw e;
        }
    }

    @Override
    public void addKeyAndCertToKeyStore(File keyFile, File certFile, String alias, File keyStoreFile, String keyStorePassword) throws IOException {

        List<String> cmd = asList("openssl", "pkcs12", "-export", "-in", certFile.getAbsolutePath(),
                "-inkey", keyFile.getAbsolutePath(), "-name", alias, "-out", keyStoreFile.getAbsolutePath(), "-passout",
                "pass:" + keyStorePassword);

        exec(cmd);
    }

    @Override
    public void deleteFromTrustStore(List<String> aliases, File trustStoreFile, String trustStorePassword)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {

        try {
            FileInputStream isTrustStore = null;
            try {
                isTrustStore = new FileInputStream(trustStoreFile);
                KeyStore trustStore = KeyStore.getInstance("PKCS12");
                trustStore.load(isTrustStore, trustStorePassword.toCharArray());
                for (String alias : aliases) {
                    trustStore.deleteEntry(alias);
                }
                FileOutputStream osTrustStore = null;
                try {
                    osTrustStore = new FileOutputStream(trustStoreFile);
                    trustStore.store(osTrustStore, trustStorePassword.toCharArray());
                } finally {
                    if (osTrustStore != null) {
                        osTrustStore.close();
                    }
                }
            } finally {
                if (isTrustStore != null) {
                    isTrustStore.close();
                }
            }
        } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException e) {
            throw e;
        }
    }

    @Override
    public void renewSelfSignedCert(File keyFile, File certFile, Subject sbj, int days) throws IOException {
        // See https://serverfault.com/questions/306345/certification-authority-root-certificate-expiry-and-renewal

        //openssl req -new -key root.key -out newcsr.csr
        File csrFile = File.createTempFile("renewal", ".csr");

        List<String> cmd = new ArrayList<>(asList("openssl", "x509",
                "-x509toreq",
                "-in", certFile.getAbsolutePath(),
                "-signkey", keyFile.getAbsolutePath(),
                "-out", csrFile.getAbsolutePath()));

        exec(cmd);

        //openssl x509 -req -days 3650 -in newcsr.csr -signkey root.key -out newroot.pem
        List<String> cmd2 = new ArrayList<>(asList("openssl", "x509",
                "-req",
                "-days", String.valueOf(days),
                "-in", csrFile.getAbsolutePath(),
                "-signkey", keyFile.getAbsolutePath(),
                "-out", certFile.getAbsolutePath()));

        // subject alt names need to be in an openssl configuration file
        InputStream is = getClass().getClassLoader().getResourceAsStream("openssl.conf");
        File openSslConf = File.createTempFile("openssl-", ".conf");
        Files.copy(is, openSslConf.toPath(), StandardCopyOption.REPLACE_EXISTING);

        File sna = addSubjectAltNames(openSslConf, sbj);
        cmd2.addAll(asList("-extfile", sna.toPath().toString(), "-extensions", "v3_req"));

        exec(cmd2);

        if (!sna.delete()) {
            log.warn("{} cannot be deleted", sna.getName());
        }
        if (!openSslConf.delete()) {
            log.warn("{} cannot be deleted", openSslConf.getName());
        }

        if (!csrFile.delete()) {
            log.warn("{} cannot be deleted", csrFile.getName());
        }
    }

    /**
     * Add subject alt names section to the provided openssl configuration file
     *
     * @param opensslConf openssl configuration file
     * @param sbj subject information
     * @return openssl configuration file with subject alt names added
     * @throws IOException
     */
    private File addSubjectAltNames(File opensslConf, Subject sbj) throws IOException {

        File sna = File.createTempFile("sna-", ".conf");
        Files.copy(opensslConf.toPath(), sna.toPath(), StandardCopyOption.REPLACE_EXISTING);

        if (sbj != null) {
            if (sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {
                BufferedWriter out = null;
                try {
                    out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(sna, true), "UTF8"));
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
                } finally {
                    if (out != null) {
                        out.close();
                    }
                }
            }
        }

        return sna;
    }

    @Override
    public void generateCsr(File keyFile, File csrFile, Subject sbj) throws IOException {

        List<String> cmd = new ArrayList<>(asList("openssl", "req", "-new", "-batch", "-nodes",
                "-keyout", keyFile.getAbsolutePath(), "-out", csrFile.getAbsolutePath()));

        File sna = null;
        File openSslConf = null;
        if (sbj != null) {

            if (sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {

                // subject alt names need to be in an openssl configuration file
                InputStream is = getClass().getClassLoader().getResourceAsStream("openssl.conf");
                openSslConf = File.createTempFile("openssl-", ".conf");
                Files.copy(is, openSslConf.toPath(), StandardCopyOption.REPLACE_EXISTING);

                sna = addSubjectAltNames(openSslConf, sbj);
                cmd.addAll(asList("-config", sna.toPath().toString(), "-extensions", "v3_req"));
            }

            cmd.addAll(asList("-subj", sbj.toString()));
        }

        exec(cmd);

        if (sna != null) {
            if (!sna.delete()) {
                log.warn("{} cannot be deleted", sna.getName());
            }
        }
        if (openSslConf != null) {
            if (!openSslConf.delete()) {
                log.warn("{} cannot be deleted", openSslConf.getName());
            }
        }
    }

    @Override
    public void generateCert(File csrFile, File caKey, File caCert, File crtFile, int days) throws IOException {
        generateCert(csrFile, caKey, caCert, crtFile, null, days);
    }

    @Override
    public void generateCert(File csrFile, byte[] caKey, byte[] caCert, File crtFile, int days) throws IOException {
        generateCert(csrFile, caKey, caCert, crtFile, null, days);
    }

    @Override
    public void generateCert(File csrFile, File caKey, File caCert, File crtFile, Subject sbj, int days) throws IOException {

        List<String> cmd = new ArrayList<>(asList("openssl", "x509", "-req", "-days", String.valueOf(days),
            "-in", csrFile.getAbsolutePath(), "-CA", caCert.getAbsolutePath(), "-CAkey", caKey.getAbsolutePath(), "-CAcreateserial",
            "-out", crtFile.getAbsolutePath()));

        File sna = null;
        File openSslConf = null;
        if (sbj != null) {

            if (sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {

                // subject alt names need to be in an openssl configuration file
                InputStream is = getClass().getClassLoader().getResourceAsStream("openssl.conf");
                openSslConf = File.createTempFile("openssl-", ".conf");
                Files.copy(is, openSslConf.toPath(), StandardCopyOption.REPLACE_EXISTING);

                sna = addSubjectAltNames(openSslConf, sbj);
                cmd.addAll(asList("-extfile", sna.toPath().toString(), "-extensions", "v3_req"));
            }
        }

        exec(cmd);

        if (sna != null) {
            if (!sna.delete()) {
                log.warn("{} cannot be deleted", sna.getName());
            }
        }
        if (openSslConf != null) {
            if (!openSslConf.delete()) {
                log.warn("{} cannot be deleted", openSslConf.getName());
            }
        }

        // We need to remove CA serial file
        Files.deleteIfExists(Paths.get(caCert.getPath().replace(".crt", ".srl")));
    }

    @Override
    public void generateCert(File csrFile, byte[] caKey, byte[] caCert, File crtFile, Subject sbj, int days) throws IOException {

        File caKeyFile = File.createTempFile("ca-key-", ".key");
        Files.write(caKeyFile.toPath(), caKey);

        File caCertFile = File.createTempFile("ca-crt-", ".crt");
        Files.write(caCertFile.toPath(), caCert);

        generateCert(csrFile, caKeyFile, caCertFile, crtFile, sbj, days);

        if (!caKeyFile.delete()) {
            log.warn("{} cannot be deleted", caKeyFile.getName());
        }
        if (!caCertFile.delete()) {
            log.warn("{} cannot be deleted", caCertFile.getName());
        }
    }

    private void exec(List<String> cmd) throws IOException {
        File out = null;

        try {

            out = File.createTempFile("openssl-", Integer.toString(cmd.hashCode()));

            ProcessBuilder processBuilder = new ProcessBuilder(cmd)
                    .redirectOutput(out)
                    .redirectErrorStream(true);
            log.debug("Running command {}", processBuilder.command());

            Process proc = processBuilder.start();

            OutputStream outputStream = proc.getOutputStream();
            // close subprocess' stdin
            outputStream.close();

            int result = proc.waitFor();
            String stdout = new String(Files.readAllBytes(out.toPath()), Charset.defaultCharset());

            log.debug(stdout);
            log.debug("result {}", result);

        } catch (InterruptedException ignored) {
        } finally {
            if (out != null) {
                if (!out.delete()) {
                    log.warn("{} cannot be deleted", out.getName());
                }
            }
        }
    }
}
