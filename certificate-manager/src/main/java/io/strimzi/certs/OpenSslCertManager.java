/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * An OpenSSL based certificates manager
 */
public class OpenSslCertManager implements CertManager {

    private static final Logger log = LogManager.getLogger(OpenSslCertManager.class);

    @Override
    public void generateSelfSignedCert(File keyFile, File certFile, int days) throws IOException {
        generateSelfSignedCert(keyFile, certFile, null, days);
    }

    @Override
    public void generateSelfSignedCert(File keyFile, File certFile, Subject sbj, int days) throws IOException {

        List<String> cmd = new ArrayList<>(Arrays.asList("openssl", "req", "-x509", "-new", "-days", String.valueOf(days), "-batch", "-nodes",
                "-out", certFile.getAbsolutePath(), "-keyout", keyFile.getAbsolutePath()));

        File sna = null;
        if (sbj != null) {

            if (sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {

                // subject alt names need to be in an openssl configuration file
                File file = new File(getClass().getClassLoader().getResource("openssl.conf").getFile());
                sna = addSubjectAltNames(file, sbj);
                cmd.addAll(Arrays.asList("-config", sna.toPath().toString(), "-extensions", "v3_req"));
            }

            cmd.addAll(Arrays.asList("-subj", sbj.toString()));
        }

        exec(cmd);

        if (sna != null) {
            if (!sna.delete()) {
                log.warn("{} cannot be deleted", sna.getName());
            }
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

        File sna = File.createTempFile("sna", "sna");
        Files.copy(opensslConf.toPath(), sna.toPath(), StandardCopyOption.REPLACE_EXISTING);

        StringBuilder sb = new StringBuilder();
        boolean newline = false;
        for (Map.Entry entry: sbj.subjectAltNames().entrySet()) {
            if (newline) {
                sb.append("\n");
            }
            sb.append(entry.getKey()).append(" = ").append(entry.getValue());
            newline = true;
        }

        PrintWriter out = null;
        try {
            out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(sna, true), "UTF8")));
            out.append(sb.toString());
        } catch (IOException e) {
            log.error("Error writing the subject alternative names", e);
        } finally {
            if (out != null)
                out.close();
        }

        return sna;
    }

    @Override
    public void generateCsr(File keyFile, File csrFile, Subject sbj) throws IOException {

        List<String> cmd = new ArrayList<>(Arrays.asList("openssl", "req", "-new", "-batch", "-nodes",
                "-keyout", keyFile.getAbsolutePath(), "-out", csrFile.getAbsolutePath()));

        File sna = null;
        if (sbj != null) {

            if (sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {

                // subject alt names need to be in an openssl configuration file
                File file = new File(getClass().getClassLoader().getResource("openssl.conf").getFile());
                sna = addSubjectAltNames(file, sbj);
                cmd.addAll(Arrays.asList("-config", sna.toPath().toString(), "-extensions", "v3_req"));
            }

            cmd.addAll(Arrays.asList("-subj", sbj.toString()));
        }

        exec(cmd);

        if (sna != null) {
            if (!sna.delete()) {
                log.warn("{} cannot be deleted", sna.getName());
            }
        }
    }

    @Override
    public void generateCert(File csrFile, File caKey, File caCert, File crtFile, int days) throws IOException {
        exec("openssl", "x509", "-req", "-days", String.valueOf(days), "-in", csrFile.getAbsolutePath(),
                "-CA", caCert.getAbsolutePath(), "-CAkey", caKey.getAbsolutePath(), "-CAcreateserial",
                "-out", crtFile.getAbsolutePath());
    }

    @Override
    public void generateCert(File csrFile, byte[] caKey, byte[] caCert, File crtFile, int days) throws IOException {

        File caKeyFile = File.createTempFile("tls", "ca");
        Files.write(caKeyFile.toPath(), caKey);

        File caCertFile = File.createTempFile("tls", "ca");
        Files.write(caCertFile.toPath(), caCert);

        generateCert(csrFile, caKeyFile, caCertFile, crtFile, days);

        caKeyFile.delete();
        caCertFile.delete();
    }

    private void exec(String... cmd) throws IOException {
        exec(Arrays.asList(cmd));
    }

    private void exec(List<String> cmd) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(cmd).redirectErrorStream(true);
        log.info("Running command {}", processBuilder.command());

        BufferedReader reader = null;
        try {
            Process proc = processBuilder.start();
            InputStream stdout = proc.getInputStream();
            reader = new BufferedReader(new InputStreamReader(stdout, "UTF8"));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            int result = proc.waitFor();
            log.info("result {}", result);
        } catch (InterruptedException ignored) {
        } finally {
            if (reader != null)
                reader.close();
        }
    }
}
