/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

        if (sbj != null) {
            cmd.addAll(Arrays.asList("-subj", sbj.toString()));
        }

        exec(cmd);
    }

    @Override
    public void generateCsr(File keyFile, File csrFile, Subject sbj) throws IOException {
        exec("openssl", "req", "-new", "-batch", "-nodes",
                "-keyout", keyFile.getAbsolutePath(), "-subj", sbj.toString(), "-out", csrFile.getAbsolutePath());
    }

    @Override
    public void generateCert(File csrFile, File caKey, File caCert, File crtFile, int days) throws IOException {
        exec("openssl", "x509", "-req", "-days", String.valueOf(days), "-in", csrFile.getAbsolutePath(),
                "-CA", caCert.getAbsolutePath(), "-CAkey", caKey.getAbsolutePath(), "-CAcreateserial",
                "-out", crtFile.getAbsolutePath());
    }

    private void exec(String... cmd) throws IOException {
        exec(Arrays.asList(cmd));
    }

    private void exec(List<String> cmd) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(cmd);
        log.info("Running command {}", processBuilder.command());

        try {
            Process proc = processBuilder.start();
            int result = proc.waitFor();
            log.info("result {}", result);
        } catch (InterruptedException ignored) {
        }
    }
}
