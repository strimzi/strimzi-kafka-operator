/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class MockCertManager implements CertManager {

    private void write(File keyFile, String str) throws IOException {
        try (FileWriter writer = new FileWriter(keyFile)) {
            writer.write(str);
        }
    }

    /**
     * Generate a self-signed certificate
     *
     * @param keyFile  path to the file which will contain the private key
     * @param certFile path to the file which will contain the self signed certificate
     * @param sbj      subject information
     * @param days     certificate duration
     * @throws IOException
     */
    @Override
    public void generateSelfSignedCert(File keyFile, File certFile, Subject sbj, int days) throws IOException {
        write(keyFile, "key file for self-signed cert");
        write(certFile, "cert file for self-signed cert");
    }

    /**
     * Generate a self-signed certificate
     *
     * @param keyFile  path to the file which will contain the private key
     * @param certFile path to the file which will contain the self signed certificate
     * @param days     certificate duration
     * @throws IOException
     */
    @Override
    public void generateSelfSignedCert(File keyFile, File certFile, int days) throws IOException {
        write(keyFile, "key file for self-signed cert");
        write(certFile, "cert file for self-signed cert");
    }

    /**
     * Generate a certificate sign request
     *
     * @param keyFile path to the file which will contain the private key
     * @param csrFile path to the file which will contain the certificate sign request
     * @param sbj     subject information
     */
    @Override
    public void generateCsr(File keyFile, File csrFile, Subject sbj) throws IOException {
        write(csrFile, "csr file");
    }

    /**
     * Generate a certificate signed by a Certificate Authority
     *
     * @param csrFile path to the file containing the certificate sign request
     * @param caKey   path to the file containing the CA private key
     * @param caCert  path to the file containing the CA certificate
     * @param crtFile path to the file which will contain the signed certificate
     * @param days    certificate duration
     * @throws IOException
     */
    @Override
    public void generateCert(File csrFile, File caKey, File caCert, File crtFile, int days) throws IOException {
        write(crtFile, "crt file");
    }

    /**
     * Generate a certificate signed by a Certificate Authority
     *
     * @param csrFile path to the file containing the certificate sign request
     * @param caKey   CA private key bytes
     * @param caCert  CA certificate bytes
     * @param crtFile path to the file which will contain the signed certificate
     * @param days    certificate duration
     * @throws IOException
     */
    @Override
    public void generateCert(File csrFile, byte[] caKey, byte[] caCert, File crtFile, int days) throws IOException {
        write(crtFile, "crt file");
    }
}
