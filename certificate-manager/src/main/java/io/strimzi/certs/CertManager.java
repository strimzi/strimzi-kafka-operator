/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import java.io.File;
import java.io.IOException;

public interface CertManager {

    /**
     * Generate a self-signed certificate
     *
     * @param keyFile path to the file which will contain the private key
     * @param certFile path to the file which will contain the self signed certificate
     * @param sbj subject information
     * @param days certificate duration
     * @throws IOException
     */
    void generateSelfSignedCert(File keyFile, File certFile, Subject sbj, int days) throws IOException;

    /**
     * Generate a self-signed certificate
     *
     * @param keyFile path to the file which will contain the private key
     * @param certFile path to the file which will contain the self signed certificate
     * @param days certificate duration
     * @throws IOException
     */
    void generateSelfSignedCert(File keyFile, File certFile, int days) throws IOException;

    /**
     * Generate a certificate sign request
     *
     * @param keyFile path to the file which will contain the private key
     * @param csrFile path to the file which will contain the certificate sign request
     * @param sbj subject information
     */
    void generateCsr(File keyFile, File csrFile, Subject sbj) throws IOException;

    /**
     * Generate a certificate signed by a Certificate Authority
     *
     * @param csrFile path to the file containing the certificate sign request
     * @param caKey path to the file containing the CA private key
     * @param caCert path to the file containing the CA certificate
     * @param crtFile path to the file which will contain the signed certificate
     * @param days certificate duration
     * @throws IOException
     */
    void generateCert(File csrFile, File caKey, File caCert, File crtFile, int days) throws IOException;

    /**
     * Generate a certificate signed by a Certificate Authority
     *
     * @param csrFile path to the file containing the certificate sign request
     * @param caKey CA private key bytes
     * @param caCert CA certificate bytes
     * @param crtFile path to the file which will contain the signed certificate
     * @param days certificate duration
     * @throws IOException
     */
    void generateCert(File csrFile, byte[] caKey, byte[] caCert, File crtFile, int days) throws IOException;
}
