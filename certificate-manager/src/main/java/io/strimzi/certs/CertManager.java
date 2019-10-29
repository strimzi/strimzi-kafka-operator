/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import java.io.File;
import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public interface CertManager {
    /**
     * Generate a self-signed certificate
     *
     * @param keyFile path to the file which will contain the private key
     * @param certFile path to the file which will contain the self signed certificate
     * @param sbj subject information
     * @param days certificate duration
     * @throws IOException If an input or output file could not be read/written.
     * @throws CertificateException if it's not able to load a certificate as X509
     * @throws KeyStoreException if it's not possible to create the keystore
     * @throws NoSuchAlgorithmException if the algorithm for loading certificates is not supported
     */
    void generateSelfSignedCert(File keyFile, File certFile, Subject sbj, int days)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException;

    /**
     * Generate a self-signed certificate
     *
     * @param keyFile path to the file which will contain the private key
     * @param certFile path to the file which will contain the self signed certificate
     * @param trustStoreFile path to the file which will contain the self signed certificate in PKCS12 format
     * @param trustStorePassword password for the truststore
     * @param sbj subject information
     * @param days certificate duration
     * @throws IOException If an input or output file could not be read/written.
     * @throws CertificateException if it's not able to load a certificate as X509
     * @throws KeyStoreException if it's not possible to create the keystore
     * @throws NoSuchAlgorithmException if the algorithm for loading certificates is not supported
     */
    void generateSelfSignedCert(File keyFile, File certFile, File trustStoreFile, String trustStorePassword, Subject sbj, int days)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException;

    /**
     * Generate a self-signed certificate
     *
     * @param keyFile path to the file which will contain the private key
     * @param certFile path to the file which will contain the self signed certificate
     * @param days certificate duration
     * @throws IOException If an input or output file could not be read/written.
     * @throws CertificateException if it's not able to load a certificate as X509
     * @throws KeyStoreException if it's not possible to create the keystore
     * @throws NoSuchAlgorithmException if the algorithm for loading certificates is not supported
     */
    void generateSelfSignedCert(File keyFile, File certFile, int days)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException;

    /**
     * Renew a new self-signed certificate, keeping the existing private key
     * @param keyFile path to the file containing the existing private key
     * @param certFile path to the file which will contain the new self signed certificate
     * @param sbj subject information
     * @param days certificate duration
     * @throws IOException If an input or output file could not be read/written.
     * @throws CertificateException if it's not able to load a certificate as X509
     * @throws KeyStoreException if it's not possible to create the keystore
     * @throws NoSuchAlgorithmException if the algorithm for loading certificates is not supported
     */
    void renewSelfSignedCert(File keyFile, File certFile, Subject sbj, int days)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException;

    /**
     * Renew a new self-signed certificate, keeping the existing private key
     * @param keyFile path to the file containing the existing private key
     * @param certFile path to the file which will contain the new self signed certificate
     * @param trustStoreFile path to the file which will contain the new self signed certificate in PKCS12 format
     * @param trustStorePassword password for the truststore
     * @param sbj subject information
     * @param days certificate duration
     * @throws IOException If an input or output file could not be read/written.
     * @throws CertificateException if it's not able to load a certificate as X509
     * @throws KeyStoreException if it's not possible to create the keystore
     * @throws NoSuchAlgorithmException if the algorithm for loading certificates is not supported
     */
    void renewSelfSignedCert(File keyFile, File certFile, File trustStoreFile, String trustStorePassword, Subject sbj, int days)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException;

    /**
     * Generate a certificate sign request
     *
     * @param keyFile path to the file which will contain the private key
     * @param csrFile path to the file which will contain the certificate sign request
     * @param sbj subject information
     * @throws IOException If an input or output file could not be read/written.
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
     * @throws IOException If an input or output file could not be read/written.
     */
    void generateCert(File csrFile, File caKey, File caCert, File crtFile, int days) throws IOException;

    /**
     * Generate a certificate signed by a Certificate Authority
     *
     * @param csrFile path to the file containing the certificate sign request
     * @param caKey path to the file containing the CA private key
     * @param caCert path to the file containing the CA certificate
     * @param crtFile path to the file which will contain the signed certificate
     * @param sbj subject information
     * @param days certificate duration
     * @throws IOException If an input or output file could not be read/written.
     */
    void generateCert(File csrFile, File caKey, File caCert, File crtFile, Subject sbj, int days) throws IOException;

    /**
     * Generate a certificate signed by a Certificate Authority
     *
     * @param csrFile path to the file containing the certificate sign request
     * @param caKey CA private key bytes
     * @param caCert CA certificate bytes
     * @param crtFile path to the file which will contain the signed certificate
     * @param days certificate duration
     * @throws IOException If an input or output file could not be read/written.
     */
    void generateCert(File csrFile, byte[] caKey, byte[] caCert, File crtFile, int days) throws IOException;

    /**
     * Generate a certificate signed by a Certificate Authority
     *
     * @param csrFile path to the file containing the certificate sign request
     * @param caKey CA private key bytes
     * @param caCert CA certificate bytes
     * @param crtFile path to the file which will contain the signed certificate
     * @param sbj subject information
     * @param days certificate duration
     * @throws IOException If an input or output file could not be read/written.
     */
    void generateCert(File csrFile, byte[] caKey, byte[] caCert, File crtFile, Subject sbj, int days) throws IOException;
}
