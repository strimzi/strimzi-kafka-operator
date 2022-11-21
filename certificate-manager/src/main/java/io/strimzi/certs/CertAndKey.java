/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Holds the Certificate and Key
 */
public class CertAndKey {
    private final byte[] key;
    private final byte[] cert;
    private final byte[] trustStore;
    private final byte[] keyStore;
    private final String storePassword;

    /**
     * Construct the CertAndKey instance from byte arrays of the certificate and key
     *
     * @param key   Byte array with the key bytes
     * @param cert  Byte array with the cert bytes
     */
    public CertAndKey(byte[] key, byte[] cert) {
        this(key, cert, null, null, null);
    }

    /**
     * Constructs the CertAndKey instance
     *
     * @param key               Byte array with the key bytes
     * @param cert              Byte array with the cert bytes
     * @param trustStore        Byte array with the truststore
     * @param keyStore          Byte array with the keystore
     * @param storePassword     The store password
     */
    public CertAndKey(byte[] key, byte[] cert, byte[] trustStore, byte[] keyStore, String storePassword) {
        this.key = key;
        this.cert = cert;
        this.trustStore = trustStore;
        this.keyStore = keyStore;
        this.storePassword = storePassword;
    }

    /**
     * @return The key as a byte array
     */
    public byte[] key() {
        return key;
    }

    /**
     * @return  The key as base64 encoded String
     */
    public String keyAsBase64String() {
        return Base64.getEncoder().encodeToString(key());
    }

    /**
     * @return  The cert as a byte array
     */
    public byte[] cert() {
        return cert;
    }

    /**
     * @return  The cert as base64 encoded String
     */
    public String certAsBase64String() {
        return Base64.getEncoder().encodeToString(cert());
    }

    /**
     * @return  The truststore as a byte array
     */
    public byte[] trustStore() {
        return trustStore;
    }

    /**
     * @return  The truststore as base64 encoded String
     */
    public String trustStoreAsBase64String() {
        return Base64.getEncoder().encodeToString(trustStore());
    }

    /**
     * @return  The keystore as a byte array
     */
    public byte[] keyStore() {
        return keyStore;
    }

    /**
     * @return  The keystore as base64 encoded String
     */
    public String keyStoreAsBase64String() {
        return Base64.getEncoder().encodeToString(keyStore());
    }

    /**
     * @return  The store password
     */
    public String storePassword() {
        return storePassword;
    }

    /**
     * @return  The store password as base64 encoded String
     */
    public String storePasswordAsBase64String() {
        return Base64.getEncoder().encodeToString(storePassword.getBytes(StandardCharsets.US_ASCII));
    }
}
