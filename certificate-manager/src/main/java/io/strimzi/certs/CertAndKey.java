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
     * Gets the key as a byte array.
     *
     * @return The key as a byte array
     */
    public byte[] key() {
        return key;
    }

    /**
     * Gets the key as a base64 encoded String.
     *
     * @return  The key as base64 encoded String
     */
    public String keyAsBase64String() {
        return Base64.getEncoder().encodeToString(key());
    }

    /**
     * Gets the certificate as a byte array.
     *
     * @return  The cert as a byte array
     */
    public byte[] cert() {
        return cert;
    }

    /**
     * Gets the certificate as a base64 encoded String.
     *
     * @return  The cert as base64 encoded String
     */
    public String certAsBase64String() {
        return Base64.getEncoder().encodeToString(cert());
    }

    /**
     * Gets the truststore as a byte array.
     *
     * @return  The truststore as a byte array
     */
    public byte[] trustStore() {
        return trustStore;
    }

    /**
     * Gets the truststore as a base64 encoded String.
     *
     * @return  The truststore as base64 encoded String
     */
    public String trustStoreAsBase64String() {
        return Base64.getEncoder().encodeToString(trustStore());
    }

    /**
     * Gets the keystore as a byte array.
     *
     * @return  The keystore as a byte array
     */
    public byte[] keyStore() {
        return keyStore;
    }

    /**
     * Gets the keystore as a base64 encoded String.
     *
     * @return  The keystore as base64 encoded String
     */
    public String keyStoreAsBase64String() {
        return Base64.getEncoder().encodeToString(keyStore());
    }

    /**
     * Gets the store password.
     *
     * @return  The store password
     */
    public String storePassword() {
        return storePassword;
    }

    /**
     * Gets the store password as a base64 encoded String.
     *
     * @return  The store password as base64 encoded String
     */
    public String storePasswordAsBase64String() {
        return Base64.getEncoder().encodeToString(storePassword.getBytes(StandardCharsets.US_ASCII));
    }
}
