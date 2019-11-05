/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class CertAndKey {

    private final byte[] key;
    private final byte[] cert;
    private final byte[] trustStore;
    private final byte[] keyStore;
    private final String storePassword;

    public CertAndKey(byte[] key, byte[] cert) {
        this(key, cert, null, null, null);
    }

    public CertAndKey(byte[] key, byte[] cert, byte[] trustStore, byte[] keyStore, String storePassword) {
        this.key = key;
        this.cert = cert;
        this.trustStore = trustStore;
        this.keyStore = keyStore;
        this.storePassword = storePassword;
    }

    public byte[] key() {
        return key;
    }

    public String keyAsBase64String() {
        return Base64.getEncoder().encodeToString(key());
    }

    public byte[] cert() {
        return cert;
    }

    public String certAsBase64String() {
        return Base64.getEncoder().encodeToString(cert());
    }

    public byte[] trustStore() {
        return trustStore;
    }

    public String trustStoreAsBase64String() {
        return Base64.getEncoder().encodeToString(trustStore());
    }

    public byte[] keyStore() {
        return keyStore;
    }

    public String keyStoreAsBase64String() {
        return Base64.getEncoder().encodeToString(keyStore());
    }

    public String storePassword() {
        return storePassword;
    }

    public String storePasswordAsBase64String() {
        return Base64.getEncoder().encodeToString(storePassword.getBytes(StandardCharsets.US_ASCII));
    }
}
