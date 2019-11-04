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
    private final byte[] store;
    private final String storePassword;

    public CertAndKey(byte[] key, byte[] cert) {
        this(key, cert, null, null);
    }

    public CertAndKey(byte[] key, byte[] cert, byte[] store, String storePassword) {
        this.key = key;
        this.cert = cert;
        this.store = store;
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

    public byte[] store() {
        return store;
    }

    public String storeAsBase64String() {
        return Base64.getEncoder().encodeToString(store());
    }

    public String storePassword() {
        return storePassword;
    }

    public String storePasswordAsBase64String() {
        return Base64.getEncoder().encodeToString(storePassword.getBytes(StandardCharsets.US_ASCII));
    }
}
