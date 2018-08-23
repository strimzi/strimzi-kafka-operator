/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import java.util.Base64;

public class CertAndKey {

    private final byte[] key;
    private final byte[] cert;

    public CertAndKey(byte[] key, byte[] cert) {
        this.key = key;
        this.cert = cert;
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
}
