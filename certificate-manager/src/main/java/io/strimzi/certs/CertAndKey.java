/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

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

    public byte[] cert() {
        return cert;
    }
}
