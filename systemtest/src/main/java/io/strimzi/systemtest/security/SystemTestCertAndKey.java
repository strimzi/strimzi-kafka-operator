/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.X509Certificate;

public class SystemTestCertAndKey {

    private final X509Certificate certificate;
    private final PrivateKey privateKey;

    public SystemTestCertAndKey(X509Certificate certificate, PrivateKey privateKey) {
        this.certificate = certificate;
        this.privateKey = privateKey;
    }

    public X509Certificate getCertificate() {
        return certificate;
    }

    public PublicKey getPublicKey() {
        return certificate.getPublicKey();
    }

    public PrivateKey getPrivateKey() {
        return privateKey;
    }
}
