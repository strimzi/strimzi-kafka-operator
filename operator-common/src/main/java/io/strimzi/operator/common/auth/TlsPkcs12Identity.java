/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.auth;

/**
 * Represents the trust set and TLS client authentication identity needed to bootstrap different clients connecting using mutual TLS.
 * @param pemTrustSet Trust set for TLS authentication in PEM format
 * @param pkcs12AuthIdentity Identity for TLS client authentication in PKCS12 format
 */
public record TlsPkcs12Identity(PemTrustSet pemTrustSet, Pkcs12AuthIdentity pkcs12AuthIdentity) {
}
