/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.auth;

/**
 * Represents a TLS client authentication identity needed to bootstrap different clients. Provides the same identity in
 * both the PEM and PKCS12 format to accommodate different client requirements.
 * @param pemAuthIdentity Identity for TLS client authentication in PEM format
 * @param pkcs12AuthIdentity Identity for TLS client authentication in PKCS12 format
 */
public record TlsIdentitySet(PemAuthIdentity pemAuthIdentity, Pkcs12AuthIdentity pkcs12AuthIdentity) {
}
