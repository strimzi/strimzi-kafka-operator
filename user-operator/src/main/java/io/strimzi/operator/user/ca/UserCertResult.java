/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.ca;

import io.strimzi.certs.CertAndKey;

/**
 * Result of issuing a user certificate.
 *
 * @param caCertBase64      Base64-encoded CA certificate
 * @param userCertAndKey    Generated user certificate and key
 */
public record UserCertResult(String caCertBase64, CertAndKey userCertAndKey) {
}
