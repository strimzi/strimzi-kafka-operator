/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.operator.common.model.PemAuthIdentity;

/**
 * Represents the set of client auth identities for the Cluster Operator needed to bootstrap different
 * clients used during the reconciliation.
 * @param pemAuthIdentity Cluster Operator identity for TLS client authentication in PEM format
 * @param pkcs12AuthIdentity Cluster Operator identity for TLS client authentication in PKCS12 format
 */
public record ClusterOperatorAuthIdentity(PemAuthIdentity pemAuthIdentity, ClusterOperatorPKCS12AuthIdentity pkcs12AuthIdentity) {
}
