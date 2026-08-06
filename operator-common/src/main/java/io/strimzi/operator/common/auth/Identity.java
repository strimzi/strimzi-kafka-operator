/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.auth;

/**
 * Represents the operator's identity configuration, including trust material and authentication credentials,
 * used when connecting to operands.
 *
 * @param trustSet      Trust set for esablishing the trust with the operand
 * @param authIdentity  Authentication identify for authenticating the operator
 */
public record Identity(TrustSet trustSet, AuthIdentity authIdentity) {
    /**
     * Dummy identity used in tests
     */
    public static final Identity DUMMY_IDENTITY = new Identity(null, null);
}
