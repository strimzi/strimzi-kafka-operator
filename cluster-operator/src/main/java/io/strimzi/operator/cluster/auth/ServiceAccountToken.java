/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.auth;

/**
 * Represents a Service Account token obtained from the Kubernetes TokenRequest API.
 *
 * @param value         The token itself (a JWT token)
 * @param issuedAtMs    Time when the token was issued as milliseconds since the epoch
 * @param expiresAtMs   Time when the token expires as milliseconds since the epoch
 */
public record ServiceAccountToken(String value, long issuedAtMs, long expiresAtMs) {
    /**
     * Checks whether the token can be still used at a given point in time. The token is considered usable only until
     * the given fraction of its lifetime elapses. That leaves enough time for the client to use it before it expires.
     *
     * @param nowMs         Point in time for which the validity should be checked as milliseconds since the epoch
     * @param threshold     Fraction of the token lifetime after which the token is not used anymore
     *
     * @return  True if the token can be still used. False otherwise.
     */
    public boolean isUsableAt(long nowMs, double threshold) {
        return nowMs < issuedAtMs + (long) (threshold * (expiresAtMs - issuedAtMs));
    }
}
