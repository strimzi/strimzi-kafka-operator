/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.auth;

import java.util.Map;

/**
 * Interface for various types of Authentication identities the operator is using when connecting to the operands.
 */
public interface AuthIdentity {
    /**
     * Indicates whether this authentication is SASL-based or not.
     *
     * @return  True when the authentication is SASL-based. False otherwise.
     */
    boolean isSasl();

    /**
     * Returns the Kafka authentication properties.
     *
     * @return  Map of Kafka authentication properties.
     */
    Map<String, String> kafkaClientProperties();
}
