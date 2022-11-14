/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

/**
 * Thrown in case the Preconditions for the KafkaRoller fails.
 */
public class KafkaRollerPreconditionFailedException extends RuntimeException {

    public KafkaRollerPreconditionFailedException(String s) {
        super(s);
    }
}
