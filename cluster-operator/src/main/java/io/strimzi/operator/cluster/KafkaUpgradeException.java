/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.strimzi.operator.cluster.model.InvalidResourceException;

/**
 * Thrown for exceptional circumstances when upgrading (or downgrading) Kafka clusters from
 * one Kafka version to another.
 */
public class KafkaUpgradeException extends InvalidResourceException {
    /**
     * Constructor
     *
     * @param s     Error message
     */
    public KafkaUpgradeException(String s) {
        super(s);
    }
}
