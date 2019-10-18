/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.config.model;

public enum Scope {
    PER_BROKER,
    CLUSTER_WIDE,
    READ_ONLY
}
