/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.config.model;

/**
 * Scope of the configuration option
 */
public enum Scope {
    /**
     * Per-broker option
     */
    PER_BROKER,

    /**
     * Cluster-wide option
     */
    CLUSTER_WIDE,

    /**
     * Read-only option
     */
    READ_ONLY
}
