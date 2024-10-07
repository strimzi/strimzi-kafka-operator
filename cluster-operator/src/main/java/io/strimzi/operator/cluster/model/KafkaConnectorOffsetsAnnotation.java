/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

/**
 * Values for strimzi.io/connector-offsets annotation on KafkaConnector or KafkaMirrorMaker2 resource.
 */
public enum KafkaConnectorOffsetsAnnotation {
    /**
     * No annotation set on the KafkaConnector or KafkaMirrorMaker2 resource.
     */
    none,
    /**
     * Used to trigger listing offsets.
     */
    list,
    /**
     * Used to trigger altering offsets.
     * This value should only be used when the connector is in the {@code stopped} state.
     */
    alter,
    /**
     * Used to trigger resetting offsets.
     * This value should only be used when the connector is in the {@code stopped} state.
     */
    reset
}
