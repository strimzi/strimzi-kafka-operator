/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * Thrown when the operator detects an attempt to decrease the number of
 * partitions in a topic (something which Kafka does not support).
 */
public class PartitionDecreaseException extends OperatorException {

    /**
     * Constructor
     *
     * @param resource   Kubernetes resource with metadata containing the namespace and cluster name etc
     * @param message    The Error message
     */
    public PartitionDecreaseException(HasMetadata resource, String message) {
        super(resource, message);
    }
}
