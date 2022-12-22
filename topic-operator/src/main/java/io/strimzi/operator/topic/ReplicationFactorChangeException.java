/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * Thrown when the operator detects an attempt to change the
 * replication factor of a topic
 * (something which the operator does not currently support because
 * it requires replica placement decisions).
 */
public class ReplicationFactorChangeException extends OperatorException {

    /**
     * Constructor
     *
     * @param resource   Kubernetes resource with metadata containing the namespace and cluster name etc
     * @param message    The Error message
     */
    public ReplicationFactorChangeException(HasMetadata resource, String message) {
        super(resource, message);
    }
}
