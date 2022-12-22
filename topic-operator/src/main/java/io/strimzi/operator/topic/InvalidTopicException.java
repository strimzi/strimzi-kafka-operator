/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * Thrown when an invalid topic is detected
 * (e.g. one with a negative number of partitions, etc.).
 */
public class InvalidTopicException extends OperatorException {

    /**
     * Constructor
     *
     * @param involvedObject  Kubernetes resource with metadata containing the namespace and cluster name
     * @param message   The error message
     */
    public InvalidTopicException(HasMetadata involvedObject, String message) {
        super(involvedObject, message);
    }
}
