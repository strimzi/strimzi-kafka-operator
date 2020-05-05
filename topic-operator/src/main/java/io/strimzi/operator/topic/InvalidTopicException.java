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
    public InvalidTopicException(HasMetadata involvedObject, String message) {
        super(involvedObject, message);
    }
}
