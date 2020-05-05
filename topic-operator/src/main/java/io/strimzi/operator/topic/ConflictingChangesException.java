/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * Thrown when the operator detects that the {@code KafkaTopic}
 * and the topic in Kafka had conflicting changes.
 */
public class ConflictingChangesException extends OperatorException {
    public ConflictingChangesException(HasMetadata resource, String message) {
        super(resource, message);
    }
}
