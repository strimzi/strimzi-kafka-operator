/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * An exception possibly with an attached K8S resource (e.g. a KafkaTopic).
 */
public class OperatorException extends RuntimeException {

    private final HasMetadata involvedObject;

    public OperatorException(HasMetadata involvedObject, String message) {
        this(involvedObject, message, null);
    }

    public OperatorException(HasMetadata involvedObject, Throwable cause) {
        this(involvedObject, null, cause);
    }

    public OperatorException(HasMetadata involvedObject, String message, Throwable cause) {
        super(message, cause);
        this.involvedObject = involvedObject;
    }

    public OperatorException(Throwable cause) {
        this(null, null, cause);
    }

    public OperatorException(String message) {
        this(null, message, null);
    }

    public OperatorException(String message, Throwable cause) {
        this(null, message, cause);
    }

    public HasMetadata getInvolvedObject() {
        return involvedObject;
    }
}
