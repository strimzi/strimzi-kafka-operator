/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * An exception possibly with an attached K8S resource (e.g. a KafkaTopic).
 */
public class OperatorException extends RuntimeException {

    /** Kubernetes resource with metadata containing the namespace and cluster name etc */
    private final HasMetadata involvedObject;

    /**
     * Constructor
     *
     * @param involvedObject   C
     * @param message    The Error message
     */
    public OperatorException(HasMetadata involvedObject, String message) {
        this(involvedObject, message, null);
    }

    /**
     * Constructor
     *
     * @param involvedObject   Kubernetes resource with metadata containing the namespace and cluster name etc
     * @param cause            Exception which caused this error
     */
    public OperatorException(HasMetadata involvedObject, Throwable cause) {
        this(involvedObject, null, cause);
    }

    /**
     * Constructor
     *
     * @param involvedObject   Kubernetes resource with metadata containing the namespace and cluster name etc
     * @param message          The Error message
     * @param cause            Exception which caused this error
     */
    public OperatorException(HasMetadata involvedObject, String message, Throwable cause) {
        super(message, cause);
        this.involvedObject = involvedObject;
    }

    /**
     * Constructor
     *
     * @param message          The Error message
     * @param cause            Exception which caused this error
     */
    public OperatorException(String message, Throwable cause) {
        this(null, message, cause);
    }
}
