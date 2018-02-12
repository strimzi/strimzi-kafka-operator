/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * An exception possibly with an attached K8S resource (e.g. a ConfigMap).
 */
public class ControllerException extends RuntimeException {

    private final HasMetadata involvedObject;

    public ControllerException(HasMetadata involvedObject, String message) {
        this(involvedObject, message, null);
    }

    public ControllerException(HasMetadata involvedObject, Throwable cause) {
        this(involvedObject, null, cause);
    }

    public ControllerException(HasMetadata involvedObject, String message, Throwable cause) {
        super(message, cause);
        this.involvedObject = involvedObject;
    }

    public ControllerException(Throwable cause) {
        this(null, null, cause);
    }

    public ControllerException(String message) {
        this(null, message, null);
    }

    public HasMetadata getInvolvedObject() {
        return involvedObject;
    }
}
