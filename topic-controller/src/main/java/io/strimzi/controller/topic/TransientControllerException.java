/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * A transient exception is one where we cannot <em>currently</em> complete the work required for reconciliation
 * but the problem should have gone away if we retry later.
 */
public class TransientControllerException extends ControllerException {

    public TransientControllerException(HasMetadata involvedObject, String message) {
        super(involvedObject, message);
    }

    public TransientControllerException(HasMetadata involvedObject, Throwable cause) {
        super(involvedObject, cause);
    }

    public TransientControllerException(HasMetadata involvedObject, String message, Throwable cause) {
        super(involvedObject, message, cause);
    }

    public TransientControllerException(Throwable cause) {
        super(cause);
    }

    public TransientControllerException(String message) {
        super(message);
    }
}
