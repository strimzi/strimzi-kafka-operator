/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.fabric8.kubernetes.api.model.HasMetadata;

public class InvalidConfigMapException extends ControllerException {
    public InvalidConfigMapException(HasMetadata involvedObject, String message) {
        super(involvedObject, message);
    }
}
