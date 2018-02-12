/*
 * Copyright 2018 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
