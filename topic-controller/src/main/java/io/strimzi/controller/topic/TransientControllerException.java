/*
 * Copyright 2018, Strimzi Authors.
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
