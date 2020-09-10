/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.kafka.model.status.HasStatus;
import io.strimzi.api.kafka.model.status.Status;

public interface HasSpecAndStatus<T extends Spec, S extends Status> extends HasMetadata, HasSpec<T>, HasStatus<S> {
}
