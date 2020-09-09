/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.model.Spec;
import io.strimzi.api.kafka.model.status.HasStatus;
import io.strimzi.api.kafka.model.status.Status;

public abstract class AbstractCustomResource<T extends Spec, S extends Status> extends CustomResource implements HasStatus<S> {
    public abstract T getSpec();
    public abstract void setSpec(T spec);
    public abstract S getStatus();
    public abstract void setStatus(S status);
}
