/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import io.strimzi.api.kafka.model.Topic;

/**
 * A {@code CustomResourceDoneable<Topic>} required for using Fabric8 CRD support.
 */
public class DoneableTopic extends CustomResourceDoneable<Topic> {
    public DoneableTopic(Topic resource, Function<Topic, Topic> function) {
        super(resource, function);
    }
    public DoneableTopic(Topic resource) {
        super(resource, x -> x);
    }
}
