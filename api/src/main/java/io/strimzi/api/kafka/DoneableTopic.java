/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.api.model.Doneable;
import io.strimzi.api.kafka.model.Topic;
import io.strimzi.api.kafka.model.TopicFluentImpl;

/**
 * A {@code CustomResourceDoneable<Topic>} required for using Fabric8 CRD support.
 */
public class DoneableTopic //extends CustomResourceDoneable<Topic>
    extends TopicFluentImpl<DoneableTopic> implements Doneable<Topic> {

    private final Topic resource;
    private final Function<Topic, Topic> function;

    public DoneableTopic(Topic resource, Function<Topic, Topic> function) {
        this.resource = resource;
        this.function = function;
    }
    public DoneableTopic(Topic resource) {
        this(resource, x -> x);
    }

    @Override
    public Topic done() {
        return function.apply(resource);
    }

    public boolean equals(Object other) {
        return super.equals(other);
    }

    public int hashCode() {
        return super.hashCode();
    }


}
