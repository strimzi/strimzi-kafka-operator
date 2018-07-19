/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.api.model.Doneable;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicFluentImpl;

/**
 * A {@code CustomResourceDoneable<Topic>} required for using Fabric8 CRD support.
 */
public class DoneableKafkaTopic //extends CustomResourceDoneable<Topic>
    extends KafkaTopicFluentImpl<DoneableKafkaTopic> implements Doneable<KafkaTopic> {

    private final KafkaTopic resource;
    private final Function<KafkaTopic, KafkaTopic> function;

    public DoneableKafkaTopic(KafkaTopic resource, Function<KafkaTopic, KafkaTopic> function) {
        this.resource = resource;
        this.function = function;
    }
    public DoneableKafkaTopic(KafkaTopic resource) {
        this(resource, x -> x);
    }

    @Override
    public KafkaTopic done() {
        return function.apply(resource);
    }

    public boolean equals(Object other) {
        return super.equals(other);
    }

    public int hashCode() {
        return super.hashCode();
    }


}
