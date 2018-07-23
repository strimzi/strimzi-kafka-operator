/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import io.strimzi.api.kafka.model.KafkaTopic;

/**
 * A {@code CustomResourceDoneable<Topic>} required for using Fabric8 CRD support.
 */
public class DoneableKafkaTopic extends CustomResourceDoneable<KafkaTopic> {
    public DoneableKafkaTopic(KafkaTopic resource, Function<KafkaTopic, KafkaTopic> function) {
        super(resource, function);
    }
    public DoneableKafkaTopic(KafkaTopic resource) {
        super(resource, x -> x);
    }
}