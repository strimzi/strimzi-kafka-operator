/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.strimzi.api.kafka.model.KafkaUser;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;

/**
 * A {@code CustomResourceDoneable<KafkaUser>} required for using Fabric8 CRD support.
 */
public class DoneableKafkaUser extends CustomResourceDoneable<KafkaUser> {
    public DoneableKafkaUser(KafkaUser resource, Function<KafkaUser, KafkaUser> function) {
        super(resource, function);
    }
    public DoneableKafkaUser(KafkaUser resource) {
        super(resource, x -> x);
    }
}
