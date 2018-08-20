/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;

/**
 * A {@code CustomResourceDoneable<KafkaConnectS2IAssembly>} required for using Fabric8 CRD support.
 */
public class DoneableKafkaConnectS2I extends CustomResourceDoneable<KafkaConnectS2I> {
    public DoneableKafkaConnectS2I(KafkaConnectS2I resource, Function<KafkaConnectS2I, KafkaConnectS2I> function) {
        super(resource, function);
    }
    public DoneableKafkaConnectS2I(KafkaConnectS2I resource) {
        super(resource, x -> x);
    }
}
