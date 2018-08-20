/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;

/**
 * A {@code CustomResourceDoneable<KafkaConnectAssembly>} required for using Fabric8 CRD support.
 */
public class DoneableKafkaConnect extends CustomResourceDoneable<KafkaConnect> {
    public DoneableKafkaConnect(KafkaConnect resource, Function<KafkaConnect, KafkaConnect> function) {
        super(resource, function);
    }
    public DoneableKafkaConnect(KafkaConnect resource) {
        super(resource, x -> x);
    }
}
