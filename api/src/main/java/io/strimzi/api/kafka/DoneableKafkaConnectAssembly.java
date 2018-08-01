/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import io.strimzi.api.kafka.model.KafkaConnect;

/**
 * A {@code CustomResourceDoneable<KafkaConnectAssembly>} required for using Fabric8 CRD support.
 */
public class DoneableKafkaConnectAssembly extends CustomResourceDoneable<KafkaConnect> {
    public DoneableKafkaConnectAssembly(KafkaConnect resource, Function<KafkaConnect, KafkaConnect> function) {
        super(resource, function);
    }
    public DoneableKafkaConnectAssembly(KafkaConnect resource) {
        super(resource, x -> x);
    }
}
