/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import io.strimzi.api.kafka.model.KafkaConnectAssembly;

/**
 * A {@code CustomResourceDoneable<KafkaConnectAssembly>} required for using Fabric8 CRD support.
 */
public class DoneableKafkaConnectAssembly extends CustomResourceDoneable<KafkaConnectAssembly> {
    public DoneableKafkaConnectAssembly(KafkaConnectAssembly resource, Function<KafkaConnectAssembly, KafkaConnectAssembly> function) {
        super(resource, function);
    }
    public DoneableKafkaConnectAssembly(KafkaConnectAssembly resource) {
        super(resource, x -> x);
    }
}
