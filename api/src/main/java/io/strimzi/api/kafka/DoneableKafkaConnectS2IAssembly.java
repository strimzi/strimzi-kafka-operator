/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import io.strimzi.api.kafka.model.KafkaConnectS2IAssembly;

/**
 * A {@code CustomResourceDoneable<KafkaConnectS2IAssembly>} required for using Fabric8 CRD support.
 */
public class DoneableKafkaConnectS2IAssembly extends CustomResourceDoneable<KafkaConnectS2IAssembly> {
    public DoneableKafkaConnectS2IAssembly(KafkaConnectS2IAssembly resource, Function<KafkaConnectS2IAssembly, KafkaConnectS2IAssembly> function) {
        super(resource, function);
    }
    public DoneableKafkaConnectS2IAssembly(KafkaConnectS2IAssembly resource) {
        super(resource, x -> x);
    }
}
