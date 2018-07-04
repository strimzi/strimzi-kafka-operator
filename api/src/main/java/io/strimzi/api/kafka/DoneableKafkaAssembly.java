/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import io.strimzi.api.kafka.model.KafkaAssembly;

/**
 * A {@code CustomResourceDoneable<KafkaAssembly>} required for using Fabric8 CRD support.
 */
public class DoneableKafkaAssembly extends CustomResourceDoneable<KafkaAssembly> {
    public DoneableKafkaAssembly(KafkaAssembly resource, Function<KafkaAssembly, KafkaAssembly> function) {
        super(resource, function);
    }
    public DoneableKafkaAssembly(KafkaAssembly resource) {
        super(resource, x -> x);
    }
}
