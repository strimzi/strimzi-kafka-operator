/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.client.CustomResourceList;
import io.strimzi.api.kafka.model.KafkaConnectAssembly;

/**
 * A {@code CustomResourceList<KafkaConnectAssembly>} required for using Fabric8 CRD support.
 */
public class KafkaConnectAssemblyList extends CustomResourceList<KafkaConnectAssembly> {
    private static final long serialVersionUID = 1L;
}
