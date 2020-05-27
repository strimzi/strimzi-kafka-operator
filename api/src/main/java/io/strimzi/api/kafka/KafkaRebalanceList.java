/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.client.CustomResourceList;
import io.strimzi.api.kafka.model.KafkaRebalance;

/**
 * A {@code CustomResourceList<KafkaRebalance>} required for using Fabric8 CRD support.
 */
public class KafkaRebalanceList extends CustomResourceList<KafkaRebalance> {
    private static final long serialVersionUID = 1L;
}
