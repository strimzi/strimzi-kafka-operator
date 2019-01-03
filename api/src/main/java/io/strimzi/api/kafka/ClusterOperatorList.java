/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.client.CustomResourceList;
import io.strimzi.api.kafka.model.KafkaUser;

/**
 * A {@code CustomResourceList<ClusterOperator>} required for using Fabric8 CRD support.
 */
public class ClusterOperatorList extends CustomResourceList<KafkaUser> {
    private static final long serialVersionUID = 1L;
}
