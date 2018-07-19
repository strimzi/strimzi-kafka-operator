/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.client.CustomResourceList;
import io.strimzi.api.kafka.model.KafkaTopic;

/**
 * A {@code CustomResourceList<Topic>} required for using Fabric8 CRD support.
 */
public class KafkaTopicList extends CustomResourceList<KafkaTopic> {
    private static final long serialVersionUID = 1L;
}
