/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.nodepool;

import io.strimzi.api.kafka.model.AbstractCrdTest;

/**
 * This class tests the encoding and decoding of the KafkaNodePool custom resource
 */
public class KafkaNodePoolTest extends AbstractCrdTest<KafkaNodePool> {
    public KafkaNodePoolTest() {
        super(KafkaNodePool.class);
    }
}
