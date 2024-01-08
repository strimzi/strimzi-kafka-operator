/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.bridge;

import io.strimzi.api.kafka.model.AbstractCrdTest;

/**
 * The purpose of this test is to ensure:
 *
 * 1. we get a correct tree of POJOs when reading a JSON/YAML `KafkaBridge` resource.
 */
public class KafkaBridgeTest extends AbstractCrdTest<KafkaBridge> {

    public KafkaBridgeTest() {
        super(KafkaBridge.class);
    }
}
