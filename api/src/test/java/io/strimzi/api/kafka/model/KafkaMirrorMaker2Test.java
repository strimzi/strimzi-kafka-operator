/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

/**
 * This test ensures we get a correct tree of POJOs when reading a JSON/YAML `KafkaMirrorMaker2` resource.
 */
public class KafkaMirrorMaker2Test extends AbstractCrdTest<KafkaMirrorMaker2> {

    public KafkaMirrorMaker2Test() {
        super(KafkaMirrorMaker2.class);
    }
}
