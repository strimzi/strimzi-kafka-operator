/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

/**
 * The purpose of this test is to ensure:
 *
 * 1. we get a correct tree of POJOs when reading a JSON/YAML `Kafka` resource.
 */
public class KafkaTopicTest extends AbstractCrdTest<KafkaTopic, KafkaTopicBuilder> {

    public KafkaTopicTest() {
        super(KafkaTopic.class, KafkaTopicBuilder.class);
    }

}
