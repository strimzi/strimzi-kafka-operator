/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import io.strimzi.api.kafka.model.Kafka;

class KafkaJsonNodeConverterTest extends KafkaConverterTestBase {
    @Override
    ExtConverters.ExtConverter<Kafka> kafkaConverter() {
        return ExtConverters.nodeConverter(new KafkaConverter());
    }
}