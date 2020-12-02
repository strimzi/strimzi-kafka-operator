/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import io.strimzi.api.kafka.model.KafkaConnectS2I;

class KafkaConnectS2IConverterJsonNodeTest extends KafkaConnectS2IConverterTest {
    @Override
    ExtConverters.ExtConverter<KafkaConnectS2I> specableConverter() {
        return ExtConverters.nodeConverter(new KafkaConnectS2IConverter());
    }
}