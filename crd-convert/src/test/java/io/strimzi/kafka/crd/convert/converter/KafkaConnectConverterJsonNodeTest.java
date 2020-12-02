/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import io.strimzi.api.kafka.model.KafkaConnect;

class KafkaConnectConverterJsonNodeTest extends KafkaConnectConverterTest {
    @Override
    ExtConverters.ExtConverter<KafkaConnect> specableConverter() {
        return ExtConverters.nodeConverter(new KafkaConnectConverter());
    }
}