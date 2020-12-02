/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2ISpec;

// Deprecation is suppressed because of KafkaConnectS2I
@SuppressWarnings("deprecation")
public class KafkaConnectS2IConverter extends AbstractSpecableConverter<KafkaConnectS2I> {
    public KafkaConnectS2IConverter() {
        super(KafkaConnectS2ISpec.class);
    }

    @Override
    public Class<KafkaConnectS2I> crClass() {
        return KafkaConnectS2I.class;
    }
}
