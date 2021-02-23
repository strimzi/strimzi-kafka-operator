/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectSpec;

public class KafkaConnectConverter extends AbstractSpecableConverter<KafkaConnect> {
    public KafkaConnectConverter() {
        super(KafkaConnectSpec.class, "connect");
    }

    @Override
    public Class<KafkaConnect> crClass() {
        return KafkaConnect.class;
    }
}

