/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import io.strimzi.api.kafka.model.KafkaRebalance;

public class KafkaRebalanceConverter extends AbstractVersionableConverter<KafkaRebalance> {
    @Override
    public Class<KafkaRebalance> crClass() {
        return KafkaRebalance.class;
    }
}
