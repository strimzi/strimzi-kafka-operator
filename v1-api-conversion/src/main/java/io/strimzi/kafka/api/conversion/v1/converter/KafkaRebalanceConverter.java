/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.SharedConversions;

import java.util.List;

/**
 * Converter for KafkaRebalance resources
 */
public class KafkaRebalanceConverter extends AbstractConverter<KafkaRebalance> {
    /**
     * Constructor
     */
    public KafkaRebalanceConverter() {
        super(List.of(toVersionConversion(ApiVersion.V1BETA2, ApiVersion.V1,
                        SharedConversions.enforceSpec())));
    }

    @Override
    public Class<KafkaRebalance> crClass() {
        return KafkaRebalance.class;
    }
}
