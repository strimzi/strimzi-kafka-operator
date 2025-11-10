/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.NodePoolConversions;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.SharedConversions;

import java.util.List;

/**
 * Converter for Kafka Node Pool resources
 */
public class KafkaNodePoolConverter extends AbstractConverter<KafkaNodePool> {
    /**
     * Constructor
     */
    public KafkaNodePoolConverter() {
        super(List.of(toVersionConversion(ApiVersion.V1BETA2, ApiVersion.V1,
                        SharedConversions.enforceSpec(),
                        NodePoolConversions.storageOverrides())));
    }

    @Override
    public Class<KafkaNodePool> crClass() {
        return KafkaNodePool.class;
    }
}
