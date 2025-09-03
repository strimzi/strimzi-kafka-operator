/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.SharedConversions;

import java.util.List;

/**
 * Converter for KafkaTopic resources
 */
public class KafkaTopicConverter extends AbstractConverter<KafkaTopic> {
    /**
     * Constructor
     */
    public KafkaTopicConverter() {
        super(List.of(
                toVersionConversion(ApiVersion.V1ALPHA1, ApiVersion.V1BETA1),
                toVersionConversion(ApiVersion.V1BETA1, ApiVersion.V1BETA2),
                toVersionConversion(ApiVersion.V1BETA2, ApiVersion.V1,
                        SharedConversions.enforceSpec())
        ));
    }

    @Override
    public Class<KafkaTopic> crClass() {
        return KafkaTopic.class;
    }
}
