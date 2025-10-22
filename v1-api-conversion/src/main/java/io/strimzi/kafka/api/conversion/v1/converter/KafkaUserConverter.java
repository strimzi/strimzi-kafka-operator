/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.SharedConversions;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.UserConversions;

import java.util.List;

/**
 * Converter for KafkaUser resources
 */
public class KafkaUserConverter extends AbstractConverter<KafkaUser> {
    /**
     * Constructor
     */
    public KafkaUserConverter() {
        super(List.of(
                toVersionConversion(ApiVersion.V1ALPHA1, ApiVersion.V1BETA1),
                toVersionConversion(ApiVersion.V1BETA1, ApiVersion.V1BETA2),
                toVersionConversion(ApiVersion.V1BETA2, ApiVersion.V1BETA2,
                        SharedConversions.enforceSpec(),
                        UserConversions.operationToOperations()),
                toVersionConversion(ApiVersion.V1BETA2, ApiVersion.V1,
                        SharedConversions.enforceSpec(),
                        UserConversions.operationToOperations()
                )
        ));
    }

    @Override
    public Class<KafkaUser> crClass() {
        return KafkaUser.class;
    }
}
