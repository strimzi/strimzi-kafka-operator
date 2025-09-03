/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.SharedConversions;

import java.util.List;

/**
 * Converter for StrimziPodSet resources
 */
public class StrimziPodSetConverter extends AbstractConverter<StrimziPodSet> {
    /**
     * Constructor
     */
    public StrimziPodSetConverter() {
        super(List.of(
                toVersionConversion(ApiVersion.V1BETA2, ApiVersion.V1BETA2,
                        SharedConversions.enforceSpec()),
                toVersionConversion(ApiVersion.V1BETA2, ApiVersion.V1,
                        SharedConversions.enforceSpec())
        ));
    }

    @Override
    public Class<StrimziPodSet> crClass() {
        return StrimziPodSet.class;
    }
}
