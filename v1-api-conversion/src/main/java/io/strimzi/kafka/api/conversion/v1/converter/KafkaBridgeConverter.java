/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.BridgeMetricsConversion;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.SharedConversions;

import java.util.List;

/**
 * Converter for Kafka Bridge resources
 */
public class KafkaBridgeConverter extends AbstractConverter<KafkaBridge> {
    /**
     * Constructor
     */
    public KafkaBridgeConverter() {
        super(List.of(toVersionConversion(ApiVersion.V1BETA2, ApiVersion.V1,
                        SharedConversions.enforceSpec(),
                        SharedConversions.enforceOAuthClientAuthentication(),
                        SharedConversions.defaultReplicas(1),
                        SharedConversions.deleteJaegerTracing(),
                        new BridgeMetricsConversion())));
    }

    @Override
    public Class<KafkaBridge> crClass() {
        return KafkaBridge.class;
    }
}
