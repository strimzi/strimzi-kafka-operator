/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.ConnectAndConnectorConversions;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.Conversion;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.SharedConversions;

import java.util.List;

/**
 * Converter for Kafka Connect resources
 */
public class KafkaConnectConverter extends AbstractConverter<KafkaConnect> {
    /**
     * Constructor
     */
    public KafkaConnectConverter() {
        super(List.of(toVersionConversion(ApiVersion.V1BETA2, ApiVersion.V1,
                        SharedConversions.enforceSpec(),
                        SharedConversions.enforceOAuthClientAuthentication(),
                        SharedConversions.enforceExternalConfiguration(),
                        SharedConversions.defaultReplicas(3),
                        Conversion.move("/spec/build/output/additionalKanikoOptions", "/spec/build/output/additionalBuildOptions"),
                        Conversion.delete("/spec/template/deployment"),
                        SharedConversions.deleteJaegerTracing(),
                        ConnectAndConnectorConversions.connectSpecRestructuring())
        ));
    }

    @Override
    public Class<KafkaConnect> crClass() {
        return KafkaConnect.class;
    }
}

