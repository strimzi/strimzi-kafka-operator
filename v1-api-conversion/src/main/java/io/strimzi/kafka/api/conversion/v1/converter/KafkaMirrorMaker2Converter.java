/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.Conversion;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.MirrorMaker2Conversions;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.SharedConversions;

import java.util.List;

/**
 * Converter for Kafka Mirror Maker 2 resources
 */
public class KafkaMirrorMaker2Converter extends AbstractConverter<KafkaMirrorMaker2> {
    /**
     * Constructor
     */
    public KafkaMirrorMaker2Converter() {
        super(List.of(toVersionConversion(ApiVersion.V1BETA2, ApiVersion.V1,
                        SharedConversions.enforceSpec(),
                        SharedConversions.enforceExternalConfiguration(),
                        MirrorMaker2Conversions.enforceOauth(),
                        SharedConversions.defaultReplicas(3),
                        Conversion.delete("/spec/template/deployment"),
                        SharedConversions.deleteJaegerTracing(),
                        MirrorMaker2Conversions.removeHeartbeatConnector(),
                        MirrorMaker2Conversions.pauseToState(),
                        MirrorMaker2Conversions.excludedFields(),
                        MirrorMaker2Conversions.mm2SpecRestructuring())
        ));
    }

    @Override
    public Class<KafkaMirrorMaker2> crClass() {
        return KafkaMirrorMaker2.class;
    }
}

