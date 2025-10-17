/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.ConnectAndConnectorConversions;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.SharedConversions;

import java.util.List;

/**
 * Converter for Kafka Connector resources
 */
public class KafkaConnectorConverter extends AbstractConverter<KafkaConnector> {
    /**
     * Constructor
     */
    public KafkaConnectorConverter() {
        super(List.of(toVersionConversion(ApiVersion.V1BETA2, ApiVersion.V1,
                        SharedConversions.enforceSpec(),
                        ConnectAndConnectorConversions.connectorPauseToState())));
    }

    @Override
    public Class<KafkaConnector> crClass() {
        return KafkaConnector.class;
    }
}
