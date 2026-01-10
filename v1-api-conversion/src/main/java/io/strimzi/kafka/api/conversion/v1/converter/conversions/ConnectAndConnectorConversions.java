/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import io.strimzi.api.kafka.model.common.ConnectorState;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpec;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorSpec;

import java.util.HashMap;
import java.util.Map;

/**
 * Class for holding the various conversions specific for the KafkaConnect and KafkaConnector API conversion
 */
@SuppressWarnings("deprecation")
public class ConnectAndConnectorConversions {
    private ConnectAndConnectorConversions() { }

    /**
     * Restructures the KafkaConnect resource
     *
     * @return  The conversion
     */
    public static Conversion<KafkaConnect> connectSpecRestructuring() {
        return Conversion.replace("/spec", new Conversion.DefaultConversionFunction<KafkaConnectSpec>() {
            @Override
            Class<KafkaConnectSpec> convertedType() {
                return KafkaConnectSpec.class;
            }

            @Override
            public KafkaConnectSpec apply(KafkaConnectSpec spec) {
                if (spec == null) {
                    return null;
                }

                Map<String, Object> config = new HashMap<>();
                if (spec.getConfig() != null) {
                    config = spec.getConfig();
                }

                if (spec.getGroupId() == null)  {
                    spec.setGroupId(config.getOrDefault("group.id", "connect-cluster").toString());
                }

                if (spec.getConfigStorageTopic() == null)  {
                    spec.setConfigStorageTopic(config.getOrDefault("config.storage.topic", "connect-cluster-configs").toString());
                }

                if (spec.getStatusStorageTopic() == null)  {
                    spec.setStatusStorageTopic(config.getOrDefault("status.storage.topic", "connect-cluster-status").toString());
                }

                if (spec.getOffsetStorageTopic() == null)  {
                    spec.setOffsetStorageTopic(config.getOrDefault("offset.storage.topic", "connect-cluster-offsets").toString());
                }

                config.remove("group.id");
                config.remove("config.storage.topic");
                config.remove("status.storage.topic");
                config.remove("offset.storage.topic");

                return spec;
            }
        });
    }

    /**
     * Converts the pause to state in KafkaConnector resources
     *
     * @return  The conversion
     */
    public static Conversion<KafkaConnector> connectorPauseToState() {
        return Conversion.replace("/spec", new Conversion.DefaultConversionFunction<KafkaConnectorSpec>() {
            @Override
            Class<KafkaConnectorSpec> convertedType() {
                return KafkaConnectorSpec.class;
            }

            @Override
            public KafkaConnectorSpec apply(KafkaConnectorSpec spec) {
                if (spec == null) {
                    return null;
                }

                if (spec.getPause() != null)    {
                    // We convert this only if `state` is not set. Otherwise, we just remove it.
                    if (spec.getState() == null && Boolean.TRUE.equals(spec.getPause()))    {
                        spec.setState(ConnectorState.PAUSED);
                    }

                    spec.setPause(null);
                }

                return spec;
            }
        });
    }
}
