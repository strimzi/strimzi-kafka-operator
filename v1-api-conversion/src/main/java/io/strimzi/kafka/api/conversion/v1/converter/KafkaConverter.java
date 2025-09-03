/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.Conversion;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.KafkaConversions;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.SharedConversions;

import java.util.List;

/**
 * Converter for Kafka resources
 */
public class KafkaConverter extends AbstractConverter<Kafka> {
    /**
     * Constructor
     */
    public KafkaConverter() {
        super(List.of(toVersionConversion(ApiVersion.V1BETA2, ApiVersion.V1,
                SharedConversions.enforceSpec(),
                KafkaConversions.enforceOauthServerAuthentication(),
                KafkaConversions.enforceSecretsInCustomServerAuthentication(),
                KafkaConversions.enforceKeycloakAuthorization(),
                KafkaConversions.enforceOpaAuthorization(),
                KafkaConversions.enforceKafkaResources(),
                Conversion.delete("/spec/zookeeper"),
                Conversion.delete("/spec/jmxTrans"),
                Conversion.delete("/spec/cruiseControl/tlsSidecar"),
                Conversion.delete("/spec/cruiseControl/template/tlsSidecarContainer"),
                Conversion.delete("/spec/cruiseControl/brokerCapacity/cpuUtilization"),
                Conversion.delete("/spec/cruiseControl/brokerCapacity/disk"),
                Conversion.delete("/spec/kafkaExporter/template/service"),
                Conversion.delete("/spec/entityOperator/tlsSidecar"),
                Conversion.delete("/spec/entityOperator/template/tlsSidecarContainer"),
                KafkaConversions.convertReconciliationIntervalInTO(),
                Conversion.delete("/spec/entityOperator/topicOperator/zookeeperSessionTimeoutSeconds"),
                Conversion.delete("/spec/entityOperator/topicOperator/topicMetadataMaxAttempts"),
                KafkaConversions.convertReconciliationIntervalInUO(),
                Conversion.delete("/spec/entityOperator/userOperator/zookeeperSessionTimeoutSeconds"),
                Conversion.delete("/spec/kafka/template/statefulset"),
                Conversion.delete("/spec/kafka/replicas"),
                Conversion.delete("/spec/kafka/storage"),
                Conversion.delete("/status/registeredNodeIds"),
                Conversion.delete("/status/kafkaMetadataState"),
                KafkaConversions.removeListenerTypeInStatus())));
    }

    @Override
    public Class<Kafka> crClass() {
        return Kafka.class;
    }
}

