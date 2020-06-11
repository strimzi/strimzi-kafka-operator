/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.enums;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;

import java.time.Duration;

import io.strimzi.systemtest.Constants;

public enum ResourceReadiness {
    KAFKA,
    KAFKA_CONNECT,
    KAFKA_CONNECT_S2I,
    KAFKA_MIRROR_MAKER,
    KAFKA_MIRROR_MAKER2,
    KAFKA_BRIDGE,
    KAFKA_CONNECTOR,
    KAFKA_TOPIC,
    KAFKA_USER,
    SERVICE,
    SECRET,
    DEPLOYMENT;

    private static ResourceReadiness getKind(String kind) {
        switch (kind) {
            case Kafka.RESOURCE_KIND:
                return KAFKA;
            case KafkaConnect.RESOURCE_KIND:
                return KAFKA_CONNECT;
            case KafkaConnectS2I.RESOURCE_KIND:
                return KAFKA_CONNECT_S2I;
            case KafkaMirrorMaker.RESOURCE_KIND:
                return KAFKA_MIRROR_MAKER;
            case KafkaMirrorMaker2.RESOURCE_KIND:
                return KAFKA_MIRROR_MAKER2;
            case KafkaBridge.RESOURCE_KIND:
                return KAFKA_BRIDGE;
            case KafkaConnector.RESOURCE_KIND:
                return KAFKA_CONNECTOR;
            case KafkaTopic.RESOURCE_KIND:
                return KAFKA_TOPIC;
            case KafkaUser.RESOURCE_KIND:
                return KAFKA_USER;
            case Constants.SERVICE:
                return SERVICE;
            case "Secret":
                return SECRET;
            default:
                return DEPLOYMENT;
        }
    }

    public static long getTimeoutForResourceReadiness(String kind) {
        long timeout = 0;

        switch (getKind(kind)) {
            case KAFKA:
                timeout = Duration.ofMinutes(14).toMillis();
                break;
            case KAFKA_CONNECT:
            case KAFKA_CONNECT_S2I:
            case KAFKA_MIRROR_MAKER2:
                timeout = Duration.ofMinutes(10).toMillis();
                break;
            case KAFKA_MIRROR_MAKER:
            case KAFKA_BRIDGE:
            case KAFKA_CONNECTOR:
            case DEPLOYMENT:
                timeout = Duration.ofMinutes(8).toMillis();
                break;
            default:
                timeout = Duration.ofMinutes(2).toMillis();
        }

        return timeout;
    }
}



