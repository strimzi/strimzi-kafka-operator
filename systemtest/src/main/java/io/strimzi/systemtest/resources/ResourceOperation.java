/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.systemtest.Constants;

import java.time.Duration;

public class ResourceOperation {
    public static long getTimeoutForResourceReadiness(String kind) {
        long timeout;

        switch (kind) {
            case Kafka.RESOURCE_KIND:
                timeout = Duration.ofMinutes(14).toMillis();
                break;
            case KafkaConnect.RESOURCE_KIND:
            case KafkaConnectS2I.RESOURCE_KIND:
            case KafkaMirrorMaker2.RESOURCE_KIND:
            case Constants.DEPLOYMENT_CONFIG:
                timeout = Duration.ofMinutes(10).toMillis();
                break;
            case KafkaRebalance.RESOURCE_KIND:
            case KafkaMirrorMaker.RESOURCE_KIND:
            case KafkaBridge.RESOURCE_KIND:
            case KafkaConnector.RESOURCE_KIND:
            case Constants.STATEFUL_SET:
            case Constants.DEPLOYMENT:
                timeout = Duration.ofMinutes(8).toMillis();
                break;
            case Constants.KAFKA_CRUISE_CONTROL:
            case Constants.KAFKA_EXPORTER:
                timeout = Duration.ofMinutes(4).toMillis();
                break;
            default:
                timeout = Duration.ofMinutes(2).toMillis();
        }

        return timeout;
    }

    public static long getTimeoutForPodsReadiness(int expectPods) {
        return Duration.ofMinutes(4).toMillis() * expectPods;
    }
}
