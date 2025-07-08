/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.systemtest.TestConstants;

import java.time.Duration;

public class ResourceOperation {
    public static long getTimeoutForResourceReadiness() {
        return getTimeoutForResourceReadiness("default");
    }

    public static long getTimeoutForResourceReadiness(String kind) {
        return switch (kind) {
            case Kafka.RESOURCE_KIND -> Duration.ofMinutes(14).toMillis();
            case KafkaConnect.RESOURCE_KIND,
                 KafkaMirrorMaker2.RESOURCE_KIND,
                 TestConstants.DEPLOYMENT_CONFIG
                    -> Duration.ofMinutes(10).toMillis();
            case KafkaBridge.RESOURCE_KIND,
                 TestConstants.STATEFUL_SET,
                 StrimziPodSet.RESOURCE_KIND,
                 TestConstants.KAFKA_CRUISE_CONTROL_DEPLOYMENT,
                 TestConstants.KAFKA_EXPORTER_DEPLOYMENT,
                 TestConstants.DEPLOYMENT
                    -> Duration.ofMinutes(8).toMillis();
            case KafkaConnector.RESOURCE_KIND -> Duration.ofMinutes(7).toMillis();
            default -> Duration.ofMinutes(3).toMillis();
        };
    }

    public static long getTimeoutForKafkaRebalanceState(KafkaRebalanceState state) {
        return switch (state) {
            case ProposalReady,
                 Ready,
                 Rebalancing
                    -> Duration.ofMinutes(14).toMillis();
            default -> Duration.ofMinutes(6).toMillis();
        };
    }

    /**
     * timeoutForPodsOperation returns a reasonable timeout in milliseconds for a number of Pods in a quorum to roll on update,
     *  scale up or create
     */
    public static long timeoutForPodsOperation(int numberOfPods) {
        return Duration.ofMinutes(5).toMillis() * Math.max(1, numberOfPods);
    }

    public static long getTimeoutForResourceDeletion() {
        return getTimeoutForResourceDeletion("default");
    }

    public static long getTimeoutForResourceDeletion(String kind) {
        return switch (kind) {
            case Kafka.RESOURCE_KIND,
                 KafkaConnect.RESOURCE_KIND,
                 KafkaMirrorMaker2.RESOURCE_KIND,
                 KafkaBridge.RESOURCE_KIND,
                 TestConstants.STATEFUL_SET,
                 TestConstants.POD
                    -> Duration.ofMinutes(5).toMillis();
            default -> Duration.ofMinutes(2).toMillis();
        };
    }
}
