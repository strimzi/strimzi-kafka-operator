/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.systemtest.TestConstants;

import java.time.Duration;

public class ResourceOperation {
    public static long getTimeoutForResourceReadiness() {
        return getTimeoutForResourceReadiness("default");
    }

    // Deprecation is suppressed because of KafkaMirrorMaker
    @SuppressWarnings("deprecation")
    public static long getTimeoutForResourceReadiness(String kind) {
        long timeout;

        switch (kind) {
            case Kafka.RESOURCE_KIND:
                timeout = Duration.ofMinutes(14).toMillis();
                break;
            case KafkaConnect.RESOURCE_KIND:
            case KafkaMirrorMaker2.RESOURCE_KIND:
            case TestConstants.DEPLOYMENT_CONFIG:
                timeout = Duration.ofMinutes(10).toMillis();
                break;
            case KafkaMirrorMaker.RESOURCE_KIND:
            case KafkaBridge.RESOURCE_KIND:
            case TestConstants.STATEFUL_SET:
            case StrimziPodSet.RESOURCE_KIND:
            case TestConstants.KAFKA_CRUISE_CONTROL_DEPLOYMENT:
            case TestConstants.KAFKA_EXPORTER_DEPLOYMENT:
            case TestConstants.DEPLOYMENT:
                timeout = Duration.ofMinutes(8).toMillis();
                break;
            case KafkaConnector.RESOURCE_KIND:
                timeout = Duration.ofMinutes(7).toMillis();
                break;
            default:
                timeout = Duration.ofMinutes(3).toMillis();
        }

        return timeout;
    }

    public static long getTimeoutForKafkaRebalanceState(KafkaRebalanceState state) {
        long timeout;
        switch (state) {
            case ProposalReady:
            case Ready:
            case Rebalancing:
                timeout = Duration.ofMinutes(14).toMillis();
                break;
            default:
                timeout = Duration.ofMinutes(6).toMillis();
        }

        return timeout;
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

    // Deprecation is suppressed because of KafkaMirrorMaker
    @SuppressWarnings("deprecation")
    public static long getTimeoutForResourceDeletion(String kind) {
        long timeout;

        switch (kind) {
            case Kafka.RESOURCE_KIND:
            case KafkaConnect.RESOURCE_KIND:
            case KafkaMirrorMaker2.RESOURCE_KIND:
            case KafkaMirrorMaker.RESOURCE_KIND:
            case KafkaBridge.RESOURCE_KIND:
            case TestConstants.STATEFUL_SET:
            case TestConstants.POD:
                timeout = Duration.ofMinutes(5).toMillis();
                break;
            default:
                timeout = Duration.ofMinutes(3).toMillis();
        }

        return timeout;
    }
}
