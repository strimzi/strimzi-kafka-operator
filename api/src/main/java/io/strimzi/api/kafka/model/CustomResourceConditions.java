/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceState;
import io.strimzi.api.kafka.model.status.Status;

import java.util.function.Predicate;

/**
 * CustomResourceConditions supplies convenience predicates that are intended to reduce boilerplate
 * code when using fabric8 clients to wait for a CRD to enter some anticipated state. This class provides
 * a generic implementation that can be used internally, public Predicates are exposed on the CRD
 * classes.
 * <p>
 * An example usage of one of the public predicates ({@link io.strimzi.api.kafka.model.Kafka#isReady() Kafka::isReady}):
 * <pre>
 * Crds.kafkaOperation(client).inNamespace(NAMESPACE).resource(kafka).create();
 * Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(NAME).waitUntilCondition(Kafka.isReady(), 5, TimeUnit.MINUTES);
 * </pre>
 * and to wait on a specific KafkaRebalance state using {@link io.strimzi.api.kafka.model.KafkaRebalance#isInState(KafkaRebalanceState) KafkaRebalance::isInState}:
 * <pre>
 * Crds.kafkaRebalanceOperation(client).inNamespace(NAMESPACE).withName(REBALANCE_NAME)
 *     .waitUntilCondition(KafkaRebalance.isInState(KafkaRebalanceState.ProposalReady), 5, TimeUnit.MINUTES);
 * </pre>
 */
class CustomResourceConditions {

    private CustomResourceConditions() {
    }

    static <Y extends Status, T extends CustomResource<?, Y>> Predicate<T> isReady() {
        return isLatestGenerationAndAnyConditionMatches("Ready", "True");
    }

    static <Y extends Status, T extends CustomResource<?, Y>> Predicate<T> isLatestGenerationAndAnyConditionMatches(String type, String status) {
        return isStatusLatestGenerationAndMatches(anyCondition(type, status));
    }

    private static <Y extends Status> Predicate<Y> anyCondition(String expectedType, String expectedStatus) {
        return (status) -> {
            if (status.getConditions() == null) {
                return false;
            } else {
                return status.getConditions().stream().anyMatch(
                        condition -> expectedType.equals(condition.getType()) && expectedStatus.equals(condition.getStatus())
                );
            }
        };
    }

    private static <Y extends Status, T extends CustomResource<?, Y>> Predicate<T> isStatusLatestGenerationAndMatches(Predicate<Y> predicate) {
        return (T resource) -> {
            if (resource.getStatus() == null) {
                return false;
            } else {
                boolean expectedGeneration = resource.getMetadata().getGeneration() == resource.getStatus().getObservedGeneration();
                return expectedGeneration && predicate.test(resource.getStatus());
            }
        };
    }
}
