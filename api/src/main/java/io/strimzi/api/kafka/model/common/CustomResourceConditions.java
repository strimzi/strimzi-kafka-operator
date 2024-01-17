/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;

import java.util.function.Predicate;

/**
 * CustomResourceConditions supplies convenience predicates that are intended to reduce boilerplate
 * code when using fabric8 clients to wait for a CRD to enter some anticipated state. This class provides
 * a generic implementation that can be used internally, public Predicates are exposed on the CRD
 * classes.
 * <p>
 * An example usage of one of the public predicates ({@link Kafka#isReady() Kafka::isReady}):
 * <pre>
 * Crds.kafkaOperation(client).inNamespace(NAMESPACE).resource(kafka).create();
 * Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(NAME).waitUntilCondition(Kafka.isReady(), 5, TimeUnit.MINUTES);
 * </pre>
 * and to wait on a specific KafkaRebalance state using {@link KafkaRebalance#isInState(KafkaRebalanceState) KafkaRebalance::isInState}:
 * <pre>
 * Crds.kafkaRebalanceOperation(client).inNamespace(NAMESPACE).withName(REBALANCE_NAME)
 *     .waitUntilCondition(KafkaRebalance.isInState(KafkaRebalanceState.ProposalReady), 5, TimeUnit.MINUTES);
 * </pre>
 */
public class CustomResourceConditions {

    private CustomResourceConditions() {
    }

    /**
     * Returns a predicate that determines if CRD is ready. A CRD is
     * ready if the observedGeneration of its status is equal to the generation of its metadata
     * and any of the conditions of its status has type:"Ready" and status:"True"
     *
     * @return a predicate that checks if a CRD is ready
     */
    public static <Y extends Status, T extends CustomResource<?, Y>> Predicate<T> isReady() {
        return isLatestGenerationAndAnyConditionMatches("Ready", "True");
    }

    /**
     * Checks if the observedGeneration of its status is equal to the generation of its metadata
     * and any of the conditions of its status contains given type and status parameter.
     *
     * @param <Y>    Type of status
     * @param <T>    Type of Custom resource
     * @param type   State of the Custom resource
     * @param status Status of the Custom resource
     *
     * @return a predicate based on the check
     */
    public static <Y extends Status, T extends CustomResource<?, Y>> Predicate<T> isLatestGenerationAndAnyConditionMatches(String type, String status) {
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
