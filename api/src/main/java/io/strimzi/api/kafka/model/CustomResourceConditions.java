/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.model.status.Status;

import java.util.function.Predicate;

public class CustomResourceConditions {

    /**
     * Returns a predicate that determines if a CustomResource is ready. A CustomResource is
     * ready if the observedGeneration of its status is equal to the generation of its metadata
     * and any of the conditions of its status has type:"Ready" and status:"True"
     *
     * @param <Y> the type of the custom resources Status
     * @param <T> the type of the custom resource
     * @return a predicate that checks if a CustomResource is ready
     */
    public static <Y extends Status, T extends CustomResource<?, Y>> Predicate<T> isReady() {
        return isStatusLatestGenerationAndMatches(anyCondition("Ready", "True"));
    }

    /**
     * Returns a predicate that determines if a KafkaRebalance is in a ProposalReady state. A KafkaRebalance is
     * in proposal ready if the observedGeneration of its status is equal to the generation of its metadata
     * and any of the conditions of its status has type:"ProposalReady" and status:"True"
     *
     * @return a predicate that checks if a KafkaRebalance is in ProposalReady state
     */
    public static Predicate<KafkaRebalance> isKafkaRebalanceProposalReady() {
        return isStatusLatestGenerationAndMatches(anyCondition("ProposalReady", "True"));
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
