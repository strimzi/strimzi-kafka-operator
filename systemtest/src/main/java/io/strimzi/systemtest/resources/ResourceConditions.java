/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.fabric8.kubernetes.client.CustomResource;
import io.skodjob.testframe.resources.ResourceCondition;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.systemtest.enums.ConditionStatus;
import io.strimzi.systemtest.enums.CustomResourceStatus;

import java.util.Arrays;
import java.util.List;

public class ResourceConditions {

    public static <T extends CustomResource<? extends Spec, ? extends Status>> ResourceCondition<T> resourceIsReady() {
        return new ResourceCondition<>(resource ->
            checkMatchingConditions(resource, CustomResourceStatus.Ready, ConditionStatus.True),
            "readiness"
        );
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> ResourceCondition<T> resourceHasDesiredState(Enum<?> customResourceStatus) {
        return resourceHasDesiredState(customResourceStatus, ConditionStatus.True);
    }

    public static <T extends CustomResource<? extends Spec, ? extends Status>> ResourceCondition<T> resourceHasDesiredState(Enum<?> customResourceStatus, ConditionStatus conditionStatus) {
        return new ResourceCondition<>(resource ->
            checkMatchingConditions(resource, customResourceStatus, conditionStatus),
            String.format("%s state with condition status: %s", customResourceStatus.toString(), conditionStatus.toString())
        );
    }

    private static <T extends CustomResource<? extends Spec, ? extends Status>> boolean checkMatchingConditions(T resource, Enum<?> customResourceStatus, ConditionStatus conditionStatus) {
        return resource.getStatus()
            .getConditions()
            .stream()
            .anyMatch(condition -> condition.getType().equals(customResourceStatus.toString()) && condition.getStatus().equals(conditionStatus.toString()));
    }

    public static ResourceCondition<KafkaRebalance> kafkaRebalanceResourceReadiness() {
        List<String> readyConditions = Arrays.asList(KafkaRebalanceState.PendingProposal.toString(), KafkaRebalanceState.ProposalReady.toString(), KafkaRebalanceState.Ready.toString());
        return new ResourceCondition<>(rebalance -> {
            List<String> actualConditions = rebalance.getStatus().getConditions().stream().map(Condition::getType).toList();

            for (String condition: actualConditions) {
                if (readyConditions.contains(condition)) {
                    return true;
                }
            }
            return false;
        },
        "readiness");
    }
}
