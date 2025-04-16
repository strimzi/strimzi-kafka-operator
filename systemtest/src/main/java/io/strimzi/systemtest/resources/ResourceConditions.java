/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.fabric8.kubernetes.client.CustomResource;
import io.skodjob.testframe.resources.ResourceCondition;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.systemtest.enums.ConditionStatus;
import io.strimzi.systemtest.enums.CustomResourceStatus;

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
            checkMatchingConditions(resource, customResourceStatus, ConditionStatus.True),
            String.format("has %s state", customResourceStatus.toString())
        );
    }

    private static <T extends CustomResource<? extends Spec, ? extends Status>> boolean checkMatchingConditions(T resource, Enum<?> customResourceStatus, ConditionStatus conditionStatus) {
        return resource.getStatus()
            .getConditions()
            .stream()
            .anyMatch(condition -> condition.getType().equals(customResourceStatus.toString()) && condition.getStatus().equals(conditionStatus.toString()));
    }
}
