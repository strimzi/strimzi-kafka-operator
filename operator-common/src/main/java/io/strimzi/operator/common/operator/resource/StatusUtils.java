/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.Status;
import io.vertx.core.AsyncResult;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;

public class StatusUtils {
    private static final String V1ALPHA1 = "kafka.strimzi.io/v1alpha1";

    public static Condition buildConditionFromReconciliationResult(AsyncResult<Void> reconciliationResult) {
        Condition readyCondition;
        if (reconciliationResult.succeeded()) {
            readyCondition = new ConditionBuilder()
                    .withNewLastTransitionTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(dateSupplier()))
                    .withNewType("Ready")
                    .withNewStatus("True")
                    .build();
        } else {
            readyCondition = new ConditionBuilder()
                    .withNewLastTransitionTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(dateSupplier()))
                    .withNewType("NotReady")
                    .withNewStatus("True")
                    .withNewReason(reconciliationResult.cause().getClass().getSimpleName())
                    .withNewMessage(reconciliationResult.cause().getMessage())
                    .build();
        }
        return readyCondition;
    }

    private static Date dateSupplier() {
        return new Date();
    }

    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, AsyncResult<Void> result) {
        if (resource.getMetadata().getGeneration() != null)    {
            status.setObservedGeneration(resource.getMetadata().getGeneration());
        }
        Condition readyCondition = StatusUtils.buildConditionFromReconciliationResult(result);
        status.setConditions(Collections.singletonList(readyCondition));
    }

    public static <R extends CustomResource> boolean isResourceV1alpha1(R resource) {
        return resource.getApiVersion() != null && resource.getApiVersion().equals(V1ALPHA1);
    }
}
