/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.Status;
import io.vertx.core.AsyncResult;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

public class StatusUtils {
    private static final String V1ALPHA1 = "kafka.strimzi.io/v1alpha1";

    /**
     * Returns the current timestamp in ISO 8601 format, for example "2019-07-23T09:08:12.356Z".
     * @return the current timestamp in ISO 8601 format, for example "2019-07-23T09:08:12.356Z".
     */
    public static String iso8601Now() {
        return ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
    }

    public static Condition buildConditionFromException(Throwable error) {
        Condition readyCondition;
        if (error == null) {
            readyCondition = new ConditionBuilder()
                    .withLastTransitionTime(iso8601Now())
                    .withType("Ready")
                    .withStatus("True")
                    .build();
        } else {
            readyCondition = new ConditionBuilder()
                    .withLastTransitionTime(iso8601Now())
                    .withType("NotReady")
                    .withStatus("True")
                    .withReason(error.getClass().getSimpleName())
                    .withMessage(error.getMessage())
                    .build();
        }
        return readyCondition;
    }

    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, AsyncResult<Void> result) {
        setStatusConditionAndObservedGeneration(resource, status, result.cause());
    }

    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, Throwable error) {
        if (resource.getMetadata().getGeneration() != null)    {
            status.setObservedGeneration(resource.getMetadata().getGeneration());
        }
        Condition readyCondition = StatusUtils.buildConditionFromException(error);
        status.setConditions(Collections.singletonList(readyCondition));
    }

    public static <R extends CustomResource> boolean isResourceV1alpha1(R resource) {
        return resource.getApiVersion() != null && resource.getApiVersion().equals(V1ALPHA1);
    }
}
