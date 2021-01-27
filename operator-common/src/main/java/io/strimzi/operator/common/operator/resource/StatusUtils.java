/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.status.KafkaCondition;
import io.strimzi.api.kafka.model.status.KafkaConditionBuilder;
import io.strimzi.api.kafka.model.status.Status;
import io.vertx.core.AsyncResult;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

public class StatusUtils {
    private static final String V1ALPHA1 = Constants.RESOURCE_GROUP_NAME + "/" + Constants.V1ALPHA1;

    /**
     * Returns the current timestamp in ISO 8601 format, for example "2019-07-23T09:08:12.356Z".
     * @return the current timestamp in ISO 8601 format, for example "2019-07-23T09:08:12.356Z".
     */
    public static String iso8601Now() {
        return ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
    }

    public static KafkaCondition buildConditionFromException(String type, String status, Throwable error) {
        return buildCondition(type, status, error);
    }

    public static KafkaCondition buildCondition(String type, String status, Throwable error) {
        KafkaCondition readyCondition;
        if (error == null) {
            readyCondition = new KafkaConditionBuilder()
                    .withLastTransitionTime(iso8601Now())
                    .withType(type)
                    .withStatus(status)
                    .build();
        } else {
            readyCondition = new KafkaConditionBuilder()
                    .withLastTransitionTime(iso8601Now())
                    .withType(type)
                    .withStatus(status)
                    .withReason(error.getClass().getSimpleName())
                    .withMessage(error.getMessage())
                    .build();
        }
        return readyCondition;
    }

    public static KafkaCondition buildWarningCondition(String reason, String message) {
        return buildWarningCondition(reason, message, iso8601Now());
    }

    public static KafkaCondition buildWarningCondition(String reason, String message, String transitionTime) {
        return new KafkaConditionBuilder()
                .withLastTransitionTime(transitionTime)
                .withType("Warning")
                .withStatus("True")
                .withReason(reason)
                .withMessage(message)
                .build();
    }

    public static KafkaCondition buildRebalanceCondition(String type) {
        return new KafkaConditionBuilder()
                .withLastTransitionTime(iso8601Now())
                .withType(type)
                .withStatus("True")
                .build();
    }

    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, AsyncResult<Void> result) {
        setStatusConditionAndObservedGeneration(resource, status, result.cause());
    }

    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, Throwable error) {
        setStatusConditionAndObservedGeneration(resource, status, error == null ? "Ready" : "NotReady", "True", error);
    }

    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, String type, String conditionStatus, Throwable error) {
        if (resource.getMetadata().getGeneration() != null)    {
            status.setObservedGeneration(resource.getMetadata().getGeneration());
        }
        KafkaCondition readyCondition = StatusUtils.buildConditionFromException(type, conditionStatus, error);
        status.setConditions(Collections.singletonList(readyCondition));
    }

    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, String type, Throwable error) {
        setStatusConditionAndObservedGeneration(resource, status, type, "True", error);
    }

    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, String type, String conditionStatus) {
        if (resource.getMetadata().getGeneration() != null)    {
            status.setObservedGeneration(resource.getMetadata().getGeneration());
        }
        KafkaCondition condition = StatusUtils.buildCondition(type, conditionStatus, null);
        status.setConditions(Collections.singletonList(condition));
    }

    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, String type) {
        setStatusConditionAndObservedGeneration(resource, status, type, "True");
    }

    public static <R extends CustomResource> boolean isResourceV1alpha1(R resource) {
        return resource.getApiVersion() != null && resource.getApiVersion().equals(V1ALPHA1);
    }
}
