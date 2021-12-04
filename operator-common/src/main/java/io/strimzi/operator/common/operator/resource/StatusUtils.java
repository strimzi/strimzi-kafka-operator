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
import java.util.Date;

public class StatusUtils {
    /**
     * Returns the current timestamp in ISO 8601 format, for example "2019-07-23T09:08:12.356Z".
     * 
     * @return the current timestamp in ISO 8601 format, for example "2019-07-23T09:08:12.356Z".
     */
    public static String iso8601Now() {
        return ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
    }

    /**
     * Returns the timestamp of the provided date in ISO 8601 format, for example "2019-07-23T09:08:12.356Z".
     *
     * @param date The date for which should the ISO 8601 timestamp be provided
     *
     * @return the current timestamp in ISO 8601 format, for example "2019-07-23T09:08:12.356Z".
     */
    public static String iso8601(Date date) {
        return ZonedDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
    }

    public static Condition buildConditionFromException(String type, String status, Throwable error) {
        return buildCondition(type, status, error);
    }

    public static Condition buildCondition(String type, String status, Throwable error) {
        Condition readyCondition;
        if (error == null) {
            readyCondition = new ConditionBuilder()
                    .withLastTransitionTime(iso8601Now())
                    .withType(type)
                    .withStatus(status)
                    .build();
        } else {
            readyCondition = new ConditionBuilder()
                    .withLastTransitionTime(iso8601Now())
                    .withType(type)
                    .withStatus(status)
                    .withReason(error.getClass().getSimpleName())
                    .withMessage(error.getMessage())
                    .build();
        }
        return readyCondition;
    }

    public static Condition buildWarningCondition(String reason, String message) {
        return buildWarningCondition(reason, message, iso8601Now());
    }

    public static Condition buildWarningCondition(String reason, String message, String transitionTime) {
        return new ConditionBuilder()
                .withLastTransitionTime(transitionTime)
                .withType("Warning")
                .withStatus("True")
                .withReason(reason)
                .withMessage(message)
                .build();
    }

    public static Condition buildRebalanceCondition(String type) {
        return new ConditionBuilder()
                .withLastTransitionTime(iso8601Now())
                .withType(type)
                .withStatus("True")
                .build();
    }

    @SuppressWarnings({ "rawtypes" })
    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, AsyncResult<Void> result) {
        setStatusConditionAndObservedGeneration(resource, status, result.cause());
    }

    @SuppressWarnings({ "rawtypes" })
    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, Throwable error) {
        setStatusConditionAndObservedGeneration(resource, status, error == null ? "Ready" : "NotReady", "True", error);
    }

    @SuppressWarnings({ "rawtypes" })
    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, String type, String conditionStatus, Throwable error) {
        if (resource.getMetadata().getGeneration() != null)    {
            status.setObservedGeneration(resource.getMetadata().getGeneration());
        }
        Condition readyCondition = StatusUtils.buildConditionFromException(type, conditionStatus, error);
        status.setConditions(Collections.singletonList(readyCondition));
    }

    @SuppressWarnings({ "rawtypes" })
    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, String type, Throwable error) {
        setStatusConditionAndObservedGeneration(resource, status, type, "True", error);
    }

    @SuppressWarnings({ "rawtypes" })
    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, String type, String conditionStatus) {
        if (resource.getMetadata().getGeneration() != null)    {
            status.setObservedGeneration(resource.getMetadata().getGeneration());
        }
        Condition condition = StatusUtils.buildCondition(type, conditionStatus, null);
        status.setConditions(Collections.singletonList(condition));
    }

    @SuppressWarnings({ "rawtypes" })
    public static <R extends CustomResource, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, String type) {
        setStatusConditionAndObservedGeneration(resource, status, type, "True");
    }

    public static Condition getPausedCondition() {
        return new ConditionBuilder()
                .withLastTransitionTime(StatusUtils.iso8601Now())
                .withType("ReconciliationPaused")
                .withStatus("True")
                .build();
    }
}
