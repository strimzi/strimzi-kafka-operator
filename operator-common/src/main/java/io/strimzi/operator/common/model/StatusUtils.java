/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.common.model;

import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.api.kafka.model.connector.AutoRestartStatus;
import io.strimzi.api.kafka.model.connector.AutoRestartStatusBuilder;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Utility methods for working with status sections of custom resources
 */
public class StatusUtils {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(StatusUtils.class);

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
     * @param instant The date instant for which should the ISO 8601 timestamp be provided
     *
     * @return the current timestamp in ISO 8601 format, for example "2019-07-23T09:08:12.356Z".
     */
    public static String iso8601(Instant instant) {
        return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
    }

    /**
     * Returns an Instant from a string date in ISO 8601 format
     * @param date a string representing a date, for example "2019-07-23T09:08:12.356Z"
     * @return an Instant
     */
    public static Instant isoUtcDatetime(String date)  {
        return Instant.parse(date);
    }

    /**
     * Get an amount of minutes between a date and now
     * @param date the date to start from
     * @return long amount of time
     */
    public static long minutesDifferenceUntilNow(Instant date) {
        return ChronoUnit.MINUTES.between(date, ZonedDateTime.now(ZoneOffset.UTC));
    }

    /**
     * Creates condition from an exception
     *
     * @param type      Type of the condition
     * @param status    Status message
     * @param error     Exception used to build the condition
     *
     * @return  New condition based on the exception
     */
    public static Condition buildConditionFromException(String type, String status, Throwable error) {
        return buildCondition(type, status, error);
    }

    /**
     * Creates condition
     *
     * @param type      Type of the condition
     * @param status    Status message
     * @param error     Exception used to build the condition
     *
     * @return  New condition
     */
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

    /**
     * Creates a new warning type condition
     *
     * @param reason    Reason for the condition
     * @param message   Message of the condition
     *
     * @return  New warning type condition
     */
    public static Condition buildWarningCondition(String reason, String message) {
        return buildWarningCondition(reason, message, iso8601Now());
    }

    /**
     * Creates a new warning type condition
     *
     * @param reason            Reason for the condition
     * @param message           Message of the condition
     * @param transitionTime    Transition time
     *
     * @return  New warning type condition
     */
    public static Condition buildWarningCondition(String reason, String message, String transitionTime) {
        return new ConditionBuilder()
                .withLastTransitionTime(transitionTime)
                .withType("Warning")
                .withStatus("True")
                .withReason(reason)
                .withMessage(message)
                .build();
    }

    /**
     * Builds new rebalance condition
     *
     * @param type  Type of the condition
     *
     * @return  New rebalance condition
     */
    public static Condition buildRebalanceCondition(String type) {
        return new ConditionBuilder()
                .withLastTransitionTime(iso8601Now())
                .withType(type)
                .withStatus("True")
                .build();
    }

    /**
     * Sets a status with conditions and observed generation in a resource
     *
     * @param resource  Current custom resource
     * @param status    Desired status
     * @param error     Reconciliation error
     *
     * @param <R>   Type of the custom resource
     * @param <P>   Type of the custom resource spec
     * @param <S>   Type of the custom resource status
     */
    public static <R extends CustomResource<P, S>, P extends Spec, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, Throwable error) {
        setStatusConditionAndObservedGeneration(resource, status, error == null ? "Ready" : "NotReady", "True", error);
    }

    /**
     * Sets a status with conditions and observed generation in a resource
     *
     * @param resource          Current custom resource
     * @param status            Desired status
     * @param type              Type of the error
     * @param conditionStatus   Condition status
     * @param error             Reconciliation error
     *
     * @param <R>   Type of the custom resource
     * @param <P>   Type of the custom resource spec
     * @param <S>   Type of the custom resource status
     */
    public static <R extends CustomResource<P, S>, P extends Spec, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, String type, String conditionStatus, Throwable error) {
        if (resource.getMetadata().getGeneration() != null)    {
            status.setObservedGeneration(resource.getMetadata().getGeneration());
        }
        Condition readyCondition = StatusUtils.buildConditionFromException(type, conditionStatus, error);
        status.setConditions(Collections.singletonList(readyCondition));
    }

    /**
     * Sets a status with conditions and observed generation in a resource
     *
     * @param resource  Current custom resource
     * @param status    Desired status
     * @param type      Type of the error
     * @param error     Reconciliation error
     *
     * @param <R>   Type of the custom resource
     * @param <P>   Type of the custom resource spec
     * @param <S>   Type of the custom resource status
     */
    public static <R extends CustomResource<P, S>, P extends Spec, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, String type, Throwable error) {
        setStatusConditionAndObservedGeneration(resource, status, type, "True", error);
    }

    /**
     * Sets a status with conditions and observed generation in a resource
     *
     * @param resource  Current custom resource
     * @param status    Desired status
     * @param condition Condition to add to the status
     *
     * @param <R>   Type of the custom resource
     * @param <P>   Type of the custom resource spec
     * @param <S>   Type of the custom resource status
     */
    public static <R extends CustomResource<P, S>, P extends Spec, S extends Status> void setStatusConditionAndObservedGeneration(R resource, S status, Condition condition) {
        if (resource.getMetadata().getGeneration() != null)    {
            status.setObservedGeneration(resource.getMetadata().getGeneration());
        }
        status.setConditions(Collections.singletonList(condition));
    }

    /**
     * @return  Creates a paused reconciliation condition
     */
    public static Condition getPausedCondition() {
        return new ConditionBuilder()
                .withLastTransitionTime(StatusUtils.iso8601Now())
                .withType("ReconciliationPaused")
                .withStatus("True")
                .build();
    }

    /**
     * Validate the Custom Resource. This should log at the WARN level (rather than throwing) if the resource can safely
     * be reconciled (e.g. it merely using deprecated API).
     *
     * @param <T>               Custom Resource type
     * @param <P>               Custom Resource spec type
     * @param <S>               Custom Resource status type
     *
     * @param reconciliation    The reconciliation
     * @param resource          The custom resource
     *
     * @throws InvalidResourceException if the resource cannot be safely reconciled.
     *
     * @return set of conditions
     */
    public static <T extends CustomResource<P, S>, P extends Spec, S extends Status> Set<Condition> validate(Reconciliation reconciliation, T resource) {
        if (resource != null) {
            Set<Condition> warningConditions = new LinkedHashSet<>(0); // LinkedHashSet is used to maintain ordering

            ResourceVisitor.visit(reconciliation, resource, new ValidationVisitor(resource, LOGGER, warningConditions));

            return warningConditions;
        }

        return Collections.emptySet();
    }

    /**
     * Adds additional conditions to te status (this expects)
     *
     * @param status        The Status instance where additional conditions should be added
     * @param conditions    The Set with the new conditions
     */
    public static void addConditionsToStatus(Status status, Set<Condition> conditions)   {
        if (status != null)  {
            status.addConditions(conditions);
        }
    }

    /**
     * Create an AutoRestartStatus if null and
     * Increment count and set the last restart timestamp of an AutoRestartStatus
     *
     * @param autoRestart   The AutoRestart status or null
     * @return the AutoRestart status updated or a new one if it was null
     */
    public static AutoRestartStatus incrementAutoRestartStatus(AutoRestartStatus autoRestart)  {
        AutoRestartStatus newStatus;

        if (autoRestart == null)  {
            newStatus = new AutoRestartStatus();
            newStatus.setCount(1);
        } else {
            newStatus = new AutoRestartStatusBuilder(autoRestart).build();
            newStatus.setCount(autoRestart.getCount() + 1);
        }

        newStatus.setLastRestartTimestamp(iso8601Now());

        return newStatus;
    }
}
