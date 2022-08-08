/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.KafkaUserStatus;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.controller.AbstractControllerLoop;
import io.strimzi.operator.common.controller.ControllerQueue;
import io.strimzi.operator.common.controller.ReconciliationLockManager;
import io.strimzi.operator.common.metrics.ControllerMetricsHolder;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.operator.KafkaUserOperator;

import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * User controller loop is responsible for reconciling the KafkaUser and the secrets and Kafka settings which belong to it.
 */
public class UserControllerLoop extends AbstractControllerLoop {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(UserControllerLoop.class);

    private final KubernetesClient client;
    private final Lister<KafkaUser> userLister;
    private final Lister<Secret> secretLister;
    private final KafkaUserOperator userOperator;
    private final ControllerMetricsHolder metrics;

    private final String secretPrefix;
    private final long operationTimeoutMs;

    /**
     * Constructor of the UserController reconciliation loop
     *
     * @param name                  Name of the reconciliation loop. It should identify the resource it reconciles, and
     *                              possible the namespace in which it reconciles it or the number of the loop if more
     *                              than one is running in parallel.
     * @param workQueue             ControllerQueue from which the reconciliation events should be taken
     * @param lockManager           LockManager which is used to avoid the same resource being reconciled in multiple loops in parallel
     * @param scheduledExecutor     Scheduled executor service which will be passed to the AbstractControllerLoop and
     *                              used to run the progress warnings
     * @param client                The Kubernetes client
     * @param userLister            The KafkaUser resource lister for getting the resources
     * @param secretLister          The Secret lister for getting the secrets
     * @param userOperator          The KafkaUserOperator which has the logic for updating the Kubernetes or Kafka resources
     * @param metrics               The metrics holder for providing metrics about the reconciliation
     * @param config                The User Operator config
     */
    public UserControllerLoop(
            String name,
            ControllerQueue workQueue,
            ReconciliationLockManager lockManager,
            ScheduledExecutorService scheduledExecutor,
            KubernetesClient client,
            Lister<KafkaUser> userLister,
            Lister<Secret> secretLister,
            KafkaUserOperator userOperator,
            ControllerMetricsHolder metrics,
            UserOperatorConfig config
    ) {
        super(name, workQueue, lockManager, scheduledExecutor);

        this.client = client;
        this.userLister = userLister;
        this.secretLister = secretLister;
        this.userOperator = userOperator;
        this.metrics = metrics;

        this.secretPrefix = config.getSecretPrefix();
        this.operationTimeoutMs = config.getOperationTimeoutMs();
    }

    /**
     * The main reconciliation logic which handles the reconciliations.
     *
     * @param reconciliation    Reconciliation identifier used for logging
     */
    @Override
    protected void reconcile(Reconciliation reconciliation) {
        LOGGER.infoCr(reconciliation, "{} will be reconciled", reconciliation.kind());

        KafkaUser user = userLister.namespace(reconciliation.namespace()).get(reconciliation.name());

        if (user != null && Annotations.isReconciliationPausedWithAnnotation(user)) {
            // Reconciliation is paused => we make sure the status is up-to-date but don't do anything
            LOGGER.infoCr(reconciliation, "Reconciliation of {} {} in namespace {} is paused", reconciliation.kind(), reconciliation.name(), reconciliation.namespace());
            KafkaUserStatus status = UserControllerUtils.pausedStatus(reconciliation, user);
            metrics().successfulReconciliationsCounter(reconciliation.namespace()).increment();
            maybeUpdateStatus(reconciliation, user, status);
        } else {
            // Resource is not paused or is null (and we should trigger deletion) => we should proceed with reconciliation
            CompletionStage<KafkaUserStatus> reconciliationResult = userOperator
                    .reconcile(reconciliation, user, secretLister.namespace(reconciliation.namespace()).get(KafkaUserModel.getSecretName(secretPrefix, reconciliation.name())));

            try {
                KafkaUserStatus status = new KafkaUserStatus();
                Set<Condition> unknownAndDeprecatedConditions = StatusUtils.validate(reconciliation, user);

                try {
                    status = reconciliationResult.toCompletableFuture().get(operationTimeoutMs, TimeUnit.MILLISECONDS);
                    LOGGER.infoCr(reconciliation, "reconciled");
                    metrics().successfulReconciliationsCounter(reconciliation.namespace()).increment();
                } catch (ExecutionException | InterruptedException | TimeoutException | CancellationException e) {
                    // The reconciliation failed (these are returned from the get(...) call)
                    LOGGER.errorCr(reconciliation, "{} {} in namespace {} reconciliation failed", reconciliation.kind(), reconciliation.name(), reconciliation.namespace(), e);
                    metrics().failedReconciliationsCounter(reconciliation.namespace()).increment();

                    if (user != null) {
                        StatusUtils.setStatusConditionAndObservedGeneration(user, status, e);
                    }
                } finally {
                    // Update the status if the user exists
                    if (user != null) {
                        StatusUtils.addConditionsToStatus(status, unknownAndDeprecatedConditions);
                        maybeUpdateStatus(reconciliation, user, status);
                    }
                }
            } catch (Throwable t) {
                // Updating status failed
                LOGGER.errorCr(reconciliation, "Failed to update status for {} {} in namespace {}", reconciliation.kind(), reconciliation.name(), reconciliation.namespace(), t);
            }
        }
    }

    /**
     * Updates the status of the KafkaUser. The status will be updated only when it changed since last time.
     *
     * @param reconciliation    Reconciliation in which this is executed
     * @param kafkaUser         Original KafkaUser with the current status
     * @param desiredStatus     The desired status which should be set if it differs
     */
    private void maybeUpdateStatus(Reconciliation reconciliation, KafkaUser kafkaUser, KafkaUserStatus desiredStatus) {
        // KafkaUser or desiredStatus being null means deletion => no status to update
        if (kafkaUser != null && desiredStatus != null) {
            if (!new StatusDiff(kafkaUser.getStatus(), desiredStatus).isEmpty())  {
                try {
                    LOGGER.debugCr(reconciliation, "Updating status of {} {} in namespace {}", reconciliation.kind(), reconciliation.name(), reconciliation.namespace());
                    KafkaUser latestKafkaUser = userLister.namespace(reconciliation.namespace()).get(reconciliation.name());
                    if (latestKafkaUser != null) {
                        KafkaUser updateKafkaUser = new KafkaUserBuilder(latestKafkaUser)
                                .withStatus(desiredStatus)
                                .build();

                        Crds.kafkaUserOperation(client).inNamespace(reconciliation.namespace()).resource(updateKafkaUser).replaceStatus();
                    }
                } catch (KubernetesClientException e)   {
                    if (e.getCode() == 409) {
                        LOGGER.debugCr(reconciliation, "{} {} in namespace {} changed while trying to update status", reconciliation.kind(), reconciliation.name(), reconciliation.namespace());
                    } else if (e.getCode() == 404) {
                        LOGGER.debugCr(reconciliation, "{} {} in namespace {} was deleted while trying to update status", reconciliation.kind(), reconciliation.name(), reconciliation.namespace());
                    } else {
                        LOGGER.errorCr(reconciliation, "Failed to update status of {} {} in namespace {}", reconciliation.kind(), reconciliation.name(), reconciliation.namespace(), e);
                    }
                }
            }
        }
    }

    @Override
    protected ControllerMetricsHolder metrics() {
        return metrics;
    }
}
