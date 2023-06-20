/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.batching;

import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.operator.AdminApiOperator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsResult;
import org.apache.kafka.clients.admin.UserScramCredentialAlteration;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.ResourceNotFoundException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Micro-batching reconciler for patching SCRAM-SHA credentials using the Kafka Admin API.
 */
public class ScramShaCredentialsBatchReconciler extends AbstractBatchReconciler<AdminApiOperator.ReconcileRequest<UserScramCredentialAlteration, ReconcileResult<UserScramCredentialAlteration>>> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ScramShaCredentialsBatchReconciler.class);

    private final Admin adminClient;

    /**
     * Creates the SCRAM-SHA credentials micro-batching reconciler
     *
     * @param adminClient   Kafka Admin API client
     * @param queueSize     Maximal size of the batching queue
     * @param maxBatchSize  Maximal size of the batch
     * @param maxBatchTime  Maximal time for which the requests should be collected before a batch is sent
     */
    public ScramShaCredentialsBatchReconciler(Admin adminClient, int queueSize, int maxBatchSize, int maxBatchTime) {
        super("ScramShaCredentialsBatchReconciler", queueSize, maxBatchSize, maxBatchTime);
        this.adminClient = adminClient;
    }

    /**
     * Reconciles batch of requests to patch SCRAM-SHA credentials in Apache Kafka
     *
     * @param items Batch of requests which should be executed
     */
    @Override
    protected void reconcile(Collection<AdminApiOperator.ReconcileRequest<UserScramCredentialAlteration, ReconcileResult<UserScramCredentialAlteration>>> items) {
        List<UserScramCredentialAlteration> alterations = new ArrayList<>();
        items.forEach(req -> alterations.add(req.desired()));

        AlterUserScramCredentialsResult result = adminClient.alterUserScramCredentials(alterations);

        result.all()
                .toCompletionStage()
                .handleAsync((r, e) -> {
                    if (e != null)  {
                        LOGGER.warnOp("SCRAM-SHA credentials reconciliation failed", e);
                        items.forEach(req -> req.result().completeExceptionally(e));
                    } else {
                        Map<String, KafkaFuture<Void>> perItemResults = result.values();

                        items.forEach(req -> {
                            KafkaFuture<Void> itemResult = perItemResults.get(req.username());

                            if (itemResult.isCompletedExceptionally()) {
                                Exception reason = null;
                                try {
                                    itemResult.getNow(null);
                                    reason = new RuntimeException("The KafkaFuture failed without an exception");
                                } catch (Exception completionException) {
                                    reason = completionException;
                                } finally {
                                    if (reason instanceof ResourceNotFoundException
                                            && req.desired() instanceof UserScramCredentialDeletion) {
                                        LOGGER.debugCr(req.reconciliation(), "SCRAM credentials for user {} do not exist anymore", req.username());
                                        req.result().complete(ReconcileResult.noop(null));
                                    } else {
                                        LOGGER.warnCr(req.reconciliation(), "SCRAM-SHA credentials reconciliation for user {} failed", req.username(), reason);
                                        req.result().completeExceptionally(reason);
                                    }
                                }
                            } else if (itemResult.isCancelled()) {
                                LOGGER.warnCr(req.reconciliation(), "SCRAM-SHA credentials reconciliation for user {} was canceled", req.username());
                                req.result().completeExceptionally(new RuntimeException("SCRAM-SHA credentials reconciliation was canceled"));
                            } else if (itemResult.isDone()) {
                                LOGGER.debugCr(req.reconciliation(), "SCRAM-SHA credentials reconciliation for user {} succeeded", req.username());
                                req.result().complete(ReconcileResult.patched(req.desired()));
                            } else {
                                LOGGER.warnCr(req.reconciliation(), "SCRAM-SHA credentials reconciliation for user {} ended in unknown state", req.username());
                                req.result().completeExceptionally(new RuntimeException("SCRAM-SHA credentials reconciliation ended in unknown state"));
                            }
                        });
                    }

                    return null;
                });
    }
}
