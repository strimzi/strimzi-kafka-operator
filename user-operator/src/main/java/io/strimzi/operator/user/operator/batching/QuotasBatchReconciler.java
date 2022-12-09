/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.batching;

import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.operator.AdminApiOperator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Micro-batching reconciler for patching Kafka quotas using the Kafka Admin API.
 */
public class QuotasBatchReconciler extends AbstractBatchReconciler<AdminApiOperator.ReconcileRequest<ClientQuotaAlteration, ReconcileResult<ClientQuotaAlteration>>> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(QuotasBatchReconciler.class);

    private final Admin adminClient;

    /**
     * Creates the Quotas micro-batching reconciler
     *
     * @param adminClient   Kafka Admin API client
     * @param queueSize     Maximal size of the batching queue
     * @param maxBatchSize  Maximal size of the batch
     * @param maxBatchTime  Maximal time for which the requests should be collected before a batch is sent
     */
    public QuotasBatchReconciler(Admin adminClient, int queueSize, int maxBatchSize, int maxBatchTime) {
        super("QuotasBatchReconciler", queueSize, maxBatchSize, maxBatchTime);
        this.adminClient = adminClient;
    }

    /**
     * Reconciles batch of requests to Patch quotas in Apache Kafka
     *
     * @param items Batch of requests which should be executed
     */
    @Override
    protected void reconcile(Collection<AdminApiOperator.ReconcileRequest<ClientQuotaAlteration, ReconcileResult<ClientQuotaAlteration>>> items) {
        List<ClientQuotaAlteration> quotas = new ArrayList<>();
        items.forEach(req -> quotas.add(req.desired()));

        AlterClientQuotasResult result = adminClient.alterClientQuotas(quotas);

        result.all()
                .toCompletionStage()
                .handleAsync((r, e) -> {
                    if (e != null)  {
                        LOGGER.warnOp("Quotas reconciliation failed", e);
                        items.forEach(req -> req.result().completeExceptionally(e));
                    } else {
                        Map<ClientQuotaEntity, KafkaFuture<Void>> perItemResults = result.values();

                        items.forEach(req -> {
                            KafkaFuture<Void> itemResult = perItemResults.get(new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, req.username())));

                            if (itemResult.isCompletedExceptionally()) {
                                Exception reason = null;
                                try {
                                    itemResult.getNow(null);
                                    reason = new RuntimeException("The KafkaFuture failed without an exception");
                                } catch (Exception completionException) {
                                    reason = completionException;
                                } finally {
                                    LOGGER.warnCr(req.reconciliation(), "Quotas reconciliation for user {} failed", req.username(), reason);
                                    req.result().completeExceptionally(reason);
                                }
                            } else if (itemResult.isCancelled()) {
                                LOGGER.warnCr(req.reconciliation(), "Quotas reconciliation for user {} was canceled", req.username());
                                req.result().completeExceptionally(new RuntimeException("Quotas reconciliation was canceled"));
                            } else if (itemResult.isDone()) {
                                LOGGER.debugCr(req.reconciliation(), "Quotas reconciliation for user {} succeeded", req.username());
                                req.result().complete(ReconcileResult.patched(req.desired()));
                            } else {
                                LOGGER.warnCr(req.reconciliation(), "Quotas reconciliation for user {} ended in unknown state", req.username());
                                req.result().completeExceptionally(new RuntimeException("Quotas reconciliation ended in unknown state"));
                            }
                        });
                    }

                    return null;
                });
    }
}
