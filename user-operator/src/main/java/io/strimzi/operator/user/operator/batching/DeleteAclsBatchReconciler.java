/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.batching;

import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.operator.AdminApiOperator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBindingFilter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Micro-batching reconciler for deleting ACL rules using the Kafka Admin API.
 */
public class DeleteAclsBatchReconciler extends AbstractBatchReconciler<AdminApiOperator.ReconcileRequest<Collection<AclBindingFilter>, ReconcileResult<Collection<AclBindingFilter>>>> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(DeleteAclsBatchReconciler.class);

    private final Admin adminClient;

    /**
     * Creates the micro-batching reconciler for deleting ACL rules
     *
     * @param adminClient   Kafka Admin API client
     * @param queueSize     Maximal size of the batching queue
     * @param maxBatchSize  Maximal size of the batch
     * @param maxBatchTime  Maximal time for which the requests should be collected before a batch is sent
     */
    public DeleteAclsBatchReconciler(Admin adminClient, int queueSize, int maxBatchSize, int maxBatchTime) {
        super("DeleteAclsBatchReconciler", queueSize, maxBatchSize, maxBatchTime);
        this.adminClient = adminClient;
    }

    /**
     * Reconciles batch of requests to delete ACL rules in Apache Kafka
     *
     * @param items Batch of requests which should be executed
     */
    @Override
    protected void reconcile(Collection<AdminApiOperator.ReconcileRequest<Collection<AclBindingFilter>, ReconcileResult<Collection<AclBindingFilter>>>> items) {
        List<AclBindingFilter> aclFilters = new ArrayList<>();
        items.forEach(req -> aclFilters.addAll(req.desired()));

        DeleteAclsResult result = adminClient.deleteAcls(aclFilters);

        result.all()
                .toCompletionStage()
                .handleAsync((r, e) -> {
                    if (e != null)  {
                        LOGGER.warnOp("ACL reconciliation failed", e);
                        items.forEach(req -> req.result().completeExceptionally(e));
                    } else {
                        Map<AclBindingFilter, KafkaFuture<DeleteAclsResult.FilterResults>> perItemResults = result.values();

                        items.forEach(req -> {
                            final String principal = "User:" + req.username();
                            AtomicBoolean failed = new AtomicBoolean(false);

                            perItemResults.forEach((filter, fut) -> {
                                // We have to loop through the results to find results affecting our principal => these are the results related to our batch
                                if (principal.equals(filter.entryFilter().principal())) {
                                    if (fut.isCompletedExceptionally() || fut.isDone()) {
                                        try {
                                            DeleteAclsResult.FilterResults futRes = fut.getNow(null);

                                            if (futRes != null) {
                                                futRes.values().forEach(filterResult -> {
                                                    if (filterResult.exception() != null)   {
                                                        LOGGER.warnCr(req.reconciliation(), "ACL deletion for user {} and ACL filter {} failed", req.username(), filter, filterResult.exception());
                                                        failed.set(true);
                                                    }
                                                });
                                            }
                                        } catch (Throwable completionException) {
                                            LOGGER.warnCr(req.reconciliation(), "ACL deletion for user {} and ACL filter {} failed", req.username(), filter, completionException);
                                            failed.set(true);
                                        }
                                    } else if (fut.isCancelled()) {
                                        LOGGER.warnCr(req.reconciliation(), "ACL deletion for user {} and ACL filter {} was canceled", req.username(), filter);
                                        failed.set(true);
                                    } else {
                                        LOGGER.warnCr(req.reconciliation(), "ACL deletion for user {} and ACL filter {} ended in unknown state", filter, req.username());
                                        failed.set(true);
                                    }
                                }
                            });

                            if (failed.get()) {
                                req.result().completeExceptionally(new RuntimeException("ACL deletion failed"));
                            } else {
                                LOGGER.debugCr(req.reconciliation(), "ACL deletion for user {} succeeded", req.username());
                                req.result().complete(ReconcileResult.deleted());
                            }
                        });
                    }

                    return null;
                });
    }
}
