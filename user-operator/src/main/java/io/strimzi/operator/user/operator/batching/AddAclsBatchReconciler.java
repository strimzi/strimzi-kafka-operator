/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.batching;

import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.operator.AdminApiOperator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Micro-batching reconciler for creating new ACL rules using the Kafka Admin API.
 */
public class AddAclsBatchReconciler extends AbstractBatchReconciler<AdminApiOperator.ReconcileRequest<Collection<AclBinding>, ReconcileResult<Collection<AclBinding>>>> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(AddAclsBatchReconciler.class);

    private final Admin adminClient;

    /**
     * Creates the micro-batching reconciler for creating new ACL rules
     *
     * @param adminClient   Kafka Admin API client
     * @param queueSize     Maximal size of the batching queue
     * @param maxBatchSize  Maximal size of the batch
     * @param maxBatchTime  Maximal time for which the requests should be collected before a batch is sent
     */
    public AddAclsBatchReconciler(Admin adminClient, int queueSize, int maxBatchSize, int maxBatchTime) {
        super("AddAclsBatchReconciler", queueSize, maxBatchSize, maxBatchTime);
        this.adminClient = adminClient;
    }

    /**
     * Reconciles batch of requests to create new ACL rules in Apache Kafka
     *
     * @param items Batch of requests which should be executed
     */
    @Override
    protected void reconcile(Collection<AdminApiOperator.ReconcileRequest<Collection<AclBinding>, ReconcileResult<Collection<AclBinding>>>> items) {
        List<AclBinding> aclBindings = new ArrayList<>();
        items.forEach(req -> aclBindings.addAll(req.desired()));

        CreateAclsResult result = adminClient.createAcls(aclBindings);

        result.all()
                .toCompletionStage()
                .handleAsync((r, e) -> {
                    if (e != null)  {
                        LOGGER.warnOp("ACL reconciliation failed", e);
                        items.forEach(req -> req.result().completeExceptionally(e));
                    } else {
                        Map<AclBinding, KafkaFuture<Void>> perItemResults = result.values();

                        items.forEach(req -> {
                            final String principal = "User:" + req.username();
                            AtomicBoolean failed = new AtomicBoolean(false);

                            perItemResults.forEach((binding, fut) -> {
                                // We have to loop through the results to find results affecting our principal => these are the results related to our batch
                                if (principal.equals(binding.entry().principal())) {
                                    if (fut.isCompletedExceptionally()) {
                                        Exception reason = null;
                                        try {
                                            fut.getNow(null);
                                            reason = new RuntimeException("The KafkaFuture failed without an exception");
                                        } catch (Exception completionException) {
                                            reason = completionException;
                                        } finally {
                                            LOGGER.warnCr(req.reconciliation(), "ACL creation for user {} and ACL binding {} failed", req.username(), binding, reason);
                                            failed.set(true);
                                        }
                                    } else if (fut.isCancelled()) {
                                        LOGGER.warnCr(req.reconciliation(), "ACL creation for user {} and ACL binding {} was canceled", req.username(), binding);
                                        failed.set(true);
                                    } else if (fut.isDone()) {
                                        LOGGER.debugCr(req.reconciliation(), "ACL creation for user {} and ACL binding {} succeeded", req.username(), binding);
                                    } else {
                                        LOGGER.warnCr(req.reconciliation(), "ACL creation for user {} and ACL binding {} ended in unknown state", req.username(), binding);
                                        failed.set(true);
                                    }
                                }
                            });

                            if (failed.get()) {
                                req.result().completeExceptionally(new RuntimeException("ACL creation failed"));
                            } else {
                                req.result().complete(ReconcileResult.created(req.desired()));
                            }
                        });
                    }

                    return null;
                });
    }
}
