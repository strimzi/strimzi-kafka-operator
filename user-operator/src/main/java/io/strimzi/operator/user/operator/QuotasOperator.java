/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.api.kafka.model.user.KafkaUserQuotas;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.UserOperatorConfig;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.QuotaUtils;
import io.strimzi.operator.user.operator.batching.QuotasBatchReconciler;
import io.strimzi.operator.user.operator.cache.QuotasCache;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

/**
 * KafkaUserQuotasOperator is responsible for managing quotas in Apache Kafka
 */
public class QuotasOperator implements AdminApiOperator<KafkaUserQuotas, Set<String>> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(QuotasOperator.class.getName());

    private final QuotasBatchReconciler patchReconciler;
    private final QuotasCache cache;
    private final ExecutorService executor;

    /**
     * Constructor
     *
     * @param adminClient   Kafka Admin client instance
     * @param config        User operator configuration
     * @param executor      Shared executor for executing async operations
     */
    public QuotasOperator(Admin adminClient, UserOperatorConfig config, ExecutorService executor) {
        this.executor = executor;

        // Create cache for querying the Quotas locally
        this.cache = new QuotasCache(adminClient, config.getCacheRefresh());

        // Create micro-batching reconcilers for managing the quotas
        this.patchReconciler = new QuotasBatchReconciler(adminClient, config.getBatchQueueSize(), config.getBatchMaxBlockSize(), config.getBatchMaxBlockTime());
    }

    /**
     * Reconciles quotas for given user
     *
     * @param reconciliation    The reconciliation
     * @param username          Username of the reconciled user. When using TLS client auth, the username should be already in the Kafka format, e.g. CN=my-user
     * @param desired           The desired quotas configuration
     *
     * @return the CompletionStage with reconcile result
     */
    @Override
    public CompletionStage<ReconcileResult<KafkaUserQuotas>> reconcile(Reconciliation reconciliation, String username, KafkaUserQuotas desired) {
        KafkaUserQuotas current = cache.get(username);

        if (desired == null) {
            if (current == null)    {
                LOGGER.debugCr(reconciliation, "No expected quotas and no existing quotas -> NoOp");
                return CompletableFuture.completedFuture(ReconcileResult.noop(null));
            } else {
                LOGGER.debugCr(reconciliation, "No expected quotas, but {} existing quotas -> Deleting quotas", current);
                return internalDelete(reconciliation, username);
            }
        } else {
            if (current == null)  {
                LOGGER.debugCr(reconciliation, "{} expected quotas, but no existing quotas -> Adding quotas", desired);
                return internalUpsert(reconciliation, username, desired);
            } else if (!QuotaUtils.quotasEquals(current, desired)) {
                LOGGER.debugCr(reconciliation, "{} expected quotas and {} existing quotas differ -> Reconciling quotas", desired, current);
                return internalUpsert(reconciliation, username, desired);
            } else {
                LOGGER.debugCr(reconciliation, "{} expected quotas are the same as existing quotas -> NoOp", desired);
                return CompletableFuture.completedFuture(ReconcileResult.noop(desired));
            }
        }
    }

    /**
     * Starts the Cache and the patch reconciler
     */
    @Override
    public void start() {
        cache.start();
        patchReconciler.start();
    }

    /**
     * Stops the Cache and the patch reconciler
     */
    @Override
    public void stop() {
        cache.stop();

        try {
            patchReconciler.stop();
        } catch (InterruptedException e) {
            LOGGER.warnOp("Interrupted while stopping Quotas PatchReconciler");
        }
    }

    /**
     * Delete the quotas for the given user.
     *
     * @param reconciliation The reconciliation
     * @param username Name of the user
     *
     * @return the CompletionStage with reconcile result
     */
    private CompletionStage<ReconcileResult<KafkaUserQuotas>> internalDelete(Reconciliation reconciliation, String username) {
        LOGGER.debugCr(reconciliation, "Deleting quotas for user {}", username);

        return patchQuotas(reconciliation, username, emptyQuotas())
                .handleAsync((r, e) -> {
                    if (e != null)  {
                        LOGGER.warnCr(reconciliation, "Failed to delete quotas for user {}", username, e);
                        throw new CompletionException(e);
                    } else {
                        LOGGER.debugCr(reconciliation, "Quotas for user {} deleted", username);
                        cache.remove(username); // Update the cache
                        return ReconcileResult.deleted();
                    }
                }, executor);
    }

    /**
     * Set the quotas for the given user.
     *
     * @param reconciliation The reconciliation
     * @param username Name of the user
     * @param desired The desired quotas
     *
     * @return the CompletionStage with reconcile result
     */
    private CompletionStage<ReconcileResult<KafkaUserQuotas>> internalUpsert(Reconciliation reconciliation, String username, KafkaUserQuotas desired) {
        LOGGER.debugCr(reconciliation, "Upserting quotas for user {}", username);

        return patchQuotas(reconciliation, username, desired)
                .handleAsync((r, e) -> {
                    if (e != null)  {
                        LOGGER.warnCr(reconciliation, "Failed to upsert quotas of user {}", username, e);
                        throw new CompletionException(e);
                    } else {
                        LOGGER.debugCr(reconciliation, "Quotas for user {} upserted", username);
                        cache.put(username, desired); // Update the cache

                        return ReconcileResult.patched(desired);
                    }
                }, executor);
    }

    /**
     * Set the quotas for the given user.
     *
     * @param username Name of the user
     * @param desired The desired quotas
     *
     * @return the CompletionStage with reconcile result
     */
    private CompletionStage<ReconcileResult<ClientQuotaAlteration>> patchQuotas(Reconciliation reconciliation, String username, KafkaUserQuotas desired) {
        ClientQuotaEntity cqe = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, username));
        ClientQuotaAlteration cqa = new ClientQuotaAlteration(cqe, QuotaUtils.toClientQuotaAlterationOps(desired));
        CompletableFuture<ReconcileResult<ClientQuotaAlteration>> future = new CompletableFuture<>();

        try {
            patchReconciler.enqueue(new ReconcileRequest<>(reconciliation, username, cqa, future));
        } catch (InterruptedException e) {
            LOGGER.warnCr(reconciliation, "Failed to enqueue ClientQuotaAlteration", e);
            future.completeExceptionally(e);
        }

        return future;
    }

    /**
     * Utility method to generate object with null quotas
     *
     * @return  KafkaUserQuotas with all quotas being null
     */
    private static KafkaUserQuotas emptyQuotas()   {
        KafkaUserQuotas emptyQuotas = new KafkaUserQuotas();
        emptyQuotas.setProducerByteRate(null);
        emptyQuotas.setConsumerByteRate(null);
        emptyQuotas.setRequestPercentage(null);
        emptyQuotas.setControllerMutationRate(null);

        return emptyQuotas;
    }

    /**
     * @return Set with all usernames which have some ACLs set
     */
    @Override
    public CompletionStage<Set<String>> getAllUsers() {
        LOGGER.debugOp("Searching for Users with any quotas");

        Set<String> users = new HashSet<>();
        Enumeration<String> keys = cache.keys();

        while (keys.hasMoreElements())  {
            users.add(KafkaUserModel.decodeUsername(keys.nextElement()));
        }

        return CompletableFuture.completedFuture(users);
    }
}
