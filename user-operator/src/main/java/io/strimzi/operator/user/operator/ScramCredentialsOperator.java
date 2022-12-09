/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.UserOperatorConfig;
import io.strimzi.operator.user.operator.batching.ScramShaCredentialsBatchReconciler;
import io.strimzi.operator.user.operator.cache.ScramShaCredentialsCache;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialAlteration;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

/**
 * ScramCredentialsOperator is responsible for managing the SCRAM-SHA credentials in Apache Kafka.
 */
public class ScramCredentialsOperator implements AdminApiOperator<String, List<String>> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ScramCredentialsOperator.class.getName());
    private final static int ITERATIONS = 4096;
    private final static ScramMechanism SCRAM_MECHANISM = ScramMechanism.SCRAM_SHA_512;
    // Not generating new salt in every reconcile loop reduce the amount of changes (otherwise everything changes every loop)
    // This salt uses the same algorithm as Kafka
    private final static byte[] SALT =  (new BigInteger(130, new SecureRandom())).toString(36).getBytes(StandardCharsets.UTF_8);

    private final ScramShaCredentialsBatchReconciler patchReconciler;
    private final ScramShaCredentialsCache cache;
    private final ExecutorService executor;

    /**
     * Constructor
     *
     * @param adminClient   Kafka Admin client instance
     * @param config        User operator configuration
     * @param executor      Shared executor for executing async operations
     */
    public ScramCredentialsOperator(Admin adminClient, UserOperatorConfig config, ExecutorService executor) {
        this.executor = executor;

        // Create cache for querying the SCRAM-SHA Credentials locally
        this.cache = new ScramShaCredentialsCache(adminClient, config.getCacheRefresh());

        // Create micro-batching reconciler for updating the SCRAM-SHA credentials
        this.patchReconciler = new ScramShaCredentialsBatchReconciler(adminClient, config.getBatchQueueSize(), config.getBatchMaxBlockSize(), config.getBatchMaxBlockTime());
    }

    /**
     * Reconciles SCRAM-SHA credentials for given user
     *
     * @param reconciliation    The reconciliation
     * @param username          Username of the reconciled user
     * @param desired           The desired password
     *
     * @return the Future with reconcile result
     */
    @Override
    public CompletionStage<ReconcileResult<String>> reconcile(Reconciliation reconciliation, String username, String desired) {
        boolean exists = userExists(username);

        if (desired == null && !exists) {
            // Username is not found in cache so the credentials should not exist => we can ignore it.
            return CompletableFuture.completedFuture(ReconcileResult.noop(null));
        } else {
            // Username either does not exist yet and should be created or does not exist and should be deleted
            UserScramCredentialAlteration alteration;

            if (desired != null) {
                LOGGER.debugCr(reconciliation, "Upserting SCRAM-SHA credentials for user {}", username);
                alteration = new UserScramCredentialUpsertion(username, new ScramCredentialInfo(SCRAM_MECHANISM, ITERATIONS), desired.getBytes(StandardCharsets.UTF_8), SALT);
            } else {
                LOGGER.debugCr(reconciliation, "Deleting SCRAM-SHA credentials for user {}", username);
                alteration = new UserScramCredentialDeletion(username, SCRAM_MECHANISM);
            }

            CompletableFuture<ReconcileResult<UserScramCredentialAlteration>> future = new CompletableFuture<>();

            try {
                patchReconciler.enqueue(new ReconcileRequest<>(reconciliation, username, alteration, future));
            } catch (InterruptedException e) {
                LOGGER.warnCr(reconciliation, "Failed to enqueue ScramShaCredentialsAlteration", e);
                future.completeExceptionally(e);
            }

            return future.handleAsync((r, e) -> {
                if (e != null) {
                    if (desired != null) {
                        LOGGER.warnCr(reconciliation, "Failed to upsert SCRAM-SHA credentials of user {}", username, e);
                    } else {
                        LOGGER.warnCr(reconciliation, "Failed to delete SCRAM-SHA credentials of user {}", username, e);
                    }

                    throw new CompletionException(e);
                } else {
                    if (desired != null) {
                        LOGGER.debugCr(reconciliation, "Updated SCRAM credentials for user {}", username);
                        cache.put(username, true); // Update the cache
                        return ReconcileResult.patched(desired);
                    } else {
                        if (r instanceof ReconcileResult.Noop) {
                            LOGGER.debugCr(reconciliation, "SCRAM credentials for user {} did not exist anymore", username);
                            cache.remove(username); // Update the cache
                            return ReconcileResult.noop(null);
                        } else {
                            LOGGER.debugCr(reconciliation, "Deleted SCRAM credentials for user {}", username);
                            cache.remove(username); // Update the cache
                            return ReconcileResult.deleted();
                        }
                    }
                }
            }, executor);
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
            LOGGER.warnOp("Interrupted while stopping ScramShaCredentials PatchReconciler");
        }
    }

    /**
     * Utility methods which checks if the user already has some SCRAM-SHA credentials
     *
     * @param username  Name of the user
     *
     * @return  True if the user already has some credentials. False otherwise.
     */
    private boolean userExists(String username) {
        return Boolean.TRUE.equals(cache.get(username));
    }

    /**
     * @return List with all usernames which have some scram credentials set
     */
    @Override
    public CompletionStage<List<String>> getAllUsers() {
        LOGGER.debugOp("Listing all users with SCRAM credentials");
        return CompletableFuture.completedFuture(Collections.list(cache.keys()));
    }
}
