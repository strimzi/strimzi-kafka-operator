/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.UserOperatorConfig;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.strimzi.operator.user.operator.batching.AddAclsBatchReconciler;
import io.strimzi.operator.user.operator.batching.DeleteAclsBatchReconciler;
import io.strimzi.operator.user.operator.cache.AclCache;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

/**
 * SimpleAclOperator is responsible for managing the authorization rules in Apache Kafka.
 */
public class SimpleAclOperator implements AdminApiOperator<Set<SimpleAclRule>, Set<String>> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(SimpleAclOperator.class.getName());
    private static final List<String> IGNORED_USERS = Arrays.asList("*", "ANONYMOUS");

    private final AddAclsBatchReconciler addReconciler;
    private final DeleteAclsBatchReconciler deleteReconciler;
    private final AclCache cache;
    private final ExecutorService executor;

    /**
     * Constructor
     *
     * @param adminClient   Kafka Admin client instance
     * @param config        User operator configuration
     * @param executor      Shared executor for executing async operations
     */
    public SimpleAclOperator(Admin adminClient, UserOperatorConfig config, ExecutorService executor) {
        this.executor = executor;

        // Create cache for querying the ACLs locally
        this.cache = new AclCache(adminClient, config.getCacheRefresh());

        // Create micro-batching reconcilers for managing the ACLs
        this.addReconciler = new AddAclsBatchReconciler(adminClient, config.getBatchQueueSize(), config.getBatchMaxBlockSize(), config.getBatchMaxBlockTime());
        this.deleteReconciler = new DeleteAclsBatchReconciler(adminClient, config.getBatchQueueSize(), config.getBatchMaxBlockSize(), config.getBatchMaxBlockTime());
    }

    /**
     * Reconciles Acl rules for given user
     *
     * @param reconciliation The reconciliation
     * @param username  Username of the reconciled user. When using TLS client auth, the username should be already in the Kafka format, e.g. CN=my-user
     * @param desired   The list of desired Acl rules
     *
     * @return the Future with reconcile result
     */
    @Override
    public CompletionStage<ReconcileResult<Set<SimpleAclRule>>> reconcile(Reconciliation reconciliation, String username, Set<SimpleAclRule> desired) {
        Set<SimpleAclRule> current = cache.getOrDefault(username, Set.of());

        if (desired == null || desired.isEmpty()) {
            if (current.size() == 0)    {
                LOGGER.debugCr(reconciliation, "No expected Acl rules and no existing Acl rules -> NoOp");
                return CompletableFuture.completedFuture(ReconcileResult.noop(desired));
            } else {
                LOGGER.debugCr(reconciliation, "No expected Acl rules, but {} existing Acl rules -> Deleting rules", current.size());
                return internalDelete(reconciliation, username, current);
            }
        } else {
            if (current.isEmpty())  {
                LOGGER.debugCr(reconciliation, "{} expected Acl rules, but no existing Acl rules -> Adding rules", desired.size());
                return internalCreate(reconciliation, username, desired);
            } else  {
                LOGGER.debugCr(reconciliation, "{} expected Acl rules and {} existing Acl rules -> Reconciling rules", desired.size(), current.size());
                return internalUpdate(reconciliation, username, desired, current);
            }
        }
    }

    /**
     * Starts the Cache and the patch reconciler
     */
    @Override
    public void start() {
        cache.start();
        addReconciler.start();
        deleteReconciler.start();
    }

    /**
     * Stops the Cache and the patch reconciler
     */
    @Override
    public void stop() {
        cache.stop();

        try {
            addReconciler.stop();
        } catch (InterruptedException e) {
            LOGGER.warnOp("Interrupted while stopping ACL AddReconciler");
        }

        try {
            deleteReconciler.stop();
        } catch (InterruptedException e) {
            LOGGER.warnOp("Interrupted while stopping ACL DeleteReconciler");
        }
    }

    /**
     * Create ACLs for the given user.
     *
     * @param reconciliation The reconciliation
     * @param username Name of the user
     * @param desired The desired ACLs
     *
     * @return the Future with reconcile result
     */
    private CompletionStage<ReconcileResult<Set<SimpleAclRule>>> internalCreate(Reconciliation reconciliation, String username, Set<SimpleAclRule> desired) {
        LOGGER.debugCr(reconciliation, "Requesting creation of ACLs for user {}", username);

        return createAcls(reconciliation, username, desired)
                .handleAsync((r, e) -> {
                    if (e != null)  {
                        LOGGER.warnCr(reconciliation, "Failed to create ACLs of user {}", username, e);
                        throw new CompletionException(e);
                    } else {
                        LOGGER.debugCr(reconciliation, "ACLs for user {} created", username);
                        cache.put(username, desired); // Update cache
                        return ReconcileResult.created(desired);
                    }
                }, executor);
    }

    /**
     * Utility method for queueing the create ACL request in the micro-batcher for processing. This is a separate
     * method because it is called from internalCreate as well as internalUpdate.
     *
     * @param reconciliation The reconciliation
     * @param username Name of the user
     * @param desired The desired ACLs
     *
     * @return the Future with reconcile result
     */
    private CompletableFuture<ReconcileResult<Collection<AclBinding>>> createAcls(Reconciliation reconciliation, String username, Set<SimpleAclRule> desired) {
        LOGGER.debugCr(reconciliation, "Creating ACLs for user {}", username);

        CompletableFuture<ReconcileResult<Collection<AclBinding>>> future = new CompletableFuture<>();

        try {
            Collection<AclBinding> aclBindings = getAclBindings(username, desired);
            addReconciler.enqueue(new ReconcileRequest<>(reconciliation, username, aclBindings, future));
        } catch (InterruptedException e) {
            LOGGER.warnCr(reconciliation, "Failed to enqueue ACL creation request", e);
            future.completeExceptionally(e);
        }

        return future;
    }

    /**
     * Update all ACLs for given user.
     * This method is using Sets to decide which rules need to be added and which need to be deleted.
     * It delegates to {@link #internalCreate internalCreate} and {@link #internalDelete internalDelete} methods for the actual addition or deletion.
     *
     * @param reconciliation The reconciliation
     * @param username Name of the user
     * @param desired The desired ACLs
     *
     * @return the Future with reconcile result
     */
    private CompletionStage<ReconcileResult<Set<SimpleAclRule>>> internalUpdate(Reconciliation reconciliation, String username, Set<SimpleAclRule> desired, Set<SimpleAclRule> current) {
        LOGGER.debugCr(reconciliation, "Requesting update of ACLs for user {}", username);

        @SuppressWarnings({ "rawtypes" })
        List<CompletableFuture> updates = new ArrayList<>(2);

        Set<SimpleAclRule> toBeDeleted = new HashSet<>(current);
        toBeDeleted.removeAll(desired);

        if (!toBeDeleted.isEmpty()) {
            updates.add(deleteAcls(reconciliation, username, toBeDeleted).toCompletableFuture());
        }

        Set<SimpleAclRule> toBeAdded = new HashSet<>(desired);
        toBeAdded.removeAll(current);

        if (!toBeAdded.isEmpty()) {
            updates.add(createAcls(reconciliation, username, toBeAdded).toCompletableFuture());
        }

        return CompletableFuture.allOf(updates.toArray(new CompletableFuture[0]))
                .handleAsync((r, e) -> {
                    if (e != null)  {
                        LOGGER.warnCr(reconciliation, "Failed to update ACLs of user {}", username, e);
                        throw new CompletionException(e);
                    } else {
                        cache.put(username, desired); // Update cache
                        return ReconcileResult.patched(desired);
                    }
                }, executor);
    }

    /**
     * Utility method for preparing AclBindingFilters for deleting ACLs
     *
     * @param username Name of the user
     * @param aclRules ACL rules which should be deleted
     *
     * @return The Future with reconcile result
     */
    private static Collection<AclBindingFilter> getAclBindingFilters(String username, Set<SimpleAclRule> aclRules) {
        KafkaPrincipal principal = new KafkaPrincipal("User", username);
        Collection<AclBindingFilter> aclBindingFilters = new ArrayList<>();

        for (SimpleAclRule rule: aclRules) {
            aclBindingFilters.add(rule.toKafkaAclBinding(principal).toFilter());
        }

        return aclBindingFilters;
    }

    /**
     * Utility method for preparing AclBinding for creating ACLs
     *
     * @param username Name of the user
     * @param aclRules ACL rules which should be created
     *
     * @return The Future with reconcile result
     */
    private static Collection<AclBinding> getAclBindings(String username, Set<SimpleAclRule> aclRules) {
        KafkaPrincipal principal = new KafkaPrincipal("User", username);
        Collection<AclBinding> aclBindings = new ArrayList<>();

        for (SimpleAclRule rule: aclRules) {
            aclBindings.add(rule.toKafkaAclBinding(principal));
        }

        return aclBindings;
    }

    /**
     * Delete the ACLs for the given user.
     *
     * @param reconciliation The reconciliation
     * @param username Name of the user
     *
     * @return The CompletionStage with reconcile result
     */
    private CompletionStage<ReconcileResult<Set<SimpleAclRule>>> internalDelete(Reconciliation reconciliation, String username, Set<SimpleAclRule> current) {
        LOGGER.debugCr(reconciliation, "Requesting ACL deletion for user {}", username);

        return deleteAcls(reconciliation, username, current)
                .handleAsync((r, e) -> {
                    if (e != null)  {
                        LOGGER.warnCr(reconciliation, "Failed to delete ACLs of user {}", username, e);
                        throw new CompletionException(e);
                    } else {
                        LOGGER.debugCr(reconciliation, "ACLs for user {} deleted", username);
                        cache.remove(username); // Update cache
                        return ReconcileResult.deleted();
                    }
                }, executor);
    }

    /**
     * Utility method for queueing the "delete ACLs" request in the micro-batcher for processing. This is a separate
     * method because it is called from internalDelete as well as internalUpdate.
     *
     * @param reconciliation    The reconciliation
     * @param username          Name of the user
     * @param current           The current ACLs
     *
     * @return  The Future with reconcile result
     */
    private CompletableFuture<ReconcileResult<Collection<AclBindingFilter>>> deleteAcls(Reconciliation reconciliation, String username, Set<SimpleAclRule> current)   {
        LOGGER.debugCr(reconciliation, "Deleting ACLs of user {}", username);

        CompletableFuture<ReconcileResult<Collection<AclBindingFilter>>> future = new CompletableFuture<>();

        try {
            Collection<AclBindingFilter> aclBindingFilters = getAclBindingFilters(username, current);
            deleteReconciler.enqueue(new ReconcileRequest<>(reconciliation, username, aclBindingFilters, future));
        } catch (InterruptedException e) {
            LOGGER.warnCr(reconciliation, "Failed to enqueue ACL deletion request", e);
            future.completeExceptionally(e);
        }

        return future;
    }

    /**
     * @return Set with all usernames which have some ACLs set
     */
    @Override
    public CompletionStage<Set<String>> getAllUsers() {
        LOGGER.debugOp("Searching for Users with any ACL rules");

        Set<String> users = new HashSet<>();
        Set<String> ignored = new HashSet<>(IGNORED_USERS.size());
        Enumeration<String> keys = cache.keys();

        while (keys.hasMoreElements())  {
            String username = KafkaUserModel.decodeUsername(keys.nextElement());

            if (IGNORED_USERS.contains(username)) {
                if (!ignored.contains(username)) {
                    // This info message is logged only once per reconciliation even if there are multiple rules
                    LOGGER.infoOp("Existing ACLs for user '{}' will be ignored.", username);
                    ignored.add(username);
                }
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debugOp("Adding user {} to Set of users with ACLs", username);
                }

                users.add(username);
            }
        }

        return CompletableFuture.completedFuture(users);
    }
}
