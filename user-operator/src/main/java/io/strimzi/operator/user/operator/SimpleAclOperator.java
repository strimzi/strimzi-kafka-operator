/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SimpleAclOperator is responsible for managing the authorization rules in Apache Kafka / Apache Zookeeper.
 */
public class SimpleAclOperator extends AbstractAdminApiOperator<Set<SimpleAclRule>, Set<String>> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(SimpleAclOperator.class.getName());

    private static final List<String> IGNORED_USERS = Arrays.asList("*", "ANONYMOUS");

    /**
     * Constructor
     *
     * @param vertx Vertx instance
     * @param adminClient Kafka Admin client instance
     */
    public SimpleAclOperator(Vertx vertx, Admin adminClient) {
        super(vertx, adminClient);
    }

    /**
     * Reconciles Acl rules for given user
     *
     * @param reconciliation The reconciliation
     * @param username  User name of the reconciled user. When using TLS client auth, the username should be already in the Kafka format, e.g. CN=my-user
     * @param desired   The list of desired Acl rules
     *
     * @return the Future with reconcile result
     */
    @Override
    public Future<ReconcileResult<Set<SimpleAclRule>>> reconcile(Reconciliation reconciliation, String username, Set<SimpleAclRule> desired) {
        return getAsync(reconciliation, username)
                .compose(current -> {
                    if (desired == null || desired.isEmpty()) {
                        if (current.size() == 0)    {
                            LOGGER.debugCr(reconciliation, "No expected Acl rules and no existing Acl rules -> NoOp");
                            return Future.succeededFuture(ReconcileResult.noop(desired));
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
                });
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
    protected Future<ReconcileResult<Set<SimpleAclRule>>> internalCreate(Reconciliation reconciliation, String username, Set<SimpleAclRule> desired) {
        Collection<AclBinding> aclBindings = getAclBindings(username, desired);
        return Util.kafkaFutureToVertxFuture(reconciliation, vertx, adminClient.createAcls(aclBindings).all())
                .map(ReconcileResult.created(desired));
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
    private Future<ReconcileResult<Set<SimpleAclRule>>> internalUpdate(Reconciliation reconciliation, String username, Set<SimpleAclRule> desired, Set<SimpleAclRule> current) {
        Set<SimpleAclRule> toBeDeleted = new HashSet<>(current);
        toBeDeleted.removeAll(desired);

        Set<SimpleAclRule> toBeAdded = new HashSet<>(desired);
        toBeAdded.removeAll(current);

        List<Future> updates = new ArrayList<>(2);

        if (!toBeDeleted.isEmpty()) {
            updates.add(internalDelete(reconciliation, username, toBeDeleted));
        }

        if (!toBeAdded.isEmpty()) {
            updates.add(internalCreate(reconciliation, username, toBeAdded));
        }

        return CompositeFuture.all(updates)
                .map(ReconcileResult.patched(desired));
    }

    /**
     * Utility method for preparing AclBindingFilters for deleting ACLs
     *
     * @param username Name of the user
     * @param aclRules ACL rules which should be deleted
     *
     * @return The Future with reconcile result
     */
    private Collection<AclBindingFilter> getAclBindingFilters(String username, Set<SimpleAclRule> aclRules) {
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
    private Collection<AclBinding> getAclBindings(String username, Set<SimpleAclRule> aclRules) {
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
     * @return The Future with reconcile result
     */
    private Future<ReconcileResult<Set<SimpleAclRule>>> internalDelete(Reconciliation reconciliation, String username, Set<SimpleAclRule> current) {
        Collection<AclBindingFilter> aclBindingFilters = getAclBindingFilters(username, current);
        return Util.kafkaFutureToVertxFuture(reconciliation, vertx, adminClient.deleteAcls(aclBindingFilters).all())
                .map(ReconcileResult.deleted());
    }

    /**
     * Returns Set of ACLs applying to single user.
     *
     * @param reconciliation The reconciliation
     * @param username  Name of the user.
     *
     * @return The Set of ACLs applying to single user.
     */
    private Future<Set<SimpleAclRule>> getAsync(Reconciliation reconciliation, String username)   {
        LOGGER.debugCr(reconciliation, "Searching for ACL rules of user {}", username);

        KafkaPrincipal principal = new KafkaPrincipal("User", username);
        AclBindingFilter aclBindingFilter = new AclBindingFilter(ResourcePatternFilter.ANY,
                new AccessControlEntryFilter(principal.toString(), null, AclOperation.ANY, AclPermissionType.ANY));

        return Util.kafkaFutureToVertxFuture(reconciliation, vertx, adminClient.describeAcls(aclBindingFilter).values())
                .compose(aclBindings -> {
                    Set<SimpleAclRule> result = new HashSet<>(aclBindings.size());

                    LOGGER.debugCr(reconciliation, "ACL rules for user {}", username);
                    for (AclBinding aclBinding : aclBindings) {
                        LOGGER.debugOp("{}", aclBinding);
                        result.add(SimpleAclRule.fromAclBinding(aclBinding));
                    }

                    return Future.succeededFuture(result);
                });
    }

    /**
     * @return Set with all usernames which have some ACLs set
     */
    @Override
    public Future<Set<String>> getAllUsers() {
        LOGGER.debugOp("Searching for Users with any ACL rules");

        DescribeAclsResult result = adminClient.describeAcls(AclBindingFilter.ANY);
        return Util.kafkaFutureToVertxFuture(vertx, result.values())
                .compose(aclBindings -> {
                    Set<String> users = new HashSet<>();
                    Set<String> ignored = new HashSet<>(IGNORED_USERS.size());

                    for (AclBinding aclBinding : aclBindings) {
                        KafkaPrincipal principal = SecurityUtils.parseKafkaPrincipal(aclBinding.entry().principal());

                        if (KafkaPrincipal.USER_TYPE.equals(principal.getPrincipalType())) {
                            // Username in ACL might keep different format (for example based on user's subject) and need to be decoded
                            String username = KafkaUserModel.decodeUsername(principal.getName());

                            if (IGNORED_USERS.contains(username)) {
                                if (!ignored.contains(username)) {
                                    // This info message is loged only once per reconciliation even if there are multiple rules
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
                    }

                    return Future.succeededFuture(users);
                });
    }
}
