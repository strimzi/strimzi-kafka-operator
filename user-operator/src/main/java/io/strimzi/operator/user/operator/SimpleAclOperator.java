/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * SimlpeAclOperator is responsible for managing the authorization rules in Apache Kafka / Apache Zookeeper.
 * It is using Kafka's SimpleAclAuthorizer class to interact with Zookeeper and manage Acl rules.
 * Since SimpleAclAuthorizer is written in Scala, this operator is using some Scala structures required for passing to / returned from the SimpleAclAuthorizer object.
 * This class expects the SimpleAclAuthorizer instance to be passed from the outside.
 * That is useful for testing and is similar to how the Kubernetes client is passed around.
 */
@SuppressWarnings("deprecation")
public class SimpleAclOperator {
    private static final Logger log = LogManager.getLogger(SimpleAclOperator.class.getName());

    private static final List<String> IGNORED_USERS = Arrays.asList("*", "ANONYMOUS");

    private final Vertx vertx;
    private final Admin adminClient;

    /**
     * Constructor
     *
     * @param vertx Vertx instance
     * @param adminClient Kafka Admin client instance
     */
    public SimpleAclOperator(Vertx vertx, Admin adminClient)  {
        this.vertx = vertx;
        this.adminClient = adminClient;
    }

    /**
     * Reconciles Acl rules for given user
     *
     * @param username  User name of the reconciled user. When using TLS client auth, the username should be already in the Kafka format, e.g. CN=my-user
     * @param desired   The list of desired Acl rules
     * @return the Future with reconcile result
     */
    public Future<ReconcileResult<Set<SimpleAclRule>>> reconcile(String username, Set<SimpleAclRule> desired) {
        Promise<ReconcileResult<Set<SimpleAclRule>>> promise = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                Set<SimpleAclRule> current;

                try {
                    current = getAcls(username);
                } catch (Exception e)   {
                    // if authorization is not enabled in the Kafka resource, but the KafkaUser resource doesn't
                    // have ACLs, the UO can just ignore the corresponding exception
                    if (e instanceof InvalidResourceException && (desired == null || desired.isEmpty())) {
                        future.complete();
                        return;
                    } else {
                        log.error("Reconciliation failed for user {}", username, e);
                        future.fail(e);
                        return;
                    }
                }

                if (desired == null || desired.isEmpty()) {
                    if (current.size() == 0)    {
                        log.debug("User {}: No expected Acl rules and no existing Acl rules -> NoOp", username);
                        future.complete(ReconcileResult.noop(desired));
                    } else {
                        log.debug("User {}: No expected Acl rules, but {} existing Acl rules -> Deleting rules", username, current.size());
                        internalDelete(username, current).onComplete(future);
                    }
                } else {
                    if (current.isEmpty())  {
                        log.debug("User {}: {} expected Acl rules, but no existing Acl rules -> Adding rules", username, desired.size());
                        internalCreate(username, desired).onComplete(future);
                    } else  {
                        log.debug("User {}: {} expected Acl rules and {} existing Acl rules -> Reconciling rules", username, desired.size(), current.size());
                        internalUpdate(username, desired, current).onComplete(future);
                    }
                }
            },
            false,
            promise
        );
        return promise.future();
    }

    /**
     * Create all ACLs for given user
     */
    protected Future<ReconcileResult<Set<SimpleAclRule>>> internalCreate(String username, Set<SimpleAclRule> desired) {
        try {
            Collection<AclBinding> aclBindings = getAclBindings(username, desired);
            adminClient.createAcls(aclBindings).all().get();
        } catch (Exception e) {
            log.error("Adding Acl rules for user {} failed", username, e);
            return Future.failedFuture(e);
        }

        return Future.succeededFuture(ReconcileResult.created(desired));
    }

    /**
     * Update all ACLs for given user.
     * SimpleAclAuthorizer doesn't support modification of existing rules.
     * This class is using Sets to decide which rules need to be added and which need to be deleted.
     * It delagates to {@link #internalCreate internalCreate} and {@link #internalDelete internalDelete} methods for the actual addition or deletion.
     */
    protected Future<ReconcileResult<Set<SimpleAclRule>>> internalUpdate(String username, Set<SimpleAclRule> desired, Set<SimpleAclRule> current) {
        Set<SimpleAclRule> toBeDeleted = new HashSet<>(current);
        toBeDeleted.removeAll(desired);

        Set<SimpleAclRule> toBeAdded = new HashSet<>(desired);
        toBeAdded.removeAll(current);

        List<Future> updates = new ArrayList<>(2);
        updates.add(internalDelete(username, toBeDeleted));
        updates.add(internalCreate(username, toBeAdded));

        Promise<ReconcileResult<Set<SimpleAclRule>>> promise = Promise.promise();

        CompositeFuture.all(updates).onComplete(res -> {
            if (res.succeeded())    {
                promise.complete(ReconcileResult.patched(desired));
            } else  {
                log.error("Updating Acl rules for user {} failed", username, res.cause());
                promise.fail(res.cause());
            }
        });

        return promise.future();
    }

    private Collection<AclBindingFilter> getAclBindingFilters(String username, Set<SimpleAclRule> aclRules) {
        KafkaPrincipal principal = new KafkaPrincipal("User", username);
        Collection<AclBindingFilter> aclBindingFilters = new ArrayList<>();
        for (SimpleAclRule rule: aclRules) {
            aclBindingFilters.add(rule.toKafkaAclBinding(principal).toFilter());
        }
        return aclBindingFilters;
    }

    private Collection<AclBinding> getAclBindings(String username, Set<SimpleAclRule> aclRules) {
        KafkaPrincipal principal = new KafkaPrincipal("User", username);
        Collection<AclBinding> aclBindings = new ArrayList<>();
        for (SimpleAclRule rule: aclRules) {
            aclBindings.add(rule.toKafkaAclBinding(principal));
        }
        return aclBindings;
    }

    /**
     * Deletes all ACLs for given user
     */
    protected Future<ReconcileResult<Set<SimpleAclRule>>> internalDelete(String username, Set<SimpleAclRule> current) {

        try {
            Collection<AclBindingFilter> aclBindingFilters = getAclBindingFilters(username, current);
            adminClient.deleteAcls(aclBindingFilters).all().get();
        } catch (Exception e) {
            log.error("Deleting Acl rules for user {} failed", username, e);
            return Future.failedFuture(e);
        }
        return Future.succeededFuture(ReconcileResult.deleted());
    }

    /**
     * Returns Set of ACLs applying to single user.
     *
     * @param username  Name of the user.
     * @return The Set of ACLs applying to single user.
     */
    public Set<SimpleAclRule> getAcls(String username)   {
        log.debug("Searching for ACL rules of user {}", username);
        Set<SimpleAclRule> result = new HashSet<>();
        KafkaPrincipal principal = new KafkaPrincipal("User", username);

        AclBindingFilter aclBindingFilter = new AclBindingFilter(ResourcePatternFilter.ANY,
            new AccessControlEntryFilter(principal.toString(), null, AclOperation.ANY, AclPermissionType.ANY));

        Collection<AclBinding> aclBindings = null;
        try {
            aclBindings = adminClient.describeAcls(aclBindingFilter).values().get();
        } catch (InterruptedException | ExecutionException e) {
            // Admin Client API needs authorizer enabled on the Kafka brokers
            if (e.getCause() instanceof SecurityDisabledException) {
                throw new InvalidResourceException("Authorization needs to be enabled in the Kafka custom resource", e.getCause());
            } else if (e.getCause() instanceof UnknownServerException && e.getMessage().contains("Simple ACL delegation not enabled")) {
                throw new InvalidResourceException("Simple ACL delegation needs to be enabled in the Kafka custom resource", e.getCause());
            }
        }

        if (aclBindings != null) {
            log.debug("ACL rules for user {}", username);
            for (AclBinding aclBinding : aclBindings) {
                log.debug("{}", aclBinding);
                result.add(SimpleAclRule.fromAclBinding(aclBinding));
            }
        }

        return result;
    }

    /**
     * Returns set with all usernames which have some ACLs.
     *
     * @return The set with all usernames which have some ACLs.
     */
    public Set<String> getUsersWithAcls()   {
        Set<String> result = new HashSet<>();
        Set<String> ignored = new HashSet<>(IGNORED_USERS.size());

        log.debug("Searching for Users with any ACL rules");

        Collection<AclBinding> aclBindings;
        try {
            aclBindings = adminClient.describeAcls(AclBindingFilter.ANY).values().get();
        } catch (InterruptedException | ExecutionException e) {
            return result;
        }

        for (AclBinding aclBinding : aclBindings) {
            KafkaPrincipal principal = SecurityUtils.parseKafkaPrincipal(aclBinding.entry().principal());

            if (KafkaPrincipal.USER_TYPE.equals(principal.getPrincipalType()))  {
                // Username in ACL might keep different format (for example based on user's subject) and need to be decoded
                String username = KafkaUserModel.decodeUsername(principal.getName());

                if (IGNORED_USERS.contains(username))   {
                    if (!ignored.contains(username)) {
                        // This info message is loged only once per reocnciliation even if there are multiple rules
                        log.info("Existing ACLs for user '{}' will be ignored.", username);
                        ignored.add(username);
                    }
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Adding user {} to Set of users with ACLs", username);
                    }

                    result.add(username);
                }
            }
        }

        return result;
    }
}
