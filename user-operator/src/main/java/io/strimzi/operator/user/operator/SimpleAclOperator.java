/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.strimzi.operator.user.model.acl.SimpleAclRuleResource;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import kafka.security.auth.Acl;
import kafka.security.auth.Resource;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private final kafka.security.auth.SimpleAclAuthorizer authorizer;

    /**
     * Constructor
     *
     * @param vertx     Vertx instance
     * @param authorizer    SimpleAcAuthorizer instance
     */
    public SimpleAclOperator(Vertx vertx, kafka.security.auth.SimpleAclAuthorizer authorizer)  {
        this.vertx = vertx;
        this.authorizer = authorizer;
    }

    /**
     * Reconciles Acl rules for given user
     *
     * @param username  User name of the reconciled user. When using TLS client auth, the username should be already in the Kafka format, e.g. CN=my-user
     * @param desired   The list of desired Acl rules
     * @return
     */
    Future<ReconcileResult<Set<SimpleAclRule>>> reconcile(String username, Set<SimpleAclRule> desired) {
        Promise<ReconcileResult<Set<SimpleAclRule>>> promise = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                Set<SimpleAclRule> current;

                try {
                    current = getAcls(username);
                } catch (Exception e)   {
                    log.error("Reconciliation failed for user {}", username, e);
                    future.fail(e);
                    return;
                }

                if (desired == null || desired.isEmpty()) {
                    if (current.size() == 0)    {
                        log.debug("User {}: No expected Acl rules and no existing Acl rules -> NoOp", username);
                        future.complete(ReconcileResult.noop(desired));
                    } else {
                        log.debug("User {}: No expected Acl rules, but {} existing Acl rules -> Deleting rules", username, current.size());
                        internalDelete(username, current).setHandler(future);
                    }
                } else {
                    if (current.isEmpty())  {
                        log.debug("User {}: {} expected Acl rules, but no existing Acl rules -> Adding rules", username, desired.size());
                        internalCreate(username, desired).setHandler(future);
                    } else  {
                        log.debug("User {}: {} expected Acl rules and {} existing Acl rules -> Reconciling rules", username, desired.size(), current.size());
                        internalUpdate(username, desired, current).setHandler(future);
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
            HashMap<Resource, Set<Acl>> map = getResourceAclsMap(username, desired);
            for (Map.Entry<Resource, Set<Acl>> entry: map.entrySet()) {
                scala.collection.mutable.Set<Acl> add = JavaConverters.asScalaSet(entry.getValue());
                authorizer.addAcls(add.toSet(), entry.getKey());
            }
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
        Set<SimpleAclRule> toBeDeleted = new HashSet<SimpleAclRule>(current);
        toBeDeleted.removeAll(desired);

        Set<SimpleAclRule> toBeAdded = new HashSet<SimpleAclRule>(desired);
        toBeAdded.removeAll(current);

        List<Future> updates = new ArrayList<>(2);
        updates.add(internalDelete(username, toBeDeleted));
        updates.add(internalCreate(username, toBeAdded));

        Promise<ReconcileResult<Set<SimpleAclRule>>> promise = Promise.promise();

        CompositeFuture.all(updates).setHandler(res -> {
            if (res.succeeded())    {
                promise.complete(ReconcileResult.patched(desired));
            } else  {
                log.error("Updating Acl rules for user {} failed", username, res.cause());
                promise.fail(res.cause());
            }
        });

        return promise.future();
    }

    protected HashMap<Resource, Set<Acl>> getResourceAclsMap(String username, Set<SimpleAclRule> aclRules) {
        KafkaPrincipal principal = new KafkaPrincipal("User", username);
        HashMap<Resource, Set<Acl>> map = new HashMap<>();
        for (SimpleAclRule rule: aclRules) {
            Resource resource = rule.getResource().toKafkaResource();
            Set<Acl> aclSet = map.get(resource);
            if (aclSet == null) {
                aclSet = new HashSet<>();
            }
            aclSet.add(rule.toKafkaAcl(principal));
            map.put(resource, aclSet);
        }
        return map;
    }
    /**
     * Deletes all ACLs for given user
     */
    protected Future<ReconcileResult<Set<SimpleAclRule>>> internalDelete(String username, Set<SimpleAclRule> current) {

        try {
            HashMap<Resource, Set<Acl>> map =  getResourceAclsMap(username, current);
            for (Map.Entry<Resource, Set<Acl>> entry: map.entrySet()) {
                scala.collection.mutable.Set<Acl> remove = JavaConverters.asScalaSet(entry.getValue());
                authorizer.removeAcls(remove.toSet(), entry.getKey());
            }
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
        Set<SimpleAclRule> result = new HashSet<SimpleAclRule>();
        KafkaPrincipal principal = new KafkaPrincipal("User", username);

        scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> rules;

        try {
            rules = authorizer.getAcls(principal);
        } catch (Exception e)   {
            log.error("Failed to get existing Acls rules for user {}", username, e);
            throw e;
        }

        Iterator<Tuple2<Resource, scala.collection.immutable.Set<Acl>>> iter = resourceAclsIterator(rules);
        while (iter.hasNext())  {
            Tuple2<Resource, scala.collection.immutable.Set<Acl>> tuple = iter.next();
            SimpleAclRuleResource resource = SimpleAclRuleResource.fromKafkaResource(tuple._1());
            scala.collection.immutable.Set<Acl> acls = tuple._2();

            Iterator<Acl> iter2 = acls.iterator();
            while (iter2.hasNext()) {
                result.add(SimpleAclRule.fromKafkaAcl(resource, iter2.next()));
            }
        }

        return result;
    }

    private Iterator<Tuple2<Resource, scala.collection.immutable.Set<Acl>>> resourceAclsIterator(scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> rules) {
        // this cast fixes an error with VSCode compiler (using the Eclipse JDT Language Server)
        // error details: The method iterator() is ambiguous for the type Map<Resource,Set<Acl>>
        return ((scala.collection.GenIterableLike<Tuple2<Resource, scala.collection.immutable.Set<Acl>>, ?>) rules).iterator();
    }

    /**
     * Returns set with all usernames which have some ACLs.
     *
     * @return The set with all usernames which have some ACLs.
     */
    public Set<String> getUsersWithAcls()   {
        Set<String> result = new HashSet<String>();
        Set<String> ignored = new HashSet<String>(IGNORED_USERS.size());

        log.debug("Searching for Users with any ACL rules");

        scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> rules;

        try {
            rules =  authorizer.getAcls();
        } catch (Exception e)   {
            log.error("Failed to get existing Acls rules all users", e);
            return result;
        }

        Iterator<Tuple2<Resource, scala.collection.immutable.Set<Acl>>> iter = resourceAclsIterator(rules);
        while (iter.hasNext())  {
            scala.collection.immutable.Set<Acl> acls = iter.next()._2();

            Iterator<Acl> iter2 = acls.iterator();
            while (iter2.hasNext()) {
                KafkaPrincipal principal = iter2.next().principal();

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
        }

        return result;
    }
}
