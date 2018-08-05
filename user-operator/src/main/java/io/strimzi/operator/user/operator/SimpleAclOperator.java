/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.strimzi.operator.user.model.acl.SimpleAclRuleResource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import kafka.security.auth.Acl;
import kafka.security.auth.Resource;
import kafka.security.auth.SimpleAclAuthorizer;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.Tuple2;
import scala.collection.Iterator;

public class SimpleAclOperator {
    private static final Logger log = LogManager.getLogger(SimpleAclOperator.class.getName());

    private final Vertx vertx;
    private final SimpleAclAuthorizer authorizer;

    public SimpleAclOperator(Vertx vertx, SimpleAclAuthorizer authorizer)  {
        this.vertx = vertx;
        this.authorizer = authorizer;
    }

    Future<ReconcileResult<Set<SimpleAclRule>>> reconcile(String username, Set<SimpleAclRule> desired) {
        Future<ReconcileResult<Set<SimpleAclRule>>> fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                Set<SimpleAclRule> current = getAcls(username);

                if (desired == null || desired.isEmpty()) {
                    if (current.size() == 0)    {
                        log.debug("User {}: No expected Acl rules and no existing Acl rules -> NoOp", username);
                        future.complete(ReconcileResult.noop());
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
            fut.completer()
        );
        return fut;
    }

    /**
     * Create all ACLs for given user
     */
    protected Future<ReconcileResult<Set<SimpleAclRule>>> internalCreate(String username, Set<SimpleAclRule> desired) {
        KafkaPrincipal principal = new KafkaPrincipal("User", username);
        for (SimpleAclRule rule : desired)    {
            if (log.isTraceEnabled()) {
                log.trace("Adding Acl rule {}", rule);
            }

            Acl acl = rule.toKafkaAcl(principal);
            Resource resource = rule.getResource().toKafkaResource();
            scala.collection.immutable.Set<Acl> add = new scala.collection.immutable.Set.Set1<Acl>(acl);

            authorizer.addAcls(add, resource);
        }

        return Future.succeededFuture(ReconcileResult.created(desired));
    }

    /**
     * Update all ACLs for given user
     */
    protected Future<ReconcileResult<Set<SimpleAclRule>>> internalUpdate(String username, Set<SimpleAclRule> desired, Set<SimpleAclRule> current) {
        Set<SimpleAclRule> toBeDeleted = new HashSet<SimpleAclRule>(current);
        toBeDeleted.removeAll(desired);

        Set<SimpleAclRule> toBeAdded = new HashSet<SimpleAclRule>(desired);
        toBeAdded.removeAll(current);

        List<Future> updates = new ArrayList<>(2);
        updates.add(internalDelete(username, toBeDeleted));
        updates.add(internalCreate(username, toBeAdded));

        Future fut = Future.future();

        CompositeFuture.all(updates).setHandler(res -> {
            if (res.succeeded())    {
                fut.complete(ReconcileResult.patched(desired));
            } else  {
                fut.fail(res.cause());
            }
        });

        return fut;
    }

    /**
     * Deletes all ACLs for given user
     */
    protected Future<ReconcileResult<Set<SimpleAclRule>>> internalDelete(String username, Set<SimpleAclRule> current) {
        KafkaPrincipal principal = new KafkaPrincipal("User", username);
        for (SimpleAclRule rule : current)    {
            if (log.isTraceEnabled()) {
                log.trace("Removing Acl rule {}", rule);
            }

            Acl acl = rule.toKafkaAcl(principal);
            Resource resource = rule.getResource().toKafkaResource();
            scala.collection.immutable.Set<Acl> remove = new scala.collection.immutable.Set.Set1<Acl>(acl);

            authorizer.removeAcls(remove, resource);
        }

        return Future.succeededFuture(ReconcileResult.deleted());
    }

    /**
     * Get Set of ACLs applying to single user
     *
     * @param username  Name of the user
     * @return
     */
    public Set<SimpleAclRule> getAcls(String username)   {
        log.debug("Searching for ACL rules of user {}", username);
        Set<SimpleAclRule> result = new HashSet<SimpleAclRule>();
        KafkaPrincipal principal = new KafkaPrincipal("User", username);

        scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> rules =  authorizer.getAcls(principal);

        Iterator<Tuple2<Resource, scala.collection.immutable.Set<Acl>>> iter = rules.iterator();
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

    /**
     * Returns set with all usernames which have some ACLs
     *
     * @return
     */
    public Set<String> getUsersWithAcls()   {
        Set<String> result = new HashSet<String>();

        log.debug("Searching for Users with any ACL rules");
        scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> rules =  authorizer.getAcls();

        Iterator<Tuple2<Resource, scala.collection.immutable.Set<Acl>>> iter = rules.iterator();
        while (iter.hasNext())  {
            scala.collection.immutable.Set<Acl> acls = iter.next()._2();

            Iterator<Acl> iter2 = acls.iterator();
            while (iter2.hasNext()) {
                KafkaPrincipal principal = iter2.next().principal();

                if (KafkaPrincipal.USER_TYPE.equals(principal.getPrincipalType()))  {
                    // Username in ACL might keep different format (for exmaple based on user's subject) and need to be decoded
                    String username = KafkaUserModel.decodeUsername(principal.getName());

                    if (log.isTraceEnabled())   {
                        log.trace("Adding user {} to Set of users with ACLs", username);
                    }

                    result.add(username);
                }
            }
        }

        return result;
    }
}
