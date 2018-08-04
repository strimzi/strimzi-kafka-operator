package io.strimzi.operator.user.operator;

import io.strimzi.api.kafka.model.AclResourcePatternType;
import io.strimzi.api.kafka.model.AclRule;
import io.strimzi.api.kafka.model.AclRuleClusterResource;
import io.strimzi.api.kafka.model.AclRuleGroupResource;
import io.strimzi.api.kafka.model.AclRuleResource;
import io.strimzi.api.kafka.model.AclRuleTopicResource;
import io.strimzi.operator.common.operator.resource.ReconcileResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import kafka.security.auth.Acl;
import kafka.security.auth.All$;
import kafka.security.auth.Allow$;
import kafka.security.auth.Alter$;
import kafka.security.auth.AlterConfigs$;
import kafka.security.auth.Cluster$;
import kafka.security.auth.ClusterAction$;
import kafka.security.auth.Create$;
import kafka.security.auth.Delete$;
import kafka.security.auth.Deny$;
import kafka.security.auth.Describe$;
import kafka.security.auth.DescribeConfigs$;
import kafka.security.auth.Group$;
import kafka.security.auth.IdempotentWrite$;
import kafka.security.auth.Operation;
import kafka.security.auth.PermissionType;
import kafka.security.auth.Read$;
import kafka.security.auth.Resource;
import kafka.security.auth.ResourceType;
import kafka.security.auth.SimpleAclAuthorizer;
import kafka.security.auth.Topic$;
import kafka.security.auth.Write$;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.Tuple2;
import scala.collection.Iterator;

public class SimpleAclOperator {
    private static final Logger log = LogManager.getLogger(SimpleAclOperator.class.getName());

    private final Vertx vertx;
    private final SimpleAclAuthorizer authorizer;

    public SimpleAclOperator(Vertx vertx, String zookeeperConnect, Long zookeeperConnectionTimeoutMs, Long zookeeeprSessionTimeoutMs)  {
        this.vertx = vertx;
        log.debug("Conneting to Zookeeper {}", zookeeperConnect);
        Map authorizerConfig = new HashMap<String, Object>();
        authorizerConfig.put(SimpleAclAuthorizer.ZkUrlProp(), zookeeperConnect);
        authorizerConfig.put("zookeeper.connect", zookeeperConnect);
        authorizerConfig.put(SimpleAclAuthorizer.ZkConnectionTimeOutProp(), zookeeperConnectionTimeoutMs);
        authorizerConfig.put(SimpleAclAuthorizer.ZkSessionTimeOutProp(), zookeeeprSessionTimeoutMs);

        this.authorizer = new SimpleAclAuthorizer();
        this.authorizer.configure(authorizerConfig);
    }

    Future<ReconcileResult<List<AclRule>>> reconcile(String username, List<AclRule> desired) {
        Future<ReconcileResult<List<AclRule>>> fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    List<AclRule> current = getAcls(username);

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
                            log.debug("User {}: {} expected Acl rules and {} existing Acl rules -> Updating rules", username, desired.size(), current.size());
                            // Update rules
                        }
                    }

                    /*T current = operation().inNamespace(namespace).withName(name).get();
                    if (desired != null) {
                        if (current == null) {
                            log.debug("{} {}/{} does not exist, creating it", resourceKind, namespace, name);
                            internalCreate(namespace, name, desired).setHandler(future);
                        } else {
                            log.debug("{} {}/{} already exists, patching it", resourceKind, namespace, name);
                            internalPatch(namespace, name, current, desired).setHandler(future);
                        }
                    } else {
                        if (current != null) {
                            // Deletion is desired
                            log.debug("{} {}/{} exist, deleting it", resourceKind, namespace, name);
                            internalDelete(namespace, name).setHandler(future);
                        } else {
                            log.debug("{} {}/{} does not exist, noop", resourceKind, namespace, name);
                            future.complete(ReconcileResult.noop());
                        }
                    }*/

                },
                false,
                fut.completer()
        );
        return fut;
    }

    /**
     * Create all ACLs for given user
     */
    protected Future<ReconcileResult<List<AclRule>>> internalCreate(String username, List<AclRule> desired) {
        KafkaPrincipal principal = new KafkaPrincipal("User", username);
        for (AclRule rule : desired)    {
            log.trace("Adding Acl rule {}", rule);
            Acl acl = createAclFromAclRule(principal, rule);
            Resource resource = createResourceFromAclResource(rule.getResource());
            scala.collection.immutable.Set<Acl> remove = new scala.collection.immutable.Set.Set1<Acl>(acl);

            authorizer.addAcls(remove, resource);
        }

        return Future.succeededFuture(ReconcileResult.created(desired));
    }

    /**
     * Update all ACLs for given user
     */
    protected Future<ReconcileResult<List<AclRule>>> internalUpdate(String username, List<AclRule> desired, List<AclRule> current) {
        List<AclRule> toBeDeleted = new ArrayList<AclRule>(current);
        toBeDeleted.removeAll(desired);

        List<AclRule> toBeAdded = new ArrayList<AclRule>(desired);
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
    protected Future<ReconcileResult<List<AclRule>>> internalDelete(String username, List<AclRule> current) {
        KafkaPrincipal principal = new KafkaPrincipal("User", username);
        for (AclRule rule : current)    {
            log.trace("Removing Acl rule {}", rule);
            Acl acl = createAclFromAclRule(principal, rule);
            Resource resource = createResourceFromAclResource(rule.getResource());
            scala.collection.immutable.Set<Acl> remove = new scala.collection.immutable.Set.Set1<Acl>(acl);

            authorizer.removeAcls(remove, resource);
        }

        return Future.succeededFuture(ReconcileResult.deleted());
    }

    /**
     * Get List of ACLs applying to single user
     *
     * @param username  Name of the user
     * @return
     */
    public List<AclRule> getAcls(String username)   {
        log.debug("Searching for ACL rules of user {}", username);
        List<AclRule> result = new ArrayList<AclRule>();
        KafkaPrincipal principal = new KafkaPrincipal("User", username);

        scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> rules =  authorizer.getAcls(principal);

        Iterator<Tuple2<Resource, scala.collection.immutable.Set<Acl>>> iter = rules.iterator();
        while (iter.hasNext())  {
            scala.collection.immutable.Set<Acl> acls = iter.next()._2;

            Iterator<Acl> iter2 = acls.iterator();
            while (iter2.hasNext()) {
                Acl rule = iter2.next();
                result.add(new AclRule());
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
            scala.collection.immutable.Set<Acl> acls = iter.next()._2;

            Iterator<Acl> iter2 = acls.iterator();
            while (iter2.hasNext()) {
                KafkaPrincipal principal = iter2.next().principal();

                if (KafkaPrincipal.USER_TYPE.equals(principal.getPrincipalType()))  {
                    if (log.isTraceEnabled())   {
                        log.trace("Adding user {} to list of users with ACLs", principal.getName());
                    }

                    result.add(principal.getName());
                }
            }
        }

        return result;
    }

    private static Acl createAclFromAclRule(KafkaPrincipal principal, AclRule rule)   {
        PermissionType type;
        Operation operation;

        switch (rule.getType()) {
            case DENY:
                type = Deny$.MODULE$;
                break;
            case ALLOW:
                type = Allow$.MODULE$;
                break;
            default:
                throw new IllegalArgumentException("Invalid AclRule type: " + rule.getOperation());
        }

        switch (rule.getOperation()) {
            case READ:
                operation = Read$.MODULE$;
                break;
            case WRITE:
                operation = Write$.MODULE$;
                break;
            case CREATE:
                operation = Create$.MODULE$;
                break;
            case DELETE:
                operation = Delete$.MODULE$;
                break;
            case ALTER:
                operation = Alter$.MODULE$;
                break;
            case DESCRIBE:
                operation = Describe$.MODULE$;
                break;
            case CLUSTERACTION:
                operation = ClusterAction$.MODULE$;
                break;
            case ALTERCONFIGS:
                operation = AlterConfigs$.MODULE$;
                break;
            case DESCRIBECONFIGS:
                operation = DescribeConfigs$.MODULE$;
                break;
            case IDEMPOTENTWRITE:
                operation = IdempotentWrite$.MODULE$;
                break;
            case ALL:
                operation = All$.MODULE$;
                break;
            default:
                throw new IllegalArgumentException("Invalid AclRule operation: " + rule.getOperation());
        }

        return new Acl(principal, type, rule.getHost(), operation);
    }

    private static Resource createResourceFromAclResource(AclRuleResource resource)   {
        ResourceType type;
        String name= "";
        PatternType pattern = PatternType.LITERAL;

        switch (resource.getType()) {
            case AclRuleTopicResource.TYPE_TOPIC:
                type = Topic$.MODULE$;
                AclRuleTopicResource adaptedTopic = ((AclRuleTopicResource) resource);
                name = adaptedTopic.getName();

                if (AclResourcePatternType.PREFIX.equals(adaptedTopic.getPatternType()))   {
                    pattern = PatternType.PREFIXED;
                }

                break;
            case AclRuleGroupResource.TYPE_GROUP:
                type = Group$.MODULE$;
                AclRuleGroupResource adaptedGroup = ((AclRuleGroupResource) resource);
                name = adaptedGroup.getName();

                if (AclResourcePatternType.PREFIX.equals(adaptedGroup.getPatternType()))   {
                    pattern = PatternType.PREFIXED;
                }

                break;
            case AclRuleClusterResource.TYPE_CLUSTER:
                type = Cluster$.MODULE$;
                break;
            default:
                throw new IllegalArgumentException("Invalid AclRuleResource type: " + resource.getType());
        }

        return new Resource(type, name, pattern);
    }
}
