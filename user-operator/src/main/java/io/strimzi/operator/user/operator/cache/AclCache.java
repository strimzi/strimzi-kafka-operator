/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.cache;

import io.strimzi.operator.user.model.acl.SimpleAclRule;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A periodically updated ACL Cache for keeping the ACLs locally and avoid querying Kafka
 */
public class AclCache extends AbstractCache<Set<SimpleAclRule>> {
    private final static Logger LOGGER = LogManager.getLogger(AclCache.class);

    private final Admin adminClient;

    /**
     * Constructs the ACL cache
     *
     * @param adminClient           Kafka Admin client
     * @param refreshIntervalMs     Interval in which the cache should be refreshed
     */
    public AclCache(Admin adminClient, long refreshIntervalMs) {
        super("ACL", refreshIntervalMs);
        this.adminClient = adminClient;
    }

    /**
     * Loads the ACL rules from Kafka for all users
     *
     * @return  ConcurrentHashMap with all users and their ACLs
     */
    @Override
    protected ConcurrentHashMap<String, Set<SimpleAclRule>> loadCache() {
        KafkaFuture<Collection<AclBinding>> futureAcls = adminClient.describeAcls(AclBindingFilter.ANY).values();

        try {
            Collection<AclBinding> aclsBindings = futureAcls.get(1, TimeUnit.MINUTES);
            // Each user can have multiple ACL rules. So the size of the map will not directly correspond to the number
            // of rules. But we size it for 3-5 rules per user to give us at least some start and have some better
            // initial size than Java's default
            ConcurrentHashMap<String, Set<SimpleAclRule>> map = new ConcurrentHashMap<>(aclsBindings.size() / 3);

            for (AclBinding aclBinding : aclsBindings) {
                KafkaPrincipal principal = SecurityUtils.parseKafkaPrincipal(aclBinding.entry().principal());

                if (KafkaPrincipal.USER_TYPE.equals(principal.getPrincipalType())) {
                    map.computeIfAbsent(principal.getName(), k -> new HashSet<>()).add(SimpleAclRule.fromAclBinding(aclBinding));
                }
            }

            return map;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOGGER.warn("Failed to load ACLs", e);
            throw new RuntimeException("Failed to load ACLs", e);
        }
    }
}
