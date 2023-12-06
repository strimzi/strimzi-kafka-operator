/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.api.kafka.model.user.acl.AclResourcePatternType;
import io.strimzi.api.kafka.model.user.acl.AclRuleType;
import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.strimzi.operator.user.model.acl.SimpleAclRuleResource;
import io.strimzi.operator.user.model.acl.SimpleAclRuleResourceType;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class SimpleAclOperatorIT extends AdminApiOperatorIT<Set<SimpleAclRule>, Set<String>> {
    @Override
    AdminApiOperator<Set<SimpleAclRule>, Set<String>> operator() {
        return new SimpleAclOperator(adminClient, ResourceUtils.createUserOperatorConfig(), Executors.newSingleThreadExecutor());
    }

    @Override
    Set<SimpleAclRule> getOriginal() {
        Set<SimpleAclRule> acls = new HashSet<>();

        acls.add(new SimpleAclRule(
                AclRuleType.ALLOW,
                new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL),
                "*",
                AclOperation.DESCRIBE)
        );

        acls.add(new SimpleAclRule(
                AclRuleType.ALLOW,
                new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL),
                "*",
                AclOperation.READ)
        );

        acls.add(new SimpleAclRule(
                AclRuleType.ALLOW,
                new SimpleAclRuleResource("my-group", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.LITERAL),
                "*",
                AclOperation.READ)
        );

        return acls;
    }

    @Override
    Set<SimpleAclRule> getModified() {
        Set<SimpleAclRule> acls = new HashSet<>();

        acls.add(new SimpleAclRule(
                AclRuleType.ALLOW,
                new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL),
                "*",
                AclOperation.DESCRIBE)
        );

        acls.add(new SimpleAclRule(
                AclRuleType.ALLOW,
                new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL),
                "*",
                AclOperation.WRITE)
        );

        acls.add(new SimpleAclRule(
                AclRuleType.ALLOW,
                new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL),
                "*",
                AclOperation.CREATE)
        );

        return acls;
    }

    @Override
    Set<SimpleAclRule> get(String username) {
        KafkaPrincipal principal = new KafkaPrincipal("User", username);
        AclBindingFilter aclBindingFilter = new AclBindingFilter(ResourcePatternFilter.ANY,
                new AccessControlEntryFilter(principal.toString(), null, org.apache.kafka.common.acl.AclOperation.ANY, AclPermissionType.ANY));

        Collection<AclBinding> aclBindings;
        try {
            aclBindings = adminClient.describeAcls(aclBindingFilter).values().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to get ACLs", e);
        }

        Set<SimpleAclRule> result = new HashSet<>(aclBindings.size());

        for (AclBinding aclBinding : aclBindings) {
            result.add(SimpleAclRule.fromAclBinding(aclBinding));
        }

        return result.isEmpty() ? null : result;
    }

    @Override
    void assertResources(Set<SimpleAclRule> expected, Set<SimpleAclRule> actual) {
        assertThat(actual, containsInAnyOrder(expected.toArray()));
    }
}
