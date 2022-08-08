/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model.acl;

import io.strimzi.api.kafka.model.AclOperation;
import io.strimzi.api.kafka.model.AclResourcePatternType;
import io.strimzi.api.kafka.model.AclRule;
import io.strimzi.api.kafka.model.AclRuleBuilder;
import io.strimzi.api.kafka.model.AclRuleResource;
import io.strimzi.api.kafka.model.AclRuleTopicResourceBuilder;
import io.strimzi.api.kafka.model.AclRuleType;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SimpleAclRuleTest {
    private static final AclRuleResource ACL_RULE_TOPIC_RESOURCE;
    private static final SimpleAclRuleResource RESOURCE = new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
    private static final ResourcePattern KAFKA_RESOURCE_PATTERN = new ResourcePattern(ResourceType.TOPIC, "my-topic", PatternType.LITERAL);
    private static final KafkaPrincipal KAFKA_PRINCIPAL = new KafkaPrincipal("User", "my-user");

    static {
        ACL_RULE_TOPIC_RESOURCE = new AclRuleTopicResourceBuilder()
            .withName("my-topic")
            .withPatternType(AclResourcePatternType.LITERAL)
            .build();
    }

    @Test
    public void testFromCrd()   {
        AclRule rule = new AclRuleBuilder()
            .withType(AclRuleType.ALLOW)
            .withResource(ACL_RULE_TOPIC_RESOURCE)
            .withHost("127.0.0.1")
            .withOperation(AclOperation.READ)
            .build();

        List<SimpleAclRule> simpleAclRules = SimpleAclRule.fromCrd(rule);
        assertThat(simpleAclRules.get(0).getOperation(), is(AclOperation.READ));
        assertThat(simpleAclRules.get(0).getType(), is(AclRuleType.ALLOW));
        assertThat(simpleAclRules.get(0).getHost(), is("127.0.0.1"));
        assertThat(simpleAclRules.get(0).getResource(), is(RESOURCE));
    }

    @Test
    public void testFromCrdWithBothOperationsAndOperationSetAtTheSameTime()   {
        assertThrows(InvalidResourceException.class, () -> {
            AclRule rule = new AclRuleBuilder()
                .withType(AclRuleType.ALLOW)
                .withResource(ACL_RULE_TOPIC_RESOURCE)
                .withHost("127.0.0.1")
                .withOperation(AclOperation.READ)
                .withOperations(AclOperation.READ, AclOperation.WRITE, AclOperation.DESCRIBECONFIGS)
                .build();

            SimpleAclRule.fromCrd(rule);
        });
    }

    @Test
    public void testFromCrdWithThreeOperationsPerRule()   {
        AclRule rule = new AclRuleBuilder()
            .withType(AclRuleType.ALLOW)
            .withResource(ACL_RULE_TOPIC_RESOURCE)
            .withHost("127.0.0.1")
            .withOperations(AclOperation.READ, AclOperation.WRITE, AclOperation.DESCRIBE)
            .build();

        List<SimpleAclRule> simpleAclRules = SimpleAclRule.fromCrd(rule);
        assertThat(simpleAclRules.size(), is(3));
        assertThat(simpleAclRules.get(0).getOperation(), is(AclOperation.READ));
        assertThat(simpleAclRules.get(1).getOperation(), is(AclOperation.WRITE));
        assertThat(simpleAclRules.get(2).getOperation(), is(AclOperation.DESCRIBE));
        assertThat(simpleAclRules.stream().map(SimpleAclRule::getHost).collect(Collectors.toSet()), is(Set.of("127.0.0.1")));
        assertThat(simpleAclRules.stream().map(SimpleAclRule::getResource).collect(Collectors.toSet()), is(Set.of(RESOURCE)));
        assertThat(simpleAclRules.stream().map(SimpleAclRule::getType).collect(Collectors.toSet()), is(Set.of(AclRuleType.ALLOW)));
    }

    @Test
    public void testToKafkaAclBindingForSpecifiedKafkaPrincipalReturnsKafkaAclBindingForKafkaPrincipal() {
        SimpleAclRule kafkaTopicSimpleAclRule = new SimpleAclRule(AclRuleType.ALLOW, RESOURCE, "127.0.0.1", AclOperation.READ);
        AclBinding expectedAclBinding = new AclBinding(
                KAFKA_RESOURCE_PATTERN,
                new AccessControlEntry(KAFKA_PRINCIPAL.toString(), "127.0.0.1",
                        org.apache.kafka.common.acl.AclOperation.READ, AclPermissionType.ALLOW)
        );
        assertThat(kafkaTopicSimpleAclRule.toKafkaAclBinding(KAFKA_PRINCIPAL), is(expectedAclBinding));
    }

    @Test
    public void testFromAclBindingReturnsSimpleAclRule() {
        AclBinding aclBinding = new AclBinding(
                KAFKA_RESOURCE_PATTERN,
                new AccessControlEntry(KAFKA_PRINCIPAL.toString(), "127.0.0.1",
                        org.apache.kafka.common.acl.AclOperation.READ, AclPermissionType.ALLOW)
        );
        SimpleAclRule expectedSimpleAclRule = new SimpleAclRule(AclRuleType.ALLOW, RESOURCE, "127.0.0.1", AclOperation.READ);
        assertThat(SimpleAclRule.fromAclBinding(aclBinding), is(expectedSimpleAclRule));
    }

    @Test
    public void testFromKafkaAclBindingToKafkaAclBindingRoundTrip()   {
        AclBinding kafkaAclBinding = new AclBinding(
                KAFKA_RESOURCE_PATTERN,
                new AccessControlEntry(KAFKA_PRINCIPAL.toString(), "127.0.0.1",
                        org.apache.kafka.common.acl.AclOperation.READ, AclPermissionType.ALLOW)
        );
        assertThat(SimpleAclRule.fromAclBinding(kafkaAclBinding).toKafkaAclBinding(KAFKA_PRINCIPAL), is(kafkaAclBinding));
    }

    @Test
    public void testFromCrdToKafkaAclBinding()   {
        AclRule rule = new AclRuleBuilder()
            .withType(AclRuleType.ALLOW)
            .withResource(ACL_RULE_TOPIC_RESOURCE)
            .withHost("127.0.0.1")
            .withOperation(AclOperation.READ)
            .build();

        AclBinding expectedKafkaAclBinding = new AclBinding(
                KAFKA_RESOURCE_PATTERN,
                new AccessControlEntry(KAFKA_PRINCIPAL.toString(), "127.0.0.1",
                        org.apache.kafka.common.acl.AclOperation.READ, AclPermissionType.ALLOW)
        );
        assertThat(SimpleAclRule.fromCrd(rule).stream().map(simpleAclRule -> simpleAclRule.toKafkaAclBinding(KAFKA_PRINCIPAL)).collect(Collectors.toSet()), is(Set.of(expectedKafkaAclBinding)));
    }

    @Test
    public void testFromCrdWithThreeOperationsPerRuleToKafkaAclBinding()   {
        AclRule rule = new AclRuleBuilder()
            .withType(AclRuleType.ALLOW)
            .withResource(ACL_RULE_TOPIC_RESOURCE)
            .withHost("127.0.0.1")
            .withOperations(AclOperation.READ, AclOperation.WRITE, AclOperation.DESCRIBE)
            .build();

        AclBinding expectedKafkaAclBindingWithReadOperation = new AclBinding(
                KAFKA_RESOURCE_PATTERN,
                new AccessControlEntry(KAFKA_PRINCIPAL.toString(), "127.0.0.1",
                        org.apache.kafka.common.acl.AclOperation.READ, AclPermissionType.ALLOW)
        );
        AclBinding expectedKafkaAclBindingWithWriteOperation = new AclBinding(
                KAFKA_RESOURCE_PATTERN,
                new AccessControlEntry(KAFKA_PRINCIPAL.toString(), "127.0.0.1",
                        org.apache.kafka.common.acl.AclOperation.WRITE, AclPermissionType.ALLOW)
        );
        AclBinding expectedKafkaAclBindingWithDescribeOperation = new AclBinding(
                KAFKA_RESOURCE_PATTERN,
                new AccessControlEntry(KAFKA_PRINCIPAL.toString(), "127.0.0.1",
                        org.apache.kafka.common.acl.AclOperation.DESCRIBE, AclPermissionType.ALLOW)
        );
        assertThat(SimpleAclRule.fromCrd(rule).stream().map(simpleAclRule -> simpleAclRule.toKafkaAclBinding(KAFKA_PRINCIPAL)).collect(Collectors.toList()), is(List.of(expectedKafkaAclBindingWithReadOperation, expectedKafkaAclBindingWithWriteOperation, expectedKafkaAclBindingWithDescribeOperation)));
    }
}
