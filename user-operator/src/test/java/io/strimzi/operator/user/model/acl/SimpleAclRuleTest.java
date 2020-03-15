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
import kafka.security.auth.Acl;
import kafka.security.auth.Allow$;
import kafka.security.auth.Read$;
import kafka.security.auth.Resource;
import kafka.security.auth.Topic$;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class SimpleAclRuleTest {
    private static AclRuleResource aclRuleTopicResource;
    private static SimpleAclRuleResource resource = new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
    private static Resource kafkaResource = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
    private static KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "my-user");

    static {
        aclRuleTopicResource = new AclRuleTopicResourceBuilder()
            .withName("my-topic")
            .withPatternType(AclResourcePatternType.LITERAL)
            .build();
    }

    @Test
    public void testFromCrd()   {
        AclRule rule = new AclRuleBuilder()
            .withType(AclRuleType.ALLOW)
            .withResource(aclRuleTopicResource)
            .withHost("127.0.0.1")
            .withOperation(AclOperation.READ)
            .build();

        SimpleAclRule simple = SimpleAclRule.fromCrd(rule);
        assertThat(simple.getOperation(), is(AclOperation.READ));
        assertThat(simple.getType(), is(AclRuleType.ALLOW));
        assertThat(simple.getHost(), is("127.0.0.1"));
        assertThat(simple.getResource(), is(resource));
    }

    @Test
    public void testToKafkaAclForSpecifiedKafkaPrincipalReturnsKafkaAclForKafkaPrincipal()   {
        SimpleAclRule kafkaTopicSimpleAclRule = new SimpleAclRule(AclRuleType.ALLOW, resource, "127.0.0.1", AclOperation.READ);
        Acl expectedKafkaAcl = new Acl(kafkaPrincipal, Allow$.MODULE$, "127.0.0.1", Read$.MODULE$);
        assertThat(kafkaTopicSimpleAclRule.toKafkaAcl(kafkaPrincipal), is(expectedKafkaAcl));
    }

    @Test
    public void testFromKafkaForResourceAndKafkaAclReturnsSimpleAclRuleForResource()   {
        Acl kafkaAcl = new Acl(kafkaPrincipal, Allow$.MODULE$, "127.0.0.1", Read$.MODULE$);
        SimpleAclRule expectedSimpleAclRule = new SimpleAclRule(AclRuleType.ALLOW, resource, "127.0.0.1", AclOperation.READ);
        assertThat(SimpleAclRule.fromKafkaAcl(resource, kafkaAcl), is(expectedSimpleAclRule));
    }

    @Test
    public void testFromKafkaAclToKafkaAclRoundtrip()   {
        Acl kafkaAcl = new Acl(kafkaPrincipal, Allow$.MODULE$, "127.0.0.1", Read$.MODULE$);
        assertThat(SimpleAclRule.fromKafkaAcl(resource, kafkaAcl).toKafkaAcl(kafkaPrincipal), is(kafkaAcl));
    }

    @Test
    public void testFromCrdToKafkaAcl()   {
        AclRule rule = new AclRuleBuilder()
            .withType(AclRuleType.ALLOW)
            .withResource(aclRuleTopicResource)
            .withHost("127.0.0.1")
            .withOperation(AclOperation.READ)
            .build();

        Acl expectedKafkaAcl = new Acl(kafkaPrincipal, Allow$.MODULE$, "127.0.0.1", Read$.MODULE$);

        assertThat(SimpleAclRule.fromCrd(rule).toKafkaAcl(kafkaPrincipal), is(expectedKafkaAcl));
    }
}
