/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model.acl;

import io.strimzi.api.kafka.model.AclResourcePatternType;
import io.strimzi.api.kafka.model.AclRuleClusterResource;
import io.strimzi.api.kafka.model.AclRuleGroupResourceBuilder;
import io.strimzi.api.kafka.model.AclRuleResource;
import io.strimzi.api.kafka.model.AclRuleTopicResourceBuilder;
import io.strimzi.api.kafka.model.AclRuleTransactionalIdResourceBuilder;
import kafka.security.auth.Cluster$;
import kafka.security.auth.Group$;
import kafka.security.auth.Resource;
import kafka.security.auth.Topic$;
import kafka.security.auth.TransactionalId$;
import org.apache.kafka.common.resource.PatternType;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class SimpleAclRuleResourceTest {
    @Test
    public void testFromCrdWithTopicResource()   {
        // Regular topic
        AclRuleResource regularTopic = new AclRuleTopicResourceBuilder()
            .withName("my-topic")
            .withPatternType(AclResourcePatternType.LITERAL)
            .build();

        SimpleAclRuleResource simple = SimpleAclRuleResource.fromCrd(regularTopic);
        assertThat(simple.getName(), is("my-topic"));
        assertThat(simple.getType(), is(SimpleAclRuleResourceType.TOPIC));
        assertThat(simple.getPattern(), is(AclResourcePatternType.LITERAL));

        // Prefixed topic
        AclRuleResource prefixedTopic = new AclRuleTopicResourceBuilder()
            .withName("my-")
            .withPatternType(AclResourcePatternType.PREFIX)
            .build();

        simple = SimpleAclRuleResource.fromCrd(prefixedTopic);
        assertThat(simple.getName(), is("my-"));
        assertThat(simple.getType(), is(SimpleAclRuleResourceType.TOPIC));
        assertThat(simple.getPattern(), is(AclResourcePatternType.PREFIX));
    }

    @Test
    public void testFromCrdWithGroupResource()   {
        // Regular group
        AclRuleResource regularGroup = new AclRuleGroupResourceBuilder()
            .withName("my-group")
            .withPatternType(AclResourcePatternType.LITERAL)
            .build();

        SimpleAclRuleResource simple = SimpleAclRuleResource.fromCrd(regularGroup);
        assertThat(simple.getName(), is("my-group"));
        assertThat(simple.getType(), is(SimpleAclRuleResourceType.GROUP));
        assertThat(simple.getPattern(), is(AclResourcePatternType.LITERAL));

        // Prefixed group
        AclRuleResource prefixedGroup = new AclRuleGroupResourceBuilder()
            .withName("my-")
            .withPatternType(AclResourcePatternType.PREFIX)
            .build();

        simple = SimpleAclRuleResource.fromCrd(prefixedGroup);
        assertThat(simple.getName(), is("my-"));
        assertThat(simple.getType(), is(SimpleAclRuleResourceType.GROUP));
        assertThat(simple.getPattern(), is(AclResourcePatternType.PREFIX));
    }

    @Test
    public void testFromCrdWithTransactionalIdResource()   {
        // Regular transactionalId
        AclRuleResource resource = new AclRuleTransactionalIdResourceBuilder()
            .withName("my-transactionalId")
            .build();

        SimpleAclRuleResource simple = SimpleAclRuleResource.fromCrd(resource);
        assertThat(simple.getName(), is("my-transactionalId"));
        assertThat(simple.getType(), is(SimpleAclRuleResourceType.TRANSACTIONAL_ID));
        assertThat(simple.getPattern(), is(AclResourcePatternType.LITERAL));
    }

    @Test
    public void testFromCrdWithClusterResource()   {
        // Regular cluster
        AclRuleResource resource = new AclRuleClusterResource();

        SimpleAclRuleResource simple = SimpleAclRuleResource.fromCrd(resource);
        assertThat(simple.getName(), is("kafka-cluster"));
        assertThat(simple.getType(), is(SimpleAclRuleResourceType.CLUSTER));
        assertThat(simple.getPattern(), is(AclResourcePatternType.LITERAL));
    }

    @Test
    public void testToKafkaResourceForTopicResource()  {
        // Regular topic
        SimpleAclRuleResource topicResourceRules = new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
        Resource expectedKafkaResource = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        assertThat(topicResourceRules.toKafkaResource(), is(expectedKafkaResource));

        // Prefixed topic
        topicResourceRules = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.PREFIX);
        expectedKafkaResource = new Resource(Topic$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(topicResourceRules.toKafkaResource(), is(expectedKafkaResource));
    }

    @Test
    public void testToKafkaResourceForGroupResource()  {
        // Regular group
        SimpleAclRuleResource groupResourceRules = new SimpleAclRuleResource("my-group", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.LITERAL);
        Resource expectedKafkaResource = new Resource(Group$.MODULE$, "my-group", PatternType.LITERAL);
        assertThat(groupResourceRules.toKafkaResource(), is(expectedKafkaResource));

        // Prefixed group
        groupResourceRules = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.PREFIX);
        expectedKafkaResource = new Resource(Group$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(groupResourceRules.toKafkaResource(), is(expectedKafkaResource));
    }

    @Test
    public void testToKafkaResourceForClusterResource()  {
        // Regular cluster
        SimpleAclRuleResource clusterResourceRules = new SimpleAclRuleResource(null, SimpleAclRuleResourceType.CLUSTER, null);
        Resource expectedKafkaResource = new Resource(Cluster$.MODULE$, "kafka-cluster", PatternType.LITERAL);
        assertThat(clusterResourceRules.toKafkaResource(), is(expectedKafkaResource));
    }

    @Test
    public void testToKafkaResourceForTransactionalIdResource()  {
        // Regular transactionalId
        SimpleAclRuleResource transactionalIdResourceRules = new SimpleAclRuleResource("my-transactionalId", SimpleAclRuleResourceType.TRANSACTIONAL_ID, null);
        Resource expectedKafkaResource = new Resource(TransactionalId$.MODULE$, "my-transactionalId", PatternType.LITERAL);
        assertThat(transactionalIdResourceRules.toKafkaResource(), is(expectedKafkaResource));

        // Prefixed transactionalId
        transactionalIdResourceRules = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TRANSACTIONAL_ID, AclResourcePatternType.PREFIX);
        expectedKafkaResource = new Resource(TransactionalId$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(transactionalIdResourceRules.toKafkaResource(), is(expectedKafkaResource));
    }

    @Test
    public void testFromKafkaResourceWithTopicResource()  {
        // Regular topic
        Resource kafkaTopicResource = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        SimpleAclRuleResource expectedTopicResourceRules = new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafkaTopicResource), is(expectedTopicResourceRules));

        // Prefixed topic
        kafkaTopicResource = new Resource(Topic$.MODULE$, "my-", PatternType.PREFIXED);
        expectedTopicResourceRules = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.PREFIX);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafkaTopicResource), is(expectedTopicResourceRules));
    }

    @Test
    public void testFromKafkaResourceWithGroupResource()  {
        // Regular group
        Resource kafkaGroupResource = new Resource(Group$.MODULE$, "my-group", PatternType.LITERAL);
        SimpleAclRuleResource expectedGroupResourceRules = new SimpleAclRuleResource("my-group", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafkaGroupResource), is(expectedGroupResourceRules));

        // Prefixed group
        kafkaGroupResource = new Resource(Group$.MODULE$, "my-", PatternType.PREFIXED);
        expectedGroupResourceRules = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.PREFIX);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafkaGroupResource), is(expectedGroupResourceRules));
    }

    @Test
    public void testFromKafkaResourceWithClusterResource()  {
        // Regular cluster
        Resource kafkaClusterResource = new Resource(Cluster$.MODULE$, "kafka-cluster", PatternType.LITERAL);
        SimpleAclRuleResource expectedClusterResourceRules = new SimpleAclRuleResource("kafka-cluster", SimpleAclRuleResourceType.CLUSTER, AclResourcePatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafkaClusterResource), is(expectedClusterResourceRules));
    }

    @Test
    public void testFromKafkaResourceWithTransactionalIdResource()  {
        // Regular transactionalId
        Resource kafkaTransactionalIdResource = new Resource(TransactionalId$.MODULE$, "my-transactionalId", PatternType.LITERAL);
        SimpleAclRuleResource expectedTransactionalIdResourceRules = new SimpleAclRuleResource("my-transactionalId", SimpleAclRuleResourceType.TRANSACTIONAL_ID, AclResourcePatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafkaTransactionalIdResource), is(expectedTransactionalIdResourceRules));

        // Prefixed transactionalId
        kafkaTransactionalIdResource = new Resource(TransactionalId$.MODULE$, "my-", PatternType.PREFIXED);
        expectedTransactionalIdResourceRules = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TRANSACTIONAL_ID, AclResourcePatternType.PREFIX);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafkaTransactionalIdResource), is(expectedTransactionalIdResourceRules));
    }

    @Test
    public void testFromKafkaResourceToKafkaResourceRoundTripForTopicResource()    {
        // Regular group
        Resource kafka = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource(), is(kafka));

        // Prefixed topic
        kafka = new Resource(Topic$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource(), is(kafka));
    }

    @Test
    public void testFromKafkaResourceToKafkaResourceRoundTripForGroupResource()  {
        // Regular group
        Resource kafka = new Resource(Group$.MODULE$, "my-group", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource(), is(kafka));

        // Prefixed group
        kafka = new Resource(Group$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource(), is(kafka));
    }

    @Test
    public void testFromKafkaResourceToKafkaResourceRoundTripForClusterResource()  {
        // Regular cluster
        Resource kafka = new Resource(Cluster$.MODULE$, "kafka-cluster", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource(), is(kafka));
    }

    @Test
    public void testFromKafkaResourceToKafkaResourceRoundTripForTransactionalIdResource()  {
        // Regular transactionID
        Resource kafka = new Resource(TransactionalId$.MODULE$, "my-transactionID", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource(), is(kafka));

        // Prefixed transactionID
        kafka = new Resource(TransactionalId$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource(), is(kafka));
    }

    @Test
    public void testFromCrdToKafkaResourceForTopicResource()    {
        // Regular group
        AclRuleResource resource = new AclRuleTopicResourceBuilder()
            .withName("my-topic")
            .withPatternType(AclResourcePatternType.LITERAL)
            .build();
        Resource expectedKafkaTopicResource = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResource(), is(expectedKafkaTopicResource));

        // Prefixed topic
        resource = new AclRuleTopicResourceBuilder()
            .withName("my-")
            .withPatternType(AclResourcePatternType.PREFIX)
            .build();
        expectedKafkaTopicResource = new Resource(Topic$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResource(), is(expectedKafkaTopicResource));
    }

    @Test
    public void testFromCrdToKafkaResourceForGroupResource()  {
        // Regular group
        AclRuleResource resource = new AclRuleGroupResourceBuilder()
            .withName("my-group")
            .withPatternType(AclResourcePatternType.LITERAL)
            .build();
        Resource expectedKafkaGroupResource = new Resource(Group$.MODULE$, "my-group", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResource(), is(expectedKafkaGroupResource));

        // Prefixed group
        resource = new AclRuleGroupResourceBuilder()
            .withName("my-")
            .withPatternType(AclResourcePatternType.PREFIX)
            .build();
        expectedKafkaGroupResource = new Resource(Group$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResource(), is(expectedKafkaGroupResource));
    }

    @Test
    public void testFromCrdToKafkaResourceForClusterResource()  {
        // Regular cluster
        AclRuleResource resource = new AclRuleClusterResource();
        Resource expectedKafkaClusterResource = new Resource(Cluster$.MODULE$, "kafka-cluster", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResource(), is(expectedKafkaClusterResource));
    }

    @Test
    public void testFromCrdToKafkaResourceForTransactionalIdResource()  {
        // Regular transactionalId
        AclRuleResource resource = new AclRuleTransactionalIdResourceBuilder()
            .withName("my-transactionalId")
            .build();
        Resource expectedKafkaTransactionalIdResource = new Resource(TransactionalId$.MODULE$, "my-transactionalId", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResource(), is(expectedKafkaTransactionalIdResource));
    }
}
