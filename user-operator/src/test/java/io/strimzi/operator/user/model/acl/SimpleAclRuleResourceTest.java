/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model.acl;

import io.strimzi.api.kafka.model.user.acl.AclResourcePatternType;
import io.strimzi.api.kafka.model.user.acl.AclRuleClusterResource;
import io.strimzi.api.kafka.model.user.acl.AclRuleGroupResourceBuilder;
import io.strimzi.api.kafka.model.user.acl.AclRuleResource;
import io.strimzi.api.kafka.model.user.acl.AclRuleTopicResourceBuilder;
import io.strimzi.api.kafka.model.user.acl.AclRuleTransactionalIdResourceBuilder;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;


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
    public void testToKafkaResourcePatternForTopicResource()  {
        // Regular topic
        SimpleAclRuleResource topicResourceRules = new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
        ResourcePattern expectedKafkaResourcePattern = new ResourcePattern(ResourceType.TOPIC, "my-topic", PatternType.LITERAL);
        assertThat(topicResourceRules.toKafkaResourcePattern(), is(expectedKafkaResourcePattern));

        // Prefixed topic
        topicResourceRules = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.PREFIX);
        expectedKafkaResourcePattern = new ResourcePattern(ResourceType.TOPIC, "my-", PatternType.PREFIXED);
        assertThat(topicResourceRules.toKafkaResourcePattern(), is(expectedKafkaResourcePattern));
    }

    @Test
    public void testToKafkaResourcePatternForGroupResource()  {
        // Regular group
        SimpleAclRuleResource groupResourceRules = new SimpleAclRuleResource("my-group", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.LITERAL);
        ResourcePattern expectedKafkaResourcePattern = new ResourcePattern(ResourceType.GROUP, "my-group", PatternType.LITERAL);
        assertThat(groupResourceRules.toKafkaResourcePattern(), is(expectedKafkaResourcePattern));

        // Prefixed group
        groupResourceRules = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.PREFIX);
        expectedKafkaResourcePattern = new ResourcePattern(ResourceType.GROUP, "my-", PatternType.PREFIXED);
        assertThat(groupResourceRules.toKafkaResourcePattern(), is(expectedKafkaResourcePattern));
    }

    @Test
    public void testToKafkaResourcePatternForClusterResource()  {
        // Regular cluster
        SimpleAclRuleResource clusterResourceRules = new SimpleAclRuleResource(null, SimpleAclRuleResourceType.CLUSTER, null);
        ResourcePattern expectedKafkaResourcePattern = new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL);
        assertThat(clusterResourceRules.toKafkaResourcePattern(), is(expectedKafkaResourcePattern));
    }

    @Test
    public void testToKafkaResourcePatternForTransactionalIdResource()  {
        // Regular transactionalId
        SimpleAclRuleResource transactionalIdResourceRules = new SimpleAclRuleResource("my-transactionalId", SimpleAclRuleResourceType.TRANSACTIONAL_ID, null);
        ResourcePattern expectedKafkaResourcePattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "my-transactionalId", PatternType.LITERAL);
        assertThat(transactionalIdResourceRules.toKafkaResourcePattern(), is(expectedKafkaResourcePattern));

        // Prefixed transactionalId
        transactionalIdResourceRules = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TRANSACTIONAL_ID, AclResourcePatternType.PREFIX);
        expectedKafkaResourcePattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "my-", PatternType.PREFIXED);
        assertThat(transactionalIdResourceRules.toKafkaResourcePattern(), is(expectedKafkaResourcePattern));
    }

    @Test
    public void testFromKafkaResourcePatternWithTopicResource()  {
        // Regular topic
        ResourcePattern kafkaTopicResourcePattern = new ResourcePattern(ResourceType.TOPIC, "my-topic", PatternType.LITERAL);
        SimpleAclRuleResource expectedTopicResourceRules = new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResourcePattern(kafkaTopicResourcePattern), is(expectedTopicResourceRules));

        // Prefixed topic
        kafkaTopicResourcePattern = new ResourcePattern(ResourceType.TOPIC, "my-", PatternType.PREFIXED);
        expectedTopicResourceRules = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.PREFIX);
        assertThat(SimpleAclRuleResource.fromKafkaResourcePattern(kafkaTopicResourcePattern), is(expectedTopicResourceRules));
    }

    @Test
    public void testFromKafkaResourcePatternWithGroupResource()  {
        // Regular group
        ResourcePattern kafkaGroupResourcePattern = new ResourcePattern(ResourceType.GROUP, "my-group", PatternType.LITERAL);
        SimpleAclRuleResource expectedGroupResourceRules = new SimpleAclRuleResource("my-group", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResourcePattern(kafkaGroupResourcePattern), is(expectedGroupResourceRules));

        // Prefixed group
        kafkaGroupResourcePattern = new ResourcePattern(ResourceType.GROUP, "my-", PatternType.PREFIXED);
        expectedGroupResourceRules = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.PREFIX);
        assertThat(SimpleAclRuleResource.fromKafkaResourcePattern(kafkaGroupResourcePattern), is(expectedGroupResourceRules));
    }

    @Test
    public void testFromKafkaResourcePatternWithClusterResource()  {
        // Regular cluster
        ResourcePattern kafkaClusterResourcePattern = new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL);
        SimpleAclRuleResource expectedClusterResourceRules = new SimpleAclRuleResource("kafka-cluster", SimpleAclRuleResourceType.CLUSTER, AclResourcePatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResourcePattern(kafkaClusterResourcePattern), is(expectedClusterResourceRules));
    }

    @Test
    public void testFromKafkaResourcePatternWithTransactionalIdResource()  {
        // Regular transactionalId
        ResourcePattern kafkaTransactionalIdResourcePattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "my-transactionalId", PatternType.LITERAL);
        SimpleAclRuleResource expectedTransactionalIdResourceRules = new SimpleAclRuleResource("my-transactionalId", SimpleAclRuleResourceType.TRANSACTIONAL_ID, AclResourcePatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResourcePattern(kafkaTransactionalIdResourcePattern), is(expectedTransactionalIdResourceRules));

        // Prefixed transactionalId
        kafkaTransactionalIdResourcePattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "my-", PatternType.PREFIXED);
        expectedTransactionalIdResourceRules = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TRANSACTIONAL_ID, AclResourcePatternType.PREFIX);
        assertThat(SimpleAclRuleResource.fromKafkaResourcePattern(kafkaTransactionalIdResourcePattern), is(expectedTransactionalIdResourceRules));
    }

    @Test
    public void testFromKafkaResourcePatternToKafkaResourcePatternRoundTripForTopicResource()    {
        // Regular topic
        ResourcePattern kafka = new ResourcePattern(ResourceType.TOPIC, "my-topic", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResourcePattern(kafka).toKafkaResourcePattern(), is(kafka));

        // Prefixed topic
        kafka = new ResourcePattern(ResourceType.TOPIC, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromKafkaResourcePattern(kafka).toKafkaResourcePattern(), is(kafka));
    }

    @Test
    public void testFromKafkaResourcePatternToKafkaResourcePatternRoundTripForGroupResource()  {
        // Regular group
        ResourcePattern kafka = new ResourcePattern(ResourceType.GROUP, "my-group", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResourcePattern(kafka).toKafkaResourcePattern(), is(kafka));

        // Prefixed group
        kafka = new ResourcePattern(ResourceType.GROUP, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromKafkaResourcePattern(kafka).toKafkaResourcePattern(), is(kafka));
    }

    @Test
    public void testFromKafkaResourcePatternToKafkaResourcePatternRoundTripForClusterResource()  {
        // Regular cluster
        ResourcePattern kafka = new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResourcePattern(kafka).toKafkaResourcePattern(), is(kafka));
    }

    @Test
    public void testFromKafkaResourcePatternToKafkaResourcePatternRoundTripForTransactionalIdResource()  {
        // Regular transactionID
        ResourcePattern kafka = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "my-transactionID", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResourcePattern(kafka).toKafkaResourcePattern(), is(kafka));

        // Prefixed transactionID
        kafka = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromKafkaResourcePattern(kafka).toKafkaResourcePattern(), is(kafka));
    }

    @Test
    public void testFromCrdToKafkaResourcePatternForTopicResource()    {
        // Regular group
        AclRuleResource resource = new AclRuleTopicResourceBuilder()
                .withName("my-topic")
                .withPatternType(AclResourcePatternType.LITERAL)
                .build();
        ResourcePattern expectedKafkaTopicResourcePattern = new ResourcePattern(ResourceType.TOPIC, "my-topic", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResourcePattern(), is(expectedKafkaTopicResourcePattern));

        // Prefixed topic
        resource = new AclRuleTopicResourceBuilder()
                .withName("my-")
                .withPatternType(AclResourcePatternType.PREFIX)
                .build();
        expectedKafkaTopicResourcePattern = new ResourcePattern(ResourceType.TOPIC, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResourcePattern(), is(expectedKafkaTopicResourcePattern));
    }

    @Test
    public void testFromCrdToKafkaResourcePatternForGroupResource()  {
        // Regular group
        AclRuleResource resource = new AclRuleGroupResourceBuilder()
                .withName("my-group")
                .withPatternType(AclResourcePatternType.LITERAL)
                .build();
        ResourcePattern expectedKafkaGroupResourcePattern = new ResourcePattern(ResourceType.GROUP, "my-group", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResourcePattern(), is(expectedKafkaGroupResourcePattern));

        // Prefixed group
        resource = new AclRuleGroupResourceBuilder()
                .withName("my-")
                .withPatternType(AclResourcePatternType.PREFIX)
                .build();
        expectedKafkaGroupResourcePattern = new ResourcePattern(ResourceType.GROUP, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResourcePattern(), is(expectedKafkaGroupResourcePattern));
    }

    @Test
    public void testFromCrdToKafkaResourcePatternForClusterResource()  {
        // Regular cluster
        AclRuleResource resource = new AclRuleClusterResource();
        ResourcePattern expectedKafkaClusterResourcePattern = new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResourcePattern(), is(expectedKafkaClusterResourcePattern));
    }

    @Test
    public void testFromCrdToKafkaResourcePatternForTransactionalIdResource()  {
        // Regular transactionalId
        AclRuleResource resource = new AclRuleTransactionalIdResourceBuilder()
                .withName("my-transactionalId")
                .build();
        ResourcePattern expectedKafkaTransactionalIdResourcePattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "my-transactionalId", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResourcePattern(), is(expectedKafkaTransactionalIdResourcePattern));
    }

    @Test
    public void testFromCrdToKafkaResourceWithoutNameThrows()  {
        // Consumer group without specified name
        AclRuleResource groupResource = new AclRuleGroupResourceBuilder()
                .withPatternType(AclResourcePatternType.LITERAL)
                .build();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> SimpleAclRuleResource.fromCrd(groupResource).toKafkaResourcePattern());
        assertThat(e.getMessage(), is("Name is required for resource type: GROUP"));

        // Topic without specified name
        AclRuleResource topicResource = new AclRuleTopicResourceBuilder()
                .withPatternType(AclResourcePatternType.LITERAL)
                .build();

        e = assertThrows(IllegalArgumentException.class, () -> SimpleAclRuleResource.fromCrd(topicResource).toKafkaResourcePattern());
        assertThat(e.getMessage(), is("Name is required for resource type: TOPIC"));
    }
}
