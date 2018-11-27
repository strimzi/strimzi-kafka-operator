/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model.acl;

import io.strimzi.api.kafka.model.AclResourcePatternType;
import io.strimzi.api.kafka.model.AclRuleClusterResource;
import io.strimzi.api.kafka.model.AclRuleGroupResource;
import io.strimzi.api.kafka.model.AclRuleResource;
import io.strimzi.api.kafka.model.AclRuleTopicResource;

import io.strimzi.api.kafka.model.AclRuleTransactionalIdResource;
import kafka.security.auth.Cluster$;
import kafka.security.auth.Group$;
import kafka.security.auth.Resource;
import kafka.security.auth.Topic$;
import kafka.security.auth.TransactionalId$;
import org.apache.kafka.common.resource.PatternType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SimpleAclRuleResourceTest {
    @Test
    public void testTopicFromCrd()   {
        // Regular topic
        AclRuleResource resource = new AclRuleTopicResource();
        ((AclRuleTopicResource) resource).setName("my-topic");
        ((AclRuleTopicResource) resource).setPatternType(AclResourcePatternType.LITERAL);

        SimpleAclRuleResource simple = SimpleAclRuleResource.fromCrd(resource);
        assertEquals("my-topic", simple.getName());
        assertEquals(SimpleAclRuleResourceType.TOPIC, simple.getType());
        assertEquals(AclResourcePatternType.LITERAL, simple.getPattern());

        // Prefixed topic
        resource = new AclRuleTopicResource();
        ((AclRuleTopicResource) resource).setName("my-");
        ((AclRuleTopicResource) resource).setPatternType(AclResourcePatternType.PREFIX);

        simple = SimpleAclRuleResource.fromCrd(resource);
        assertEquals("my-", simple.getName());
        assertEquals(SimpleAclRuleResourceType.TOPIC, simple.getType());
        assertEquals(AclResourcePatternType.PREFIX, simple.getPattern());
    }

    @Test
    public void testGroupFromCrd()   {
        // Regular group
        AclRuleResource resource = new AclRuleGroupResource();
        ((AclRuleGroupResource) resource).setName("my-group");
        ((AclRuleGroupResource) resource).setPatternType(AclResourcePatternType.LITERAL);

        SimpleAclRuleResource simple = SimpleAclRuleResource.fromCrd(resource);
        assertEquals("my-group", simple.getName());
        assertEquals(SimpleAclRuleResourceType.GROUP, simple.getType());
        assertEquals(AclResourcePatternType.LITERAL, simple.getPattern());

        // Prefixed group
        resource = new AclRuleGroupResource();
        ((AclRuleGroupResource) resource).setName("my-");
        ((AclRuleGroupResource) resource).setPatternType(AclResourcePatternType.PREFIX);

        simple = SimpleAclRuleResource.fromCrd(resource);
        assertEquals("my-", simple.getName());
        assertEquals(SimpleAclRuleResourceType.GROUP, simple.getType());
        assertEquals(AclResourcePatternType.PREFIX, simple.getPattern());
    }

    @Test
    public void testTransactionalIdFromCrd()   {
        // Regular transactionalId
        AclRuleResource resource = new AclRuleTransactionalIdResource();

        ((AclRuleTransactionalIdResource) resource).setName("my-transactionalId");
        SimpleAclRuleResource simple = SimpleAclRuleResource.fromCrd(resource);
        assertEquals("my-transactionalId", simple.getName());
        assertEquals(SimpleAclRuleResourceType.TRANSACTIONAL_ID, simple.getType());
        assertEquals(AclResourcePatternType.LITERAL, simple.getPattern());
    }

    @Test
    public void testClusterFromCrd()   {
        // Regular cluster
        AclRuleResource resource = new AclRuleClusterResource();

        SimpleAclRuleResource simple = SimpleAclRuleResource.fromCrd(resource);
        assertEquals("kafka-cluster", simple.getName());
        assertEquals(SimpleAclRuleResourceType.CLUSTER, simple.getType());
        assertEquals(AclResourcePatternType.LITERAL, simple.getPattern());
    }

    @Test
    public void testTopicToKafka()  {
        // Regular topic
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        assertEquals(kafka, strimzi.toKafkaResource());

        // Prefixed topic
        strimzi = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.PREFIX);
        kafka = new Resource(Topic$.MODULE$, "my-", PatternType.PREFIXED);
        assertEquals(kafka, strimzi.toKafkaResource());
    }

    @Test
    public void testGroupToKafka()  {
        // Regular group
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource("my-group", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(Group$.MODULE$, "my-group", PatternType.LITERAL);
        assertEquals(kafka, strimzi.toKafkaResource());

        // Prefixed group
        strimzi = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.PREFIX);
        kafka = new Resource(Group$.MODULE$, "my-", PatternType.PREFIXED);
        assertEquals(kafka, strimzi.toKafkaResource());
    }

    @Test
    public void testClusterToKafka()  {
        // Regular cluster
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource(null, SimpleAclRuleResourceType.CLUSTER, null);
        Resource kafka = new Resource(Cluster$.MODULE$, "kafka-cluster", PatternType.LITERAL);
        assertEquals(kafka, strimzi.toKafkaResource());
    }

    @Test
    public void testTransactionalIdToKafka()  {
        // Regular transactionalId
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource("my-transactionalId", SimpleAclRuleResourceType.TRANSACTIONAL_ID, null);
        Resource kafka = new Resource(TransactionalId$.MODULE$, "my-transactionalId", PatternType.LITERAL);
        assertEquals(kafka, strimzi.toKafkaResource());

        // Prefixed transactionalId
        strimzi = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TRANSACTIONAL_ID, AclResourcePatternType.PREFIX);
        kafka = new Resource(TransactionalId$.MODULE$, "my-", PatternType.PREFIXED);
        assertEquals(kafka, strimzi.toKafkaResource());
    }

    @Test
    public void testTopicFromKafka()  {
        // Regular topic
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        assertEquals(strimzi, SimpleAclRuleResource.fromKafkaResource(kafka));

        // Prefixed topic
        strimzi = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.PREFIX);
        kafka = new Resource(Topic$.MODULE$, "my-", PatternType.PREFIXED);
        assertEquals(strimzi, SimpleAclRuleResource.fromKafkaResource(kafka));
    }

    @Test
    public void testGroupFromKafka()  {
        // Regular group
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource("my-group", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(Group$.MODULE$, "my-group", PatternType.LITERAL);
        assertEquals(strimzi, SimpleAclRuleResource.fromKafkaResource(kafka));

        // Prefixed group
        strimzi = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.PREFIX);
        kafka = new Resource(Group$.MODULE$, "my-", PatternType.PREFIXED);
        assertEquals(strimzi, SimpleAclRuleResource.fromKafkaResource(kafka));
    }

    @Test
    public void testClusterFromKafka()  {
        // Regular topic
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource("kafka-cluster", SimpleAclRuleResourceType.CLUSTER, AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(Cluster$.MODULE$, "kafka-cluster", PatternType.LITERAL);
        assertEquals(strimzi, SimpleAclRuleResource.fromKafkaResource(kafka));
    }

    @Test
    public void testTransactionalIdFromKafka()  {
        // Regular transactionalId
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource("my-transactionalId", SimpleAclRuleResourceType.TRANSACTIONAL_ID, AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(TransactionalId$.MODULE$, "my-transactionalId", PatternType.LITERAL);
        assertEquals(strimzi, SimpleAclRuleResource.fromKafkaResource(kafka));

        // Prefixed transactionalId
        strimzi = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TRANSACTIONAL_ID, AclResourcePatternType.PREFIX);
        kafka = new Resource(TransactionalId$.MODULE$, "my-", PatternType.PREFIXED);
        assertEquals(strimzi, SimpleAclRuleResource.fromKafkaResource(kafka));
    }

    @Test
    public void testTopicRoundTrip()    {
        // Regular group
        Resource kafka = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        assertEquals(kafka, SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource());

        // Prefixed topic
        kafka = new Resource(Topic$.MODULE$, "my-", PatternType.PREFIXED);
        assertEquals(kafka, SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource());
    }

    @Test
    public void testGroupRoundTrip()  {
        // Regular group
        Resource kafka = new Resource(Group$.MODULE$, "my-group", PatternType.LITERAL);
        assertEquals(kafka, SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource());

        // Prefixed group
        kafka = new Resource(Group$.MODULE$, "my-", PatternType.PREFIXED);
        assertEquals(kafka, SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource());
    }

    @Test
    public void testClusterRoundTrip()  {
        // Regular cluster
        Resource kafka = new Resource(Cluster$.MODULE$, "kafka-cluster", PatternType.LITERAL);
        assertEquals(kafka, SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource());
    }

    @Test
    public void testTransactionIDRoundTrip()  {
        // Regular transactionID
        Resource kafka = new Resource(TransactionalId$.MODULE$, "my-transactionID", PatternType.LITERAL);
        assertEquals(kafka, SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource());

        // Prefixed transactionID
        kafka = new Resource(TransactionalId$.MODULE$, "my-", PatternType.PREFIXED);
        assertEquals(kafka, SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource());
    }

    @Test
    public void testTopicPassthrough()    {
        // Regular group
        AclRuleResource resource = new AclRuleTopicResource();
        ((AclRuleTopicResource) resource).setName("my-topic");
        ((AclRuleTopicResource) resource).setPatternType(AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        assertEquals(kafka, SimpleAclRuleResource.fromCrd(resource).toKafkaResource());

        // Prefixed topic
        resource = new AclRuleTopicResource();
        ((AclRuleTopicResource) resource).setName("my-");
        ((AclRuleTopicResource) resource).setPatternType(AclResourcePatternType.PREFIX);
        kafka = new Resource(Topic$.MODULE$, "my-", PatternType.PREFIXED);
        assertEquals(kafka, SimpleAclRuleResource.fromCrd(resource).toKafkaResource());
    }

    @Test
    public void testGroupPassthrough()  {
        // Regular group
        AclRuleResource resource = new AclRuleGroupResource();
        ((AclRuleGroupResource) resource).setName("my-group");
        ((AclRuleGroupResource) resource).setPatternType(AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(Group$.MODULE$, "my-group", PatternType.LITERAL);
        assertEquals(kafka, SimpleAclRuleResource.fromCrd(resource).toKafkaResource());

        // Prefixed group
        resource = new AclRuleGroupResource();
        ((AclRuleGroupResource) resource).setName("my-");
        ((AclRuleGroupResource) resource).setPatternType(AclResourcePatternType.PREFIX);
        kafka = new Resource(Group$.MODULE$, "my-", PatternType.PREFIXED);
        assertEquals(kafka, SimpleAclRuleResource.fromCrd(resource).toKafkaResource());
    }

    @Test
    public void testClusterPassthrough()  {
        // Regular cluster
        AclRuleResource resource = new AclRuleClusterResource();
        Resource kafka = new Resource(Cluster$.MODULE$, "kafka-cluster", PatternType.LITERAL);
        assertEquals(kafka, SimpleAclRuleResource.fromCrd(resource).toKafkaResource());
    }

    @Test
    public void testTransactionalIdPassthrough()  {
        // Regular transactionalId
        AclRuleResource resource = new AclRuleTransactionalIdResource();
        ((AclRuleTransactionalIdResource) resource).setName("my-transactionalId");
        Resource kafka = new Resource(TransactionalId$.MODULE$, "my-transactionalId", PatternType.LITERAL);
        assertEquals(kafka, SimpleAclRuleResource.fromCrd(resource).toKafkaResource());
    }
}
