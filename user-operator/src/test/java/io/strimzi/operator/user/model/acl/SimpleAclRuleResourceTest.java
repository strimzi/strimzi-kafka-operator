/*
 * Copyright Strimzi authors.
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
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class SimpleAclRuleResourceTest {
    @Test
    public void testTopicFromCrd()   {
        // Regular topic
        AclRuleResource resource = new AclRuleTopicResource();
        ((AclRuleTopicResource) resource).setName("my-topic");
        ((AclRuleTopicResource) resource).setPatternType(AclResourcePatternType.LITERAL);

        SimpleAclRuleResource simple = SimpleAclRuleResource.fromCrd(resource);
        assertThat(simple.getName(), is("my-topic"));
        assertThat(simple.getType(), is(SimpleAclRuleResourceType.TOPIC));
        assertThat(simple.getPattern(), is(AclResourcePatternType.LITERAL));

        // Prefixed topic
        resource = new AclRuleTopicResource();
        ((AclRuleTopicResource) resource).setName("my-");
        ((AclRuleTopicResource) resource).setPatternType(AclResourcePatternType.PREFIX);

        simple = SimpleAclRuleResource.fromCrd(resource);
        assertThat(simple.getName(), is("my-"));
        assertThat(simple.getType(), is(SimpleAclRuleResourceType.TOPIC));
        assertThat(simple.getPattern(), is(AclResourcePatternType.PREFIX));
    }

    @Test
    public void testGroupFromCrd()   {
        // Regular group
        AclRuleResource resource = new AclRuleGroupResource();
        ((AclRuleGroupResource) resource).setName("my-group");
        ((AclRuleGroupResource) resource).setPatternType(AclResourcePatternType.LITERAL);

        SimpleAclRuleResource simple = SimpleAclRuleResource.fromCrd(resource);
        assertThat(simple.getName(), is("my-group"));
        assertThat(simple.getType(), is(SimpleAclRuleResourceType.GROUP));
        assertThat(simple.getPattern(), is(AclResourcePatternType.LITERAL));

        // Prefixed group
        resource = new AclRuleGroupResource();
        ((AclRuleGroupResource) resource).setName("my-");
        ((AclRuleGroupResource) resource).setPatternType(AclResourcePatternType.PREFIX);

        simple = SimpleAclRuleResource.fromCrd(resource);
        assertThat(simple.getName(), is("my-"));
        assertThat(simple.getType(), is(SimpleAclRuleResourceType.GROUP));
        assertThat(simple.getPattern(), is(AclResourcePatternType.PREFIX));
    }

    @Test
    public void testTransactionalIdFromCrd()   {
        // Regular transactionalId
        AclRuleResource resource = new AclRuleTransactionalIdResource();

        ((AclRuleTransactionalIdResource) resource).setName("my-transactionalId");
        SimpleAclRuleResource simple = SimpleAclRuleResource.fromCrd(resource);
        assertThat(simple.getName(), is("my-transactionalId"));
        assertThat(simple.getType(), is(SimpleAclRuleResourceType.TRANSACTIONAL_ID));
        assertThat(simple.getPattern(), is(AclResourcePatternType.LITERAL));
    }

    @Test
    public void testClusterFromCrd()   {
        // Regular cluster
        AclRuleResource resource = new AclRuleClusterResource();

        SimpleAclRuleResource simple = SimpleAclRuleResource.fromCrd(resource);
        assertThat(simple.getName(), is("kafka-cluster"));
        assertThat(simple.getType(), is(SimpleAclRuleResourceType.CLUSTER));
        assertThat(simple.getPattern(), is(AclResourcePatternType.LITERAL));
    }

    @Test
    public void testTopicToKafka()  {
        // Regular topic
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        assertThat(strimzi.toKafkaResource(), is(kafka));

        // Prefixed topic
        strimzi = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.PREFIX);
        kafka = new Resource(Topic$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(strimzi.toKafkaResource(), is(kafka));
    }

    @Test
    public void testGroupToKafka()  {
        // Regular group
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource("my-group", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(Group$.MODULE$, "my-group", PatternType.LITERAL);
        assertThat(strimzi.toKafkaResource(), is(kafka));

        // Prefixed group
        strimzi = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.PREFIX);
        kafka = new Resource(Group$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(strimzi.toKafkaResource(), is(kafka));
    }

    @Test
    public void testClusterToKafka()  {
        // Regular cluster
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource(null, SimpleAclRuleResourceType.CLUSTER, null);
        Resource kafka = new Resource(Cluster$.MODULE$, "kafka-cluster", PatternType.LITERAL);
        assertThat(strimzi.toKafkaResource(), is(kafka));
    }

    @Test
    public void testTransactionalIdToKafka()  {
        // Regular transactionalId
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource("my-transactionalId", SimpleAclRuleResourceType.TRANSACTIONAL_ID, null);
        Resource kafka = new Resource(TransactionalId$.MODULE$, "my-transactionalId", PatternType.LITERAL);
        assertThat(strimzi.toKafkaResource(), is(kafka));

        // Prefixed transactionalId
        strimzi = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TRANSACTIONAL_ID, AclResourcePatternType.PREFIX);
        kafka = new Resource(TransactionalId$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(strimzi.toKafkaResource(), is(kafka));
    }

    @Test
    public void testTopicFromKafka()  {
        // Regular topic
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka), is(strimzi));

        // Prefixed topic
        strimzi = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.PREFIX);
        kafka = new Resource(Topic$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka), is(strimzi));
    }

    @Test
    public void testGroupFromKafka()  {
        // Regular group
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource("my-group", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(Group$.MODULE$, "my-group", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka), is(strimzi));

        // Prefixed group
        strimzi = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.GROUP, AclResourcePatternType.PREFIX);
        kafka = new Resource(Group$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka), is(strimzi));
    }

    @Test
    public void testClusterFromKafka()  {
        // Regular topic
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource("kafka-cluster", SimpleAclRuleResourceType.CLUSTER, AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(Cluster$.MODULE$, "kafka-cluster", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka), is(strimzi));
    }

    @Test
    public void testTransactionalIdFromKafka()  {
        // Regular transactionalId
        SimpleAclRuleResource strimzi = new SimpleAclRuleResource("my-transactionalId", SimpleAclRuleResourceType.TRANSACTIONAL_ID, AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(TransactionalId$.MODULE$, "my-transactionalId", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka), is(strimzi));

        // Prefixed transactionalId
        strimzi = new SimpleAclRuleResource("my-", SimpleAclRuleResourceType.TRANSACTIONAL_ID, AclResourcePatternType.PREFIX);
        kafka = new Resource(TransactionalId$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka), is(strimzi));
    }

    @Test
    public void testTopicRoundTrip()    {
        // Regular group
        Resource kafka = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource(), is(kafka));

        // Prefixed topic
        kafka = new Resource(Topic$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource(), is(kafka));
    }

    @Test
    public void testGroupRoundTrip()  {
        // Regular group
        Resource kafka = new Resource(Group$.MODULE$, "my-group", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource(), is(kafka));

        // Prefixed group
        kafka = new Resource(Group$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource(), is(kafka));
    }

    @Test
    public void testClusterRoundTrip()  {
        // Regular cluster
        Resource kafka = new Resource(Cluster$.MODULE$, "kafka-cluster", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource(), is(kafka));
    }

    @Test
    public void testTransactionIDRoundTrip()  {
        // Regular transactionID
        Resource kafka = new Resource(TransactionalId$.MODULE$, "my-transactionID", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource(), is(kafka));

        // Prefixed transactionID
        kafka = new Resource(TransactionalId$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromKafkaResource(kafka).toKafkaResource(), is(kafka));
    }

    @Test
    public void testTopicPassthrough()    {
        // Regular group
        AclRuleResource resource = new AclRuleTopicResource();
        ((AclRuleTopicResource) resource).setName("my-topic");
        ((AclRuleTopicResource) resource).setPatternType(AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResource(), is(kafka));

        // Prefixed topic
        resource = new AclRuleTopicResource();
        ((AclRuleTopicResource) resource).setName("my-");
        ((AclRuleTopicResource) resource).setPatternType(AclResourcePatternType.PREFIX);
        kafka = new Resource(Topic$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResource(), is(kafka));
    }

    @Test
    public void testGroupPassthrough()  {
        // Regular group
        AclRuleResource resource = new AclRuleGroupResource();
        ((AclRuleGroupResource) resource).setName("my-group");
        ((AclRuleGroupResource) resource).setPatternType(AclResourcePatternType.LITERAL);
        Resource kafka = new Resource(Group$.MODULE$, "my-group", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResource(), is(kafka));

        // Prefixed group
        resource = new AclRuleGroupResource();
        ((AclRuleGroupResource) resource).setName("my-");
        ((AclRuleGroupResource) resource).setPatternType(AclResourcePatternType.PREFIX);
        kafka = new Resource(Group$.MODULE$, "my-", PatternType.PREFIXED);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResource(), is(kafka));
    }

    @Test
    public void testClusterPassthrough()  {
        // Regular cluster
        AclRuleResource resource = new AclRuleClusterResource();
        Resource kafka = new Resource(Cluster$.MODULE$, "kafka-cluster", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResource(), is(kafka));
    }

    @Test
    public void testTransactionalIdPassthrough()  {
        // Regular transactionalId
        AclRuleResource resource = new AclRuleTransactionalIdResource();
        ((AclRuleTransactionalIdResource) resource).setName("my-transactionalId");
        Resource kafka = new Resource(TransactionalId$.MODULE$, "my-transactionalId", PatternType.LITERAL);
        assertThat(SimpleAclRuleResource.fromCrd(resource).toKafkaResource(), is(kafka));
    }
}
