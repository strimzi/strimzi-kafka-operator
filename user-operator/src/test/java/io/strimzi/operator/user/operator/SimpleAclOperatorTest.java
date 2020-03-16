/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.api.kafka.model.AclOperation;
import io.strimzi.api.kafka.model.AclResourcePatternType;
import io.strimzi.api.kafka.model.AclRuleType;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.strimzi.operator.user.model.acl.SimpleAclRuleResource;
import io.strimzi.operator.user.model.acl.SimpleAclRuleResourceType;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import kafka.security.auth.Acl;
import kafka.security.auth.Allow$;
import kafka.security.auth.Cluster$;
import kafka.security.auth.Describe$;
import kafka.security.auth.Group$;
import kafka.security.auth.Read$;
import kafka.security.auth.Resource;
import kafka.security.auth.SimpleAclAuthorizer;
import kafka.security.auth.Topic$;
import kafka.security.auth.Write$;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class SimpleAclOperatorTest {
    protected static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @Test
    public void testGetUsersFromAcls(VertxTestContext context)  {
        SimpleAclAuthorizer mockAuthorizer = mock(SimpleAclAuthorizer.class);
        SimpleAclOperator aclOp = new SimpleAclOperator(vertx, mockAuthorizer);

        KafkaPrincipal foo = new KafkaPrincipal("User", "CN=foo");
        Acl fooAcl = new Acl(foo, Allow$.MODULE$, "*", Read$.MODULE$);
        KafkaPrincipal bar = new KafkaPrincipal("User", "CN=bar");
        Acl barAcl = new Acl(bar, Allow$.MODULE$, "*", Read$.MODULE$);
        KafkaPrincipal baz = new KafkaPrincipal("User", "baz");
        Acl bazAcl = new Acl(baz, Allow$.MODULE$, "*", Read$.MODULE$);
        KafkaPrincipal all = new KafkaPrincipal("User", "*");
        Acl allAcl = new Acl(all, Allow$.MODULE$, "*", Read$.MODULE$);
        KafkaPrincipal anonymous = new KafkaPrincipal("User", "ANONYMOUS");
        Acl anonymousAcl = new Acl(anonymous, Allow$.MODULE$, "*", Read$.MODULE$);
        Resource res1 = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        Resource res2 = new Resource(Group$.MODULE$, "my-group", PatternType.LITERAL);
        scala.collection.immutable.Set<Acl> set1 = new scala.collection.immutable.Set.Set3<>(fooAcl, barAcl, allAcl);
        scala.collection.immutable.Set<Acl> set2 = new scala.collection.immutable.Set.Set2<>(bazAcl, anonymousAcl);
        scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> map = new scala.collection.immutable.Map.Map2<>(res1, set1, res2, set2);
        when(mockAuthorizer.getAcls()).thenReturn(map);

        ArgumentCaptor<KafkaPrincipal> principalCaptor = ArgumentCaptor.forClass(KafkaPrincipal.class);
        when(mockAuthorizer.getAcls(principalCaptor.capture())).thenReturn(map);

        assertThat(aclOp.getUsersWithAcls(), is(new HashSet(asList("foo", "bar", "baz"))));
        context.completeNow();
    }

    @Test
    public void testReconcileInternalCreateAddsAclsToAuthorizer(VertxTestContext context) {
        SimpleAclAuthorizer mockAuthorizer = mock(SimpleAclAuthorizer.class);
        SimpleAclOperator aclOp = new SimpleAclOperator(vertx, mockAuthorizer);

        scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> map = new scala.collection.immutable.HashMap<Resource, scala.collection.immutable.Set<Acl>>();
        ArgumentCaptor<KafkaPrincipal> principalCaptor = ArgumentCaptor.forClass(KafkaPrincipal.class);
        when(mockAuthorizer.getAcls(principalCaptor.capture())).thenReturn(map);

        ArgumentCaptor<scala.collection.immutable.Set<Acl>> aclCaptor = ArgumentCaptor.forClass(scala.collection.immutable.Set.class);
        ArgumentCaptor<Resource> resourceCaptor = ArgumentCaptor.forClass(Resource.class);
        doNothing().when(mockAuthorizer).addAcls(aclCaptor.capture(), resourceCaptor.capture());

        SimpleAclRuleResource ruleResource1 = new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.CLUSTER, AclResourcePatternType.LITERAL);
        SimpleAclRuleResource ruleResource2 = new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
        SimpleAclRule resource1DescribeRule = new SimpleAclRule(AclRuleType.ALLOW, ruleResource1, "*", AclOperation.DESCRIBE);
        SimpleAclRule resource2ReadRule = new SimpleAclRule(AclRuleType.ALLOW, ruleResource2, "*", AclOperation.READ);
        SimpleAclRule resource2WriteRule = new SimpleAclRule(AclRuleType.ALLOW, ruleResource2, "*", AclOperation.WRITE);

        KafkaPrincipal foo = new KafkaPrincipal("User", "CN=foo");
        Acl readAcl = new Acl(foo, Allow$.MODULE$, "*", Read$.MODULE$);
        Acl writeAcl = new Acl(foo, Allow$.MODULE$, "*", Write$.MODULE$);
        Acl describeAcl = new Acl(foo, Allow$.MODULE$, "*", Describe$.MODULE$);
        scala.collection.immutable.Set<Acl> expectedResource1RuleSet = new scala.collection.immutable.Set.Set1<>(describeAcl);
        scala.collection.immutable.Set<Acl> expectedResource2RuleSet = new scala.collection.immutable.Set.Set2<>(readAcl, writeAcl);

        Resource resource1 = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        Resource resource2 = new Resource(Cluster$.MODULE$, "kafka-cluster", PatternType.LITERAL);

        Checkpoint async = context.checkpoint();
        aclOp.reconcile("CN=foo", new LinkedHashSet<>(asList(resource2ReadRule, resource2WriteRule, resource1DescribeRule)))
            .setHandler(context.succeeding(rr -> context.verify(() -> {
                List<scala.collection.immutable.Set<Acl>> capturedAcls = aclCaptor.getAllValues();
                List<Resource> capturedResource = resourceCaptor.getAllValues();

                assertThat(capturedAcls, hasSize(2));
                assertThat(capturedResource, hasSize(2));

                assertThat(capturedResource, hasItems(resource1, resource2));
                assertThat(capturedAcls, hasItems(expectedResource1RuleSet, expectedResource2RuleSet));

                async.flag();
            })));
    }

    @Test
    public void testReconcileInternalUpdateCreatesNewAclsAndDeletesOldAcls(VertxTestContext context) {
        SimpleAclAuthorizer mockAuthorizer = mock(SimpleAclAuthorizer.class);
        SimpleAclOperator aclOp = new SimpleAclOperator(vertx, mockAuthorizer);

        SimpleAclRuleResource resource = new SimpleAclRuleResource("my-topic2", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
        SimpleAclRule rule1 = new SimpleAclRule(AclRuleType.ALLOW, resource, "*", AclOperation.WRITE);

        KafkaPrincipal foo = new KafkaPrincipal("User", "CN=foo");
        Acl readAcl = new Acl(foo, Allow$.MODULE$, "*", Read$.MODULE$);
        scala.collection.immutable.Set<Acl> readAclSet = new scala.collection.immutable.Set.Set1<>(readAcl);
        Acl writeAcl = new Acl(foo, Allow$.MODULE$, "*", Write$.MODULE$);
        scala.collection.immutable.Set<Acl> writeAclSet = new scala.collection.immutable.Set.Set1<>(writeAcl);

        Resource resource1 = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        Resource resource2 = new Resource(Topic$.MODULE$, "my-topic2", PatternType.LITERAL);

        scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> map = new scala.collection.immutable.Map.Map1<>(resource1, readAclSet);
        ArgumentCaptor<KafkaPrincipal> principalCaptor = ArgumentCaptor.forClass(KafkaPrincipal.class);
        when(mockAuthorizer.getAcls(principalCaptor.capture())).thenReturn(map);

        ArgumentCaptor<scala.collection.immutable.Set<Acl>> aclCaptor = ArgumentCaptor.forClass(scala.collection.immutable.Set.class);
        ArgumentCaptor<Resource> resourceCaptor = ArgumentCaptor.forClass(Resource.class);
        doNothing().when(mockAuthorizer).addAcls(aclCaptor.capture(), resourceCaptor.capture());

        ArgumentCaptor<scala.collection.immutable.Set<Acl>> deleteAclCaptor = ArgumentCaptor.forClass(scala.collection.immutable.Set.class);
        ArgumentCaptor<Resource> deleterResourceCaptor = ArgumentCaptor.forClass(Resource.class);
        when(mockAuthorizer.removeAcls(deleteAclCaptor.capture(), deleterResourceCaptor.capture())).thenReturn(true);

        Checkpoint async = context.checkpoint();
        aclOp.reconcile("CN=foo", new LinkedHashSet(asList(rule1)))
            .setHandler(context.succeeding(rr -> context.verify(() -> {
                List<scala.collection.immutable.Set<Acl>> capturedAcls = aclCaptor.getAllValues();
                List<Resource> capturedResource = resourceCaptor.getAllValues();
                List<scala.collection.immutable.Set<Acl>> deleteCapturedAcls = deleteAclCaptor.getAllValues();
                List<Resource> deleteCapturedResource = deleterResourceCaptor.getAllValues();

                // Create Write rule for resource 2
                assertThat(capturedAcls, hasSize(1));
                assertThat(capturedAcls, hasItem(writeAclSet));
                assertThat(capturedResource, hasSize(1));
                assertThat(capturedResource, hasItem(resource2));

                // Delete read rule for resource 1
                assertThat(deleteCapturedAcls, hasSize(1));
                assertThat(deleteCapturedAcls, hasItem(readAclSet));
                assertThat(deleteCapturedResource, hasSize(1));
                assertThat(deleteCapturedResource, hasItem(resource1));

                async.flag();
            })));
    }

    @Test
    public void testReconcileInternalDelete(VertxTestContext context) {
        SimpleAclAuthorizer mockAuthorizer = mock(SimpleAclAuthorizer.class);
        SimpleAclOperator aclOp = new SimpleAclOperator(vertx, mockAuthorizer);

        KafkaPrincipal foo = new KafkaPrincipal("User", "CN=foo");
        Acl readAcl = new Acl(foo, Allow$.MODULE$, "*", Read$.MODULE$);
        scala.collection.immutable.Set<Acl> readAclSet = new scala.collection.immutable.Set.Set1<>(readAcl);
        Resource resource1 = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);

        scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> map = new scala.collection.immutable.Map.Map1<>(resource1, readAclSet);
        ArgumentCaptor<KafkaPrincipal> principalCaptor = ArgumentCaptor.forClass(KafkaPrincipal.class);
        when(mockAuthorizer.getAcls(principalCaptor.capture())).thenReturn(map);

        ArgumentCaptor<scala.collection.immutable.Set<Acl>> deleteAclCaptor = ArgumentCaptor.forClass(scala.collection.immutable.Set.class);
        ArgumentCaptor<Resource> deleterResourceCaptor = ArgumentCaptor.forClass(Resource.class);
        when(mockAuthorizer.removeAcls(deleteAclCaptor.capture(), deleterResourceCaptor.capture())).thenReturn(true);

        Checkpoint async = context.checkpoint();
        aclOp.reconcile("CN=foo", null)
            .setHandler(context.succeeding(rr -> context.verify(() -> {
                List<scala.collection.immutable.Set<Acl>> deleteCapturedAcls = deleteAclCaptor.getAllValues();
                List<Resource> deleteCapturedResource = deleterResourceCaptor.getAllValues();

                // Delete correct read rule for resource 1
                assertThat(deleteCapturedAcls, hasSize(1));
                assertThat(deleteCapturedAcls, hasItem(readAclSet));
                assertThat(deleteCapturedResource, hasSize(1));
                assertThat(deleteCapturedResource, hasItem(resource1));

                async.flag();
            })));
    }
}
