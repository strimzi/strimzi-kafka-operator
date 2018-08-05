/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.api.kafka.model.AclOperation;
import io.strimzi.api.kafka.model.AclResourcePatternType;
import io.strimzi.api.kafka.model.AclRuleType;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.strimzi.operator.user.model.acl.SimpleAclRuleResource;
import io.strimzi.operator.user.model.acl.SimpleAclRuleResourceType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import kafka.security.auth.Acl;
import kafka.security.auth.Allow$;
import kafka.security.auth.Group$;
import kafka.security.auth.Read$;
import kafka.security.auth.Resource;
import kafka.security.auth.SimpleAclAuthorizer;
import kafka.security.auth.Topic$;
import kafka.security.auth.Write$;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class SimpleAclOperatorTest {
    protected static Vertx vertx;

    @BeforeClass
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void after() {
        vertx.close();
    }

    @Test
    public void testGetUsersFromAcls(TestContext context)  {
        SimpleAclAuthorizer mockAuthorizer = mock(SimpleAclAuthorizer.class);
        SimpleAclOperator aclOp = new SimpleAclOperator(vertx, mockAuthorizer);

        KafkaPrincipal foo = new KafkaPrincipal("User", "CN=foo");
        Acl fooAcl = new Acl(foo, Allow$.MODULE$, "*", Read$.MODULE$);
        KafkaPrincipal bar = new KafkaPrincipal("User", "CN=bar");
        Acl barAcl = new Acl(bar, Allow$.MODULE$, "*", Read$.MODULE$);
        KafkaPrincipal baz = new KafkaPrincipal("User", "baz");
        Acl bazAcl = new Acl(baz, Allow$.MODULE$, "*", Read$.MODULE$);
        Resource res1 = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        Resource res2 = new Resource(Group$.MODULE$, "my-group", PatternType.LITERAL);
        scala.collection.immutable.Set<Acl> set1 = new scala.collection.immutable.Set.Set2<>(fooAcl, barAcl);
        scala.collection.immutable.Set<Acl> set2 = new scala.collection.immutable.Set.Set1<>(bazAcl);
        scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> map = new scala.collection.immutable.Map.Map2<>(res1, set1, res2, set2);
        when(mockAuthorizer.getAcls()).thenReturn(map);

        ArgumentCaptor<KafkaPrincipal> principalCaptor = ArgumentCaptor.forClass(KafkaPrincipal.class);
        when(mockAuthorizer.getAcls(principalCaptor.capture())).thenReturn(map);

        context.assertEquals(new HashSet(asList("foo", "bar", "baz")), aclOp.getUsersWithAcls());
    }

    @Test
    public void testInternalCreate(TestContext context)  {
        SimpleAclAuthorizer mockAuthorizer = mock(SimpleAclAuthorizer.class);
        SimpleAclOperator aclOp = new SimpleAclOperator(vertx, mockAuthorizer);

        scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> map = new scala.collection.immutable.HashMap<Resource, scala.collection.immutable.Set<Acl>>();
        ArgumentCaptor<KafkaPrincipal> principalCaptor = ArgumentCaptor.forClass(KafkaPrincipal.class);
        when(mockAuthorizer.getAcls(principalCaptor.capture())).thenReturn(map);

        ArgumentCaptor<scala.collection.immutable.Set<Acl>> aclCaptor = ArgumentCaptor.forClass(scala.collection.immutable.Set.class);
        ArgumentCaptor<Resource> resourceCaptor = ArgumentCaptor.forClass(Resource.class);
        doNothing().when(mockAuthorizer).addAcls(aclCaptor.capture(), resourceCaptor.capture());

        SimpleAclRuleResource resource = new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
        SimpleAclRule rule1 = new SimpleAclRule(AclRuleType.ALLOW, resource, "*", AclOperation.WRITE);
        SimpleAclRule rule2 = new SimpleAclRule(AclRuleType.ALLOW, resource, "*", AclOperation.READ);

        KafkaPrincipal foo = new KafkaPrincipal("User", "CN=foo");
        Acl acl1 = new Acl(foo, Allow$.MODULE$, "*", Read$.MODULE$);
        scala.collection.immutable.Set<Acl> set1 = new scala.collection.immutable.Set.Set1<>(acl1);
        Acl acl2 = new Acl(foo, Allow$.MODULE$, "*", Write$.MODULE$);
        scala.collection.immutable.Set<Acl> set2 = new scala.collection.immutable.Set.Set1<>(acl2);
        Resource res1 = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);

        Async async = context.async();
        Future<Void> fut = aclOp.reconcile("CN=foo", new HashSet(asList(rule1, rule2)));
        fut.setHandler(res -> {
            context.assertTrue(res.succeeded());

            List<scala.collection.immutable.Set<Acl>> capturedAcls = aclCaptor.getAllValues();
            List<Resource> capturedResource = resourceCaptor.getAllValues();

            context.assertEquals(2, capturedAcls.size());
            context.assertEquals(2, capturedResource.size());

            context.assertEquals(res1, capturedResource.get(0));
            context.assertEquals(res1, capturedResource.get(1));

            context.assertEquals(set1, capturedAcls.get(0));
            context.assertEquals(set2, capturedAcls.get(1));

            async.complete();
        });
    }

    @Test
    public void testInternalUpdate(TestContext context)  {
        SimpleAclAuthorizer mockAuthorizer = mock(SimpleAclAuthorizer.class);
        SimpleAclOperator aclOp = new SimpleAclOperator(vertx, mockAuthorizer);

        SimpleAclRuleResource resource = new SimpleAclRuleResource("my-topic2", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
        SimpleAclRule rule1 = new SimpleAclRule(AclRuleType.ALLOW, resource, "*", AclOperation.WRITE);

        KafkaPrincipal foo = new KafkaPrincipal("User", "CN=foo");
        Acl acl1 = new Acl(foo, Allow$.MODULE$, "*", Read$.MODULE$);
        scala.collection.immutable.Set<Acl> set1 = new scala.collection.immutable.Set.Set1<>(acl1);
        Acl acl2 = new Acl(foo, Allow$.MODULE$, "*", Write$.MODULE$);
        scala.collection.immutable.Set<Acl> set2 = new scala.collection.immutable.Set.Set1<>(acl2);
        Resource res1 = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);
        Resource res2 = new Resource(Topic$.MODULE$, "my-topic2", PatternType.LITERAL);

        scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> map = new scala.collection.immutable.Map.Map1<>(res1, set1);
        ArgumentCaptor<KafkaPrincipal> principalCaptor = ArgumentCaptor.forClass(KafkaPrincipal.class);
        when(mockAuthorizer.getAcls(principalCaptor.capture())).thenReturn(map);

        ArgumentCaptor<scala.collection.immutable.Set<Acl>> aclCaptor = ArgumentCaptor.forClass(scala.collection.immutable.Set.class);
        ArgumentCaptor<Resource> resourceCaptor = ArgumentCaptor.forClass(Resource.class);
        doNothing().when(mockAuthorizer).addAcls(aclCaptor.capture(), resourceCaptor.capture());

        ArgumentCaptor<scala.collection.immutable.Set<Acl>> deleteAclCaptor = ArgumentCaptor.forClass(scala.collection.immutable.Set.class);
        ArgumentCaptor<Resource> deleterResourceCaptor = ArgumentCaptor.forClass(Resource.class);
        when(mockAuthorizer.removeAcls(deleteAclCaptor.capture(), deleterResourceCaptor.capture())).thenReturn(true);

        Async async = context.async();
        Future<Void> fut = aclOp.reconcile("CN=foo", new HashSet(asList(rule1)));
        fut.setHandler(res -> {
            context.assertTrue(res.succeeded());

            List<scala.collection.immutable.Set<Acl>> capturedAcls = aclCaptor.getAllValues();
            List<Resource> capturedResource = resourceCaptor.getAllValues();
            List<scala.collection.immutable.Set<Acl>> deleteCapturedAcls = deleteAclCaptor.getAllValues();
            List<Resource> deleteCapturedResource = deleterResourceCaptor.getAllValues();

            context.assertEquals(1, capturedAcls.size());
            context.assertEquals(1, capturedResource.size());
            context.assertEquals(1, deleteCapturedAcls.size());
            context.assertEquals(1, deleteCapturedResource.size());

            context.assertEquals(res2, capturedResource.get(0));
            context.assertEquals(res1, deleteCapturedResource.get(0));

            context.assertEquals(set2, capturedAcls.get(0));
            context.assertEquals(set1, deleteCapturedAcls.get(0));

            async.complete();
        });
    }

    @Test
    public void testInternalDelete(TestContext context) {
        SimpleAclAuthorizer mockAuthorizer = mock(SimpleAclAuthorizer.class);
        SimpleAclOperator aclOp = new SimpleAclOperator(vertx, mockAuthorizer);

        KafkaPrincipal foo = new KafkaPrincipal("User", "CN=foo");
        Acl acl1 = new Acl(foo, Allow$.MODULE$, "*", Read$.MODULE$);
        scala.collection.immutable.Set<Acl> set1 = new scala.collection.immutable.Set.Set1<>(acl1);
        Resource res1 = new Resource(Topic$.MODULE$, "my-topic", PatternType.LITERAL);

        scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> map = new scala.collection.immutable.Map.Map1<>(res1, set1);
        ArgumentCaptor<KafkaPrincipal> principalCaptor = ArgumentCaptor.forClass(KafkaPrincipal.class);
        when(mockAuthorizer.getAcls(principalCaptor.capture())).thenReturn(map);

        ArgumentCaptor<scala.collection.immutable.Set<Acl>> deleteAclCaptor = ArgumentCaptor.forClass(scala.collection.immutable.Set.class);
        ArgumentCaptor<Resource> deleterResourceCaptor = ArgumentCaptor.forClass(Resource.class);
        when(mockAuthorizer.removeAcls(deleteAclCaptor.capture(), deleterResourceCaptor.capture())).thenReturn(true);

        Async async = context.async();
        Future<ReconcileResult<Set<SimpleAclRule>>> fut = aclOp.reconcile("CN=foo", null);
        fut.setHandler(res -> {
            context.assertTrue(res.succeeded());

            List<scala.collection.immutable.Set<Acl>> deleteCapturedAcls = deleteAclCaptor.getAllValues();
            List<Resource> deleteCapturedResource = deleterResourceCaptor.getAllValues();

            context.assertEquals(1, deleteCapturedAcls.size());
            context.assertEquals(1, deleteCapturedResource.size());

            context.assertEquals(res1, deleteCapturedResource.get(0));

            context.assertEquals(set1, deleteCapturedAcls.get(0));

            async.complete();
        });
    }
}
