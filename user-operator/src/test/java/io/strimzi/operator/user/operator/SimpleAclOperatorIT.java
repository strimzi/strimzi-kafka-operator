/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.strimzi.api.kafka.model.AclOperation;
import io.strimzi.api.kafka.model.AclResourcePatternType;
import io.strimzi.api.kafka.model.AclRuleType;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.strimzi.operator.user.model.acl.SimpleAclRuleResource;
import io.strimzi.operator.user.model.acl.SimpleAclRuleResourceType;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.collection.IsEmptyCollection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

@ExtendWith(VertxExtension.class)
public class SimpleAclOperatorIT {

    private static final Logger log = LogManager.getLogger(SimpleAclOperatorIT.class);
    private static final int TEST_TIMEOUT = 60;

    private static Vertx vertx;

    private static KafkaCluster kafkaCluster;

    private static SimpleAclOperator simpleAclOperator;

    private static Properties kafkaClusterConfig() {
        Properties config = new Properties();
        config.setProperty("authorizer.class.name", "kafka.security.auth.SimpleAclAuthorizer");
        config.setProperty("super.users", "User:ANONYMOUS");
        return config;
    }

    @BeforeAll
    public static void beforeAll() {
        vertx = Vertx.vertx();

        try {
            kafkaCluster =
                    new KafkaCluster()
                            .usingDirectory(Testing.Files.createTestingDirectory("simple-acl-operator-integration-test"))
                            .deleteDataPriorToStartup(true)
                            .deleteDataUponShutdown(true)
                            .addBrokers(1)
                            .withKafkaConfiguration(kafkaClusterConfig())
                            .startup();
        } catch (IOException e) {
            assertThat(false, is(true));
        }

        simpleAclOperator = new SimpleAclOperator(vertx,
                new DefaultAdminClientProvider().createAdminClient(kafkaCluster.brokerList(), null, null, null));
    }

    @Test
    public void testNoAclRules(VertxTestContext context) {
        Set<SimpleAclRule> acls = simpleAclOperator.getAcls("no-acls-user");
        context.verify(() -> {
            assertThat(acls, IsEmptyCollection.empty());
        });
        context.completeNow();
    }

    @Test
    public void testCreateAclRule(VertxTestContext context) throws InterruptedException {
        SimpleAclRule rule = new SimpleAclRule(
                AclRuleType.ALLOW,
                new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL),
                "*",
                AclOperation.READ);

        CountDownLatch async = new CountDownLatch(1);
        simpleAclOperator.reconcile("my-user", Collections.singleton(rule))
                .onComplete(ignore -> context.verify(() -> {
                    Set<SimpleAclRule> acls = simpleAclOperator.getAcls("my-user");
                    assertThat(acls, hasSize(1));
                    assertThat(acls, hasItem(rule));
                    async.countDown();
                }));

        async.await(TEST_TIMEOUT, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    public void testCreateAndUpdateAclRule(VertxTestContext context) throws InterruptedException {
        SimpleAclRule rule1 = new SimpleAclRule(
                AclRuleType.ALLOW,
                new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL),
                "*",
                AclOperation.READ);

        CountDownLatch async1 = new CountDownLatch(1);
        simpleAclOperator.reconcile("my-user", Collections.singleton(rule1))
                .onComplete(ignore -> context.verify(() -> {
                    Set<SimpleAclRule> acls = simpleAclOperator.getAcls("my-user");
                    assertThat(acls, hasSize(1));
                    assertThat(acls, hasItem(rule1));
                    async1.countDown();
                }));

        async1.await(TEST_TIMEOUT, TimeUnit.SECONDS);

        SimpleAclRule rule2 = new SimpleAclRule(
                AclRuleType.ALLOW,
                new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL),
                "*",
                AclOperation.WRITE);

        CountDownLatch async2 = new CountDownLatch(1);
        simpleAclOperator.reconcile("my-user", new HashSet<>(asList(rule1, rule2)))
                .onComplete(ignore -> context.verify(() -> {
                    Set<SimpleAclRule> acls = simpleAclOperator.getAcls("my-user");
                    assertThat(acls, hasSize(2));
                    assertThat(acls, hasItems(rule1, rule2));
                    async2.countDown();
                }));

        async2.await(TEST_TIMEOUT, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    public void testCreateAndDeleteAclRule(VertxTestContext context) throws InterruptedException {
        SimpleAclRule rule1 = new SimpleAclRule(
                AclRuleType.ALLOW,
                new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL),
                "*",
                AclOperation.READ);

        CountDownLatch async1 = new CountDownLatch(1);
        simpleAclOperator.reconcile("my-user", Collections.singleton(rule1))
                .onComplete(ignore -> context.verify(() -> {
                    Set<SimpleAclRule> acls = simpleAclOperator.getAcls("my-user");
                    assertThat(acls, hasSize(1));
                    assertThat(acls, hasItem(rule1));
                    async1.countDown();
                }));

        async1.await(TEST_TIMEOUT, TimeUnit.SECONDS);

        CountDownLatch async2 = new CountDownLatch(1);
        simpleAclOperator.reconcile("my-user", null)
                .onComplete(ignore -> context.verify(() -> {
                    Set<SimpleAclRule> acls = simpleAclOperator.getAcls("my-user");
                    assertThat(acls, IsEmptyCollection.empty());
                    async2.countDown();
                }));

        async2.await(TEST_TIMEOUT, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    public void testUsersWithAcls(VertxTestContext context) throws InterruptedException {
        SimpleAclRule rule1 = new SimpleAclRule(
                AclRuleType.ALLOW,
                new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL),
                "*",
                AclOperation.READ);

        CountDownLatch async1 = new CountDownLatch(1);
        simpleAclOperator.reconcile("my-user", Collections.singleton(rule1))
                .onComplete(ignore -> context.verify(() -> {
                    Set<SimpleAclRule> acls = simpleAclOperator.getAcls("my-user");
                    assertThat(acls, hasSize(1));
                    assertThat(acls, hasItem(rule1));
                    async1.countDown();
                }));

        async1.await(TEST_TIMEOUT, TimeUnit.SECONDS);

        SimpleAclRule rule2 = new SimpleAclRule(
                AclRuleType.ALLOW,
                new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL),
                "*",
                AclOperation.WRITE);

        CountDownLatch async2 = new CountDownLatch(1);
        simpleAclOperator.reconcile("my-user-2", Collections.singleton(rule2))
                .onComplete(ignore -> context.verify(() -> {
                    Set<SimpleAclRule> acls = simpleAclOperator.getAcls("my-user-2");
                    assertThat(acls, hasSize(1));
                    assertThat(acls, hasItem(rule2));
                    async2.countDown();
                }));

        async2.await(TEST_TIMEOUT, TimeUnit.SECONDS);

        Set<String> usersWithAcls = simpleAclOperator.getUsersWithAcls();
        context.verify(() -> {
            assertThat(usersWithAcls, hasItems("my-user", "my-user-2"));
        });
        context.completeNow();
    }

    @AfterAll
    public static void afterAll() {
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
        }
        if (vertx != null) {
            vertx.close();
        }
    }
}
