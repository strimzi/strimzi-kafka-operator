/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.TimeoutException;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.mockito.Mockito.mock;


@RunWith(VertxUnitRunner.class)
public class KafkaRollerTest extends AbstractRollerTest {

    @Test
    public void controllerless(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(ss, podOps, -1);
        doSuccessfulRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-1, c-kafka-2, c-kafka-3, c-kafka-4]");
    }

    @Test
    public void pod2IsController(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(ss, podOps, 2);
        doSuccessfulRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-1, c-kafka-3, c-kafka-4, c-kafka-2]");
    }

    @Test
    public void controllerChangesDuringRoll(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(ss, podOps, 0, 1);
        doSuccessfulRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-2, c-kafka-3, c-kafka-4, c-kafka-0, c-kafka-1]");
    }

    @Test
    public void podNotReadyAfterRolling(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.failedFuture(new TimeoutException("Timeout")));
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(ss, podOps, 1);
        // What does/did the ZK algo do?
        doFailingRollingRestart(testContext, ss, kafkaRoller,
                TimeoutException.class);
    }

    @Test
    public void errorWhenOpeningAdminClient(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture(null));
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(ss, null, null, podOps,
            new RuntimeException("Test Exception"),
            null, null,
            brokerId -> Future.succeededFuture(true),
            2);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we never find the controller we get ascending order
        doSuccessfulRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-1, c-kafka-2, c-kafka-3, c-kafka-4]");
    }

    @Test
    public void errorWhenGettingController(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture(null));
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(ss, null, null, podOps,
            null, null,
            new RuntimeException("Test Exception"),
            brokerId -> Future.succeededFuture(true),
            2);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we never find the controller we get ascending order
        doSuccessfulRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-1, c-kafka-2, c-kafka-3, c-kafka-4]");
    }

    @Test
    public void errorWhenClosingAdminClient(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(ss, null, null, podOps,
            null,
            new RuntimeException("Test Exception"), null,
            brokerId -> Future.succeededFuture(true),
            2);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we did the controller we controller last order
        doSuccessfulRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-1, c-kafka-3, c-kafka-4, c-kafka-2]");
    }

    @Test
    public void nonControllerNotInitiallyRollable(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        AtomicInteger count = new AtomicInteger(3);
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(ss, null, null, podOps,
            null, null, null,
            brokerId ->
                    brokerId == 1 ? Future.succeededFuture(count.getAndDecrement() == 0 ? true : false)
                            : Future.succeededFuture(true),
            2);
        doSuccessfulRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-3, c-kafka-4, c-kafka-1, c-kafka-2]");
    }

    @Test
    public void controllerNotInitiallyRollable(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        AtomicInteger count = new AtomicInteger(3);
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(ss, null, null, podOps,
            null, null, null,
            brokerId ->
                    brokerId == 2 ? Future.succeededFuture(count.getAndDecrement() == 0 ? true : false)
                            : Future.succeededFuture(true),
            2);
        doSuccessfulRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-1, c-kafka-3, c-kafka-4, c-kafka-2]");
    }

    @Test
    public void nonControllerNeverRollable(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(ss, null, null, podOps,
            null, null, null,
            brokerId ->
                    brokerId == 1 ? Future.succeededFuture(false)
                            : Future.succeededFuture(true),
            2);
        doFailingRollingRestart(testContext, ss, kafkaRoller,
                AbortRollException.class);
    }

    @Test
    public void controllerNeverRollable(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(ss, null, null,
                podOps,
            null, null, null,
            brokerId ->
                    brokerId == 2 ? Future.succeededFuture(false)
                            : Future.succeededFuture(true),
            2);
        doFailingRollingRestart(testContext, ss, kafkaRoller,
                AbortRollException.class);
    }

    TestingKafkaRoller rollerWithControllers(StatefulSet ss, PodOperator podOps, int... controllers) {
        return new TestingKafkaRoller(ss, null, null, podOps,
            null, null, null,
            brokerId -> Future.succeededFuture(true),
            controllers);
    }

    void doSuccessfulRollingRestart(TestContext testContext, StatefulSet ss, TestingKafkaRoller kafkaRoller, String expected) {
        Async async = testContext.async();
        kafkaRoller.rollingRestart(pod -> true).setHandler(ar -> {
            if (ar.failed()) {
                testContext.fail(new RuntimeException("Rolling failed", ar.cause()));
            }
            testContext.assertEquals(expected, restarted.toString());
            assertNoUnclosedAdminClient(testContext, kafkaRoller);
            async.complete();
        });
    }

    private void assertNoUnclosedAdminClient(TestContext testContext, TestingKafkaRoller kafkaRoller) {
        if (!kafkaRoller.unclosedAdminClients.isEmpty()) {
            Throwable alloc = kafkaRoller.unclosedAdminClients.values().iterator().next();
            alloc.printStackTrace(System.out);
            testContext.fail(kafkaRoller.unclosedAdminClients.size() + " unclosed AdminClient instances");
        }
    }

    void doFailingRollingRestart(TestContext testContext, StatefulSet ss, TestingKafkaRoller kafkaRoller, Class<? extends Throwable> exception) {
        Async async = testContext.async();
        kafkaRoller.rollingRestart(pod -> true).setHandler(ar -> {
            if (ar.succeeded()) {
                testContext.fail(new RuntimeException("Rolling succeeded. It should have failed", ar.cause()));
            }
            testContext.assertTrue(exception.isAssignableFrom(ar.cause().getClass()),
                    ar.cause().getClass().getName() + " is not a subclass of " + exception.getName());
            assertNoUnclosedAdminClient(testContext, kafkaRoller);
            async.complete();
        });
    }

    @Override
    String ssName() {
        return "c-kafka";
    }

    private class TestingKafkaRoller extends KafkaRoller {

        int controllerCall;
        private final IdentityHashMap<AdminClient, Throwable> unclosedAdminClients;
        private final Throwable acOpenException;
        private final Throwable acCloseException;
        private final Function<Integer, Future<Boolean>> canRollFn;
        private final Throwable controllerException;
        private final int[] controllers;

        public TestingKafkaRoller(StatefulSet ss, Secret clusterCaCertSecret, Secret coKeySecret,
                                  PodOperator podOps,
                                  Throwable acOpenException, Throwable acCloseException,
                                  Throwable controllerException,
                                  Function<Integer, Future<Boolean>> canRollFn,
                                  int... controllers) {
            super(KafkaRollerTest.this.vertx, podOps, 500, 1000,
                () -> new BackOff(10L, 2, 4),
                ss, clusterCaCertSecret, coKeySecret);
            this.controllers = controllers;
            this.controllerCall = 0;
            this.acOpenException = acOpenException;
            this.controllerException = controllerException;
            this.acCloseException = acCloseException;
            this.canRollFn = canRollFn;
            this.unclosedAdminClients = new IdentityHashMap<>();
        }

        @Override
        protected Future<AdminClient> adminClient(Integer podId) {
            if (acOpenException != null) {
                return Future.failedFuture(acOpenException);
            }
            AdminClient ac = mock(AdminClient.class, invocation -> {
                if ("close".equals(invocation.getMethod().getName())) {
                    unclosedAdminClients.remove(invocation.getMock());
                    if (acCloseException != null) {
                        throw acCloseException;
                    }
                    return null;
                }
                throw new RuntimeException("Not mocked " + invocation.getMethod());
            });
            unclosedAdminClients.put(ac, new Throwable("Pod " + podId));
            return Future.succeededFuture(ac);
        }

        @Override
        protected KafkaAvailability availability(AdminClient ac) {
            return new KafkaAvailability(null) {
                @Override
                protected Future<Set<String>> topicNames() {
                    return Future.succeededFuture(Collections.emptySet());
                }

                @Override
                protected Future<Collection<TopicDescription>> describeTopics(Set<String> names) {
                    return Future.succeededFuture(Collections.emptySet());
                }

                @Override
                Future<Boolean> canRoll(int podId) {
                    return canRollFn.apply(podId);
                }
            };
        }

        @Override
        Future<Integer> controller(AdminClient ac) {
            Future<Integer> result = Future.future();
            vertx.runOnContext(ignored -> {
                if (controllerException != null) {
                    result.fail(controllerException);
                } else {
                    int index;
                    if (controllerCall < controllers.length) {
                        index = controllerCall;
                    } else {
                        index = controllers.length - 1;
                    }
                    controllerCall++;
                    result.complete(controllers[index]);
                }
            });
            return result;
        }

        @Override
        protected Future<Void> restart(Pod pod) {
            restarted.add(pod.getMetadata().getName());
            return Future.succeededFuture();
        }

    }

    // TODO Error when finding the next broker
}
