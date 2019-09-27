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
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;


@RunWith(VertxUnitRunner.class)
public class KafkaRollerTest extends AbstractRollerTest {

    @Test
    public void controllerless(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(ss, podOps, -1);
        doSuccessfulRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-1, c-kafka-2, c-kafka-3, c-kafka-4]");
    }

    @Test
    public void pod2IsController(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(ss, podOps, 2);
        doSuccessfulRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-1, c-kafka-3, c-kafka-4, c-kafka-2]");
    }

    @Test
    public void controllerChangesDuringRoll(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(ss, podOps, 0, 1);
        doSuccessfulRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-2, c-kafka-3, c-kafka-4, c-kafka-0, c-kafka-1]");
    }

    @Test
    public void pod0NotReadyAfterRolling(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId ->
            podId == 0 ? Future.failedFuture(new TimeoutException("Timeout")) : Future.succeededFuture()
        );
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(ss, podOps, 2);
        // What does/did the ZK algo do?
        doFailingRollingRestart(testContext, ss, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller2.FatalException.class, "An error while waiting for a pod to become ready",
                asList(0));
        // TODO assert subsequent rolls
    }

    @Test
    public void pod1NotReadyAfterRolling(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId ->
                podId == 1 ? Future.failedFuture(new TimeoutException("Timeout")) : Future.succeededFuture()
        );
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(ss, podOps, 2);
        // On the first reconciliation we expect it to abort when it gets to pod 1
        doFailingRollingRestart(testContext, ss, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller2.FatalException.class, "An error while waiting for a pod to become ready",
                asList(0, 1));
        // On the next reconciliation only pod 2 (controller) would need rolling, and we expect it to fail in the same way

        kafkaRoller = rollerWithControllers(ss, podOps, 2);
        doFailingRollingRestart(testContext, ss, kafkaRoller,
                asList(2, 3, 4),
                KafkaRoller2.FatalException.class, "An error while waiting for a pod to become ready",
                asList(2));
    }

    @Test
    public void pod3NotReadyAfterRolling(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId ->
                podId == 3 ? Future.failedFuture(new TimeoutException("Timeout")) : Future.succeededFuture()
        );
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(ss, podOps, 2);
        // On the first reconciliation we expect it to abort when it gets to pod 3 (but not 2, which is controller)
        doFailingRollingRestart(testContext, ss, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller2.FatalException.class, "An error while waiting for a pod to become ready",
                asList(0, 1, 3));
        // On the next reconciliation only pod 2 (controller) would need rolling, and we expect it to fail in the same way
        kafkaRoller = rollerWithControllers(ss, podOps, 2);
        doFailingRollingRestart(testContext, ss, kafkaRoller,
                asList(2, 4),
                KafkaRoller2.FatalException.class, "An error while waiting for a pod to become ready",
                asList(4, 2));
    }

    @Test
    public void controllerNotReadyAfterRolling(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId ->
                podId == 2 ? Future.failedFuture(new TimeoutException("Timeout")) : Future.succeededFuture()
        );
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(ss, podOps, 2);
        // On the first reconciliation we expect it to fail when rolling the controller (i.e. as rolling all the rest)
        doFailingRollingRestart(testContext, ss, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller2.FatalException.class, "An error while waiting for a pod to become ready",
                asList(0, 1, 3, 4, 2));
        // On the next reconciliation only pod 2 would need rolling, and we expect it to fail in the same way
        doFailingRollingRestart(testContext, ss, kafkaRoller,
                asList(2),
                KafkaRoller2.FatalException.class, "An error while waiting for a pod to become ready",
                asList(2));
    }

    @Test
    public void errorWhenOpeningAdminClient(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> Future.succeededFuture());
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
        PodOperator podOps = mockPodOps(podId -> Future.succeededFuture());
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
        PodOperator podOps = mockPodOps(podId -> Future.succeededFuture());
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
        PodOperator podOps = mockPodOps(podId -> Future.succeededFuture());
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
        PodOperator podOps = mockPodOps(podId -> Future.succeededFuture());
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
        PodOperator podOps = mockPodOps(podId -> Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(ss, null, null, podOps,
            null, null, null,
            brokerId ->
                    brokerId == 1 ? Future.succeededFuture(false)
                            : Future.succeededFuture(true),
            2);
        doFailingRollingRestart(testContext, ss, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller2.UnforceableException.class, "The pod currently is not rollable",
                // Controller last, broker 1 never restarted
                asList(0, 3, 4, 2));
        // TODO assert subsequent rolls
    }

    @Test
    public void controllerNeverRollable(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(ss, null, null,
                podOps,
            null, null, null,
            brokerId ->
                    brokerId == 2 ? Future.succeededFuture(false)
                            : Future.succeededFuture(true),
            2);
        doFailingRollingRestart(testContext, ss, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller2.UnforceableException.class, "The pod currently is not rollable",
                // We expect all non-controller pods to be rolled
                asList(0, 1, 3, 4));
        // TODO assert subsequent rolls
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

    void doFailingRollingRestart(TestContext testContext, StatefulSet ss, TestingKafkaRoller kafkaRoller,
                                 Collection<Integer> podsToRestart,
                                 Class<? extends Throwable> exception, String message,
                                 List<Integer> expectedRestart) {
        Async async = testContext.async();
        AtomicReference<AsyncResult<Void>> arReference = new AtomicReference<>();
        kafkaRoller.rollingRestart(pod -> podsToRestart.contains(podName2Number(pod.getMetadata().getName())))
            .setHandler(ar -> {
                    arReference.set(ar);
                    async.complete();
                }
            );
        async.await();
        AsyncResult<Void> ar = arReference.get();
        testContext.assertEquals(expectedRestart, restarted(),
                "The restarted pods were not as expected");
        if (ar.succeeded()) {
            testContext.fail(new RuntimeException("Rolling succeeded. It should have failed", ar.cause()));
        }
        testContext.assertTrue(exception.isAssignableFrom(ar.cause().getClass()),
                ar.cause().getClass().getName() + " is not a subclass of " + exception.getName());
        testContext.assertEquals(message, ar.cause().getMessage(),
                "The exception message was not as expected");

        assertNoUnclosedAdminClient(testContext, kafkaRoller);
    }

    private class TestingKafkaRoller extends KafkaRoller2 {

        int controllerCall;
        private final IdentityHashMap<AdminClient, Throwable> unclosedAdminClients;
        private final RuntimeException acOpenException;
        private final Throwable acCloseException;
        private final Function<Integer, Future<Boolean>> canRollFn;
        private final Throwable controllerException;
        private final int[] controllers;

        public TestingKafkaRoller(StatefulSet ss, Secret clusterCaCertSecret, Secret coKeySecret,
                                  PodOperator podOps,
                                  RuntimeException acOpenException, Throwable acCloseException,
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
        protected AdminClient adminClient(Integer podId) throws ForceableException {
            if (acOpenException != null) {
                throw new ForceableException("An error while try to create the admin client", acOpenException);
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
            return ac;
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
        int controller(AdminClient ac, long timeout, TimeUnit unit) throws ForceableException {
            if (controllerException != null) {
                throw new ForceableException("An error while trying to determine the cluster controller", controllerException);
            } else {
                int index;
                if (controllerCall < controllers.length) {
                    index = controllerCall;
                } else {
                    index = controllers.length - 1;
                }
                controllerCall++;
                return controllers[index];
            }
        }

        @Override
        protected Future<Void> restart(Pod pod) {
            restarted.add(pod.getMetadata().getName());
            return Future.succeededFuture();
        }

    }

    // TODO Error when finding the next broker
}
