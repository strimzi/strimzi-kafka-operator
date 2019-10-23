/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.TimeoutException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(VertxUnitRunner.class)
public class KafkaRollerTest {

    private static Vertx vertx;
    private List<String> restarted;

    @BeforeClass
    public static void startVertx() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void stopVertx() {
        vertx.close();
    }

    private static int podName2Number(String podName) {
        return Integer.parseInt(podName.substring(ssName().length() + 1));
    }

    private static String clusterName() {
        return "c";
    }

    private static String ssName() {
        return "c-kafka";
    }

    private static String ssNamespace() {
        return "ns";
    }

    @Test
    public void controllerless(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(ss, podOps, -1);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void pod2IsController(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(ss, podOps, 2);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, 2));
    }

    @Test
    public void controllerChangesDuringRoll(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        StatefulSet ss = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(ss, podOps, 0, 1);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(2, 3, 4, 0, 1));
    }

    @Test
    public void pod0NotReadyAfterRolling(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId ->
            podId == 0 ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );
        StatefulSet sts = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(sts, podOps, 2);
        // What does/did the ZK algo do?
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalException.class, "Error while waiting for restarted pod c-kafka-0 to become ready",
                singletonList(0));
        // TODO assert subsequent rolls
    }

    @Test
    public void pod1NotReadyAfterRolling(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId ->
                podId == 1 ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );
        StatefulSet sts = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(sts, podOps, 2);
        // On the first reconciliation we expect it to abort when it gets to pod 1
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalException.class, "Error while waiting for restarted pod c-kafka-1 to become ready",
                asList(1));
        // On the next reconciliation only pod 2 (controller) would need rolling, and we expect it to fail in the same way
        kafkaRoller = rollerWithControllers(sts, podOps, 2);
        clearRestarted();
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(2, 3, 4),
                KafkaRoller.FatalException.class, "Error while waiting for non-restarted pod c-kafka-1 to become ready",
                emptyList());
    }

    @Test
    public void pod3NotReadyAfterRolling(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId ->
                podId == 3 ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );
        StatefulSet sts = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(sts, podOps, 2);
        // On the first reconciliation we expect it to abort when it gets to pod 3 (but not 2, which is controller)
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalException.class, "Error while waiting for restarted pod c-kafka-3 to become ready",
                asList(3));
        // On the next reconciliation only pods 2 (controller) and 4 would need rolling, and we expect it to fail in the same way
        kafkaRoller = rollerWithControllers(sts, podOps, 2);
        clearRestarted();
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 4),
                KafkaRoller.FatalException.class, "Error while waiting for non-restarted pod c-kafka-3 to become ready",
                emptyList());
    }

    @Test
    public void controllerNotReadyAfterRolling(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId ->
                podId == 2 ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );
        StatefulSet sts = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = rollerWithControllers(sts, podOps, 2);
        // On the first reconciliation we expect it to fail when rolling the controller (i.e. as rolling all the rest)
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalException.class, "Error while waiting for restarted pod c-kafka-2 to become ready",
                asList(0, 1, 3, 4, 2));
        // On the next reconciliation only pod 2 would need rolling, and we expect it to fail in the same way
        kafkaRoller = rollerWithControllers(sts, podOps, 2);
        clearRestarted();
        doFailingRollingRestart(testContext, kafkaRoller,
                singletonList(2),
                KafkaRoller.FatalException.class, "Error while waiting for restarted pod c-kafka-2 to become ready",
                singletonList(2));
    }

    @Test
    public void errorWhenOpeningAdminClient(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        StatefulSet sts = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(sts, null, null, podOps,
            new RuntimeException("Test Exception"),
            null, null,
            brokerId -> succeededFuture(true),
            2);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we never find the controller we get ascending order
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void errorWhenGettingController(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        StatefulSet sts = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(sts, null, null, podOps,
            null, null,
            new RuntimeException("Test Exception"),
            brokerId -> succeededFuture(true),
            2);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we never find the controller we get ascending order
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void errorWhenClosingAdminClient(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        StatefulSet sts = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(sts, null, null, podOps,
            null,
            new RuntimeException("Test Exception"), null,
            brokerId -> succeededFuture(true),
            2);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we did the controller we controller last order
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, 2));
    }

    @Test
    public void nonControllerNotInitiallyRollable(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        StatefulSet sts = buildStatefulSet();
        AtomicInteger count = new AtomicInteger(3);
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(sts, null, null, podOps,
            null, null, null,
            brokerId ->
                    brokerId == 1 ? succeededFuture(count.getAndDecrement() == 0)
                            : succeededFuture(true),
            2);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 3, 4, 1, 2));
    }

    private static final Logger log = LogManager.getLogger(KafkaRollerTest.class);

    @Test
    public void controllerNotInitiallyRollable(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        StatefulSet sts = buildStatefulSet();
        AtomicInteger count = new AtomicInteger(2);
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(sts, null, null, podOps,
            null, null, null,
            brokerId -> {
                if (brokerId == 2) {
                    boolean b = count.getAndDecrement() == 0;
                    log.info("Can broker {} be rolled now ? {}", brokerId, b);
                    return succeededFuture(b);
                } else {
                    return succeededFuture(true);
                }
            },
            2);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, 2));
    }

    @Test
    public void nonControllerNeverRollable(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        StatefulSet sts = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(sts, null, null, podOps,
            null, null, null,
            brokerId ->
                    brokerId == 1 ? succeededFuture(false)
                            : succeededFuture(true),
            2);
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.UnforceableException.class, "Pod c-kafka-1 is currently not rollable",
                // Controller last, broker 1 never restarted
                asList(0, 3, 4, 2));
        // TODO assert subsequent rolls
        kafkaRoller = new TestingKafkaRoller(sts, null, null, podOps,
            null, null, null,
            brokerId -> succeededFuture(brokerId != 1),
            2);
        clearRestarted();
        doFailingRollingRestart(testContext, kafkaRoller,
                singletonList(1),
                KafkaRoller.UnforceableException.class, "Pod c-kafka-1 is currently not rollable",
                // Controller last, broker 1 never restarted
                emptyList());
    }

    @Test
    public void controllerNeverRollable(TestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        StatefulSet sts = buildStatefulSet();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(sts, null, null,
                podOps,
            null, null, null,
            brokerId ->
                    brokerId == 2 ? succeededFuture(false)
                            : succeededFuture(true),
            2);
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.UnforceableException.class, "Pod c-kafka-2 is currently not rollable",
                // We expect all non-controller pods to be rolled
                asList(0, 1, 3, 4));
        clearRestarted();
        kafkaRoller = new TestingKafkaRoller(sts, null, null,
            podOps,
            null, null, null,
            brokerId -> succeededFuture(brokerId != 2),
            2);
        doFailingRollingRestart(testContext, kafkaRoller,
                singletonList(2),
                KafkaRoller.UnforceableException.class, "Pod c-kafka-2 is currently not rollable",
                // We expect all non-controller pods to be rolled
                emptyList());
    }

    private TestingKafkaRoller rollerWithControllers(StatefulSet ss, PodOperator podOps, int... controllers) {
        return new TestingKafkaRoller(ss, null, null, podOps,
            null, null, null,
            brokerId -> succeededFuture(true),
            controllers);
    }

    private void doSuccessfulRollingRestart(TestContext testContext, TestingKafkaRoller kafkaRoller,
                                    Collection<Integer> podsToRestart,
                                    List<Integer> expected) {
        Async async = testContext.async();
        kafkaRoller.rollingRestart(pod -> podsToRestart.contains(podName2Number(pod.getMetadata().getName()))).setHandler(ar -> {
            if (ar.failed()) {
                testContext.fail(new RuntimeException("Rolling failed", ar.cause()));
            }
            testContext.assertEquals(expected, restarted());
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

    private void doFailingRollingRestart(TestContext testContext, TestingKafkaRoller kafkaRoller,
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
        if (ar.succeeded()) {
            testContext.fail(new RuntimeException("Rolling succeeded. It should have failed", ar.cause()));
        }
        testContext.assertTrue(exception.isAssignableFrom(ar.cause().getClass()),
                ar.cause().getClass().getName() + " is not a subclass of " + exception.getName());
        testContext.assertEquals(message, ar.cause().getMessage(),
                "The exception message was not as expected");
        testContext.assertEquals(expectedRestart, restarted(),
                "The restarted pods were not as expected");
        assertNoUnclosedAdminClient(testContext, kafkaRoller);
    }

    public List<Integer> restarted() {
        return restarted.stream().map(KafkaRollerTest::podName2Number).collect(Collectors.toList());
    }

    @Before
    public void clearRestarted() {
        restarted = new ArrayList<>();
    }

    private PodOperator mockPodOps(Function<Integer, Future<Void>> readiness) {
        PodOperator podOps = mock(PodOperator.class);
        when(podOps.get(any(), any())).thenAnswer(
            invocation -> new PodBuilder()
                    .withNewMetadata()
                        .withNamespace(invocation.getArgument(0))
                        .withName(invocation.getArgument(1))
                    .endMetadata()
                .build()
        );
        when(podOps.readiness(any(), any(), anyLong(), anyLong())).thenAnswer(invocationOnMock ->  {
            String podName = invocationOnMock.getArgument(1);
            return readiness.apply(podName2Number(podName));
        });
        when(podOps.isReady(anyString(), anyString())).thenAnswer(invocationOnMock ->  {
            String podName = invocationOnMock.getArgument(1);
            Future<Void> ready = readiness.apply(podName2Number(podName));
            if (ready.succeeded()) {
                return true;
            } else {
                if (ready.cause() instanceof TimeoutException) {
                    return false;
                } else {
                    throw ready.cause();
                }
            }
        });
        return podOps;
    }

    private StatefulSet buildStatefulSet() {
        return new StatefulSetBuilder()
                .withNewMetadata()
                .withName(ssName())
                .withNamespace(ssNamespace())
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, clusterName())
                .endMetadata()
                .withNewSpec()
                .withReplicas(5)
                .endSpec()
                .build();
    }

    private class TestingKafkaRoller extends KafkaRoller {

        int controllerCall;
        private final IdentityHashMap<AdminClient, Throwable> unclosedAdminClients;
        private final RuntimeException acOpenException;
        private final Throwable acCloseException;
        private final Function<Integer, Future<Boolean>> canRollFn;
        private final Throwable controllerException;
        private final int[] controllers;

        private TestingKafkaRoller(StatefulSet ss, Secret clusterCaCertSecret, Secret coKeySecret,
                                  PodOperator podOps,
                                  RuntimeException acOpenException, Throwable acCloseException,
                                  Throwable controllerException,
                                  Function<Integer, Future<Boolean>> canRollFn,
                                  int... controllers) {
            super(KafkaRollerTest.vertx, podOps, 500, 1000,
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
                    AdminClient mock = (AdminClient) invocation.getMock();
                    unclosedAdminClients.remove(mock);
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
                    return succeededFuture(Collections.emptySet());
                }

                @Override
                protected Future<Collection<TopicDescription>> describeTopics(Set<String> names) {
                    return succeededFuture(Collections.emptySet());
                }

                @Override
                Future<Boolean> canRoll(int podId) {
                    return canRollFn.apply(podId);
                }
            };
        }

        @Override
        int controller(int podId, AdminClient ac, long timeout, TimeUnit unit) throws ForceableException {
            if (controllerException != null) {
                throw new ForceableException("An error while trying to determine the cluster controller from pod " + podName(podId), controllerException);
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
            return succeededFuture();
        }

    }

    // TODO Error when finding the next broker
}
