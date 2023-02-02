/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.TimeoutException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaRollerTest {

    private static final Logger LOGGER = LogManager.getLogger(KafkaRollerTest.class);

    private static final int REPLICAS = 5;

    private static Vertx vertx;
    private List<String> restarted;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
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

    private static String stsNamespace() {
        return "ns";
    }

    private static <X, E extends Throwable> Function<X, E> noException() {
        return podId -> null;
    }

    @Test
    public void testRollWithNoController(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = rollerWithControllers(podOps, -1);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void testRollTcpProbe(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        AdminClientProvider mock = givenControllerFutureFailsWithTimeout();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(KafkaRollerTest.REPLICAS), podOps,
                noException(), null, noException(), noException(), noException(),
                brokerId -> succeededFuture(true),
                true, mock, true, -1);
        List<String> expectedTcpProbes = List.of(
                "c-kafka-0.c-kafka-brokers.ns.svc.cluster.local:9091",
                "c-kafka-1.c-kafka-brokers.ns.svc.cluster.local:9091",
                "c-kafka-2.c-kafka-brokers.ns.svc.cluster.local:9091",
                "c-kafka-3.c-kafka-brokers.ns.svc.cluster.local:9091",
                "c-kafka-4.c-kafka-brokers.ns.svc.cluster.local:9091"
        );
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4), () -> {
                    assertEquals(kafkaRoller.tcpProbes, expectedTcpProbes);
                });
    }

    private static AdminClientProvider givenControllerFutureFailsWithTimeout() {
        KafkaFutureImpl<Node> controllerFuture = new KafkaFutureImpl<>();
        controllerFuture.completeExceptionally(new java.util.concurrent.TimeoutException("fail"));
        DescribeClusterResult mockResult = mock(DescribeClusterResult.class);
        when(mockResult.controller()).thenReturn(controllerFuture);
        Admin admin = mock(Admin.class);
        when(admin.describeCluster()).thenReturn(mockResult);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any(), anyString())).thenReturn(admin);
        return mock;
    }


    @Test
    public void testRollWithPod2AsController(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = rollerWithControllers(podOps, 2);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, 2));
    }

    @Test
    public void tesRollWithtAControllerChange(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = rollerWithControllers(podOps, 0, 1);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(2, 3, 4, 0, 1));
    }

    @Test
    public void pod0NotReadyAfterRolling(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
            podId == 0 ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );
        TestingKafkaRoller kafkaRoller = rollerWithControllers(podOps, 2);
        // What does/did the ZK algo do?
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-0 to become ready",
                singletonList(0));
        // TODO assert subsequent rolls
    }

    @Test
    public void pod1NotReadyAfterRolling(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
                podId == 1 ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );
        TestingKafkaRoller kafkaRoller = rollerWithControllers(podOps, 2);
        // On the first reconciliation we expect it to abort when it gets to pod 1
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-1 to become ready",
                asList(1));
        // On the next reconciliation only pod 2 (controller) would need rolling, and we expect it to fail in the same way
        kafkaRoller = rollerWithControllers(podOps, 2);
        clearRestarted();
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-1 to become ready",
                emptyList());
    }

    @Test
    public void pod3NotReadyAfterRolling(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
                podId == 3 ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );
        TestingKafkaRoller kafkaRoller = rollerWithControllers(podOps, 2);
        // On the first reconciliation we expect it to abort when it gets to pod 3 (but not 2, which is controller)
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-3 to become ready",
                asList(3));
        // On the next reconciliation only pods 2 (controller) and 4 would need rolling, and we expect it to fail in the same way
        kafkaRoller = rollerWithControllers(podOps, 2);
        clearRestarted();
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-3 to become ready",
                emptyList());
    }

    @Test
    public void controllerNotReadyAfterRolling(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId ->
                podId == 2 ? failedFuture(new TimeoutException("Timeout")) : succeededFuture()
        );
        TestingKafkaRoller kafkaRoller = rollerWithControllers(podOps, 2);
        // On the first reconciliation we expect it to fail when rolling the controller (i.e. as rolling all the rest)
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-2 to become ready",
                asList(0, 1, 3, 4, 2));
        // On the next reconciliation only pod 2 would need rolling, and we expect it to fail in the same way
        kafkaRoller = rollerWithControllers(podOps, 2);
        clearRestarted();
        doFailingRollingRestart(testContext, kafkaRoller,
                singletonList(2),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-2 to become ready",
                singletonList(2));
    }

    public List<String> addPodNames(int replicas) {
        ArrayList<String> podNames = new ArrayList<>(replicas);

        for (int podId = 0; podId < replicas; podId++) {
            podNames.add(KafkaResources.kafkaPodName(clusterName(), podId));
        }

        return podNames;
    }

    public List<String> addDisconnectedPodNames(int replicas) {
        ArrayList<String> podNames = new ArrayList<>(replicas);

        podNames.add(KafkaResources.kafkaPodName(clusterName(), 10));
        podNames.add(KafkaResources.kafkaPodName(clusterName(), 200));
        podNames.add(KafkaResources.kafkaPodName(clusterName(), 30));
        podNames.add(KafkaResources.kafkaPodName(clusterName(), 400));
        podNames.add(KafkaResources.kafkaPodName(clusterName(), 500));
        return podNames;
    }

    @Test
    public void testRollWithDisconnectedPodNames(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addDisconnectedPodNames(REPLICAS), podOps,
                bootstrapBrokers -> bootstrapBrokers != null && bootstrapBrokers.equals(singletonList(1)) ? new RuntimeException("Test Exception") : null,
                null, noException(), noException(), noException(),
                brokerId -> succeededFuture(true),
                false, new DefaultAdminClientProvider(), false, 30);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we never find the controller we get ascending order
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(10, 200, 30, 400, 500),
                asList(10, 200, 400, 500, 30));
    }


    @Test
    public void testRollHandlesErrorWhenOpeningAdminClient(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS), podOps,
            bootstrapBrokers -> bootstrapBrokers != null && bootstrapBrokers.equals(singletonList(1)) ? new RuntimeException("Test Exception") : null,
            null, noException(), noException(), noException(),
            brokerId -> succeededFuture(true),
                false, new DefaultAdminClientProvider(), false, 2);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we never find the controller we get ascending order
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, 2));
    }

    @Test
    public void testRollHandlesErrorWhenGettingControllerFromNonController(VertxTestContext testContext) {
        int controller = 2;
        int nonController = 1;
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS), podOps,
                noException(), null,
            podId -> podId == nonController ? new RuntimeException("Test Exception") : null, noException(), noException(),
            brokerId -> succeededFuture(true),
                false, new DefaultAdminClientProvider(), false, controller);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we never find the controller we get ascending order
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 3, 4, nonController, controller));
    }

    @Test
    public void testRollHandlesErrorWhenGettingControllerFromController(VertxTestContext testContext) {
        int controller = 2;
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS), podOps,
            noException(), null,
            podId -> podId == controller ? new RuntimeException("Test Exception") : null, noException(), noException(),
            brokerId -> succeededFuture(true),
                false, new DefaultAdminClientProvider(), false, controller);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we never find the controller we get ascending order
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, controller));
    }

    @Test
    public void testRollHandlesErrorWhenClosingAdminClient(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS), podOps,
            noException(),
            new RuntimeException("Test Exception"), noException(), noException(), noException(),
            brokerId -> succeededFuture(true),
                false, new DefaultAdminClientProvider(), false, 2);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we did the controller we controller last order
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, 2));
    }

    @Test
    public void testNonControllerNotInitiallyRollable(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        AtomicInteger count = new AtomicInteger(3);
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS), podOps,
                noException(), null, noException(), noException(), noException(),
            brokerId ->
                    brokerId == 1 ? succeededFuture(count.getAndDecrement() == 0)
                            : succeededFuture(true),
                false, new DefaultAdminClientProvider(), false, 2);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 3, 4, 1, 2));
    }

    @Test
    public void testControllerNotInitiallyRollable(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        AtomicInteger count = new AtomicInteger(2);
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS), podOps,
                noException(), null, noException(), noException(), noException(),
            brokerId -> {
                if (brokerId == 2) {
                    boolean b = count.getAndDecrement() == 0;
                    LOGGER.info("Can broker {} be rolled now ? {}", brokerId, b);
                    return succeededFuture(b);
                } else {
                    return succeededFuture(true);
                }
            },
                false, new DefaultAdminClientProvider(), false, 2);
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, 2));
    }

    @Test
    public void testNonControllerNeverRollable(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS), podOps,
                noException(), null, noException(), noException(), noException(),
            brokerId ->
                    brokerId == 1 ? succeededFuture(false)
                            : succeededFuture(true),
                false, new DefaultAdminClientProvider(), false, 2);
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-1 cannot be updated right now.",
                // Controller last, broker 1 never restarted
                asList(0, 3, 4, 2));
        // TODO assert subsequent rolls
        kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS), podOps,
                noException(), null, noException(), noException(), noException(),
            brokerId -> succeededFuture(brokerId != 1),
                false, new DefaultAdminClientProvider(), false, 2);
        clearRestarted();
        doFailingRollingRestart(testContext, kafkaRoller,
                singletonList(1),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-1 cannot be updated right now.",
                // Controller last, broker 1 never restarted
                emptyList());
    }

    @Test
    public void testControllerNeverRollable(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS),
                podOps,
                noException(), null, noException(), noException(), noException(),
            brokerId ->
                    brokerId == 2 ? succeededFuture(false)
                            : succeededFuture(true),
                false, new DefaultAdminClientProvider(), false, 2);
        doFailingRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-2 cannot be updated right now.",
                // We expect all non-controller pods to be rolled
                asList(0, 1, 3, 4));
        clearRestarted();
        kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS),
            podOps,
                noException(), null, noException(), noException(), noException(),
            brokerId -> succeededFuture(brokerId != 2),
                false, new DefaultAdminClientProvider(), false, 2);
        doFailingRollingRestart(testContext, kafkaRoller,
                singletonList(2),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-2 cannot be updated right now.",
                // We expect all non-controller pods to be rolled
                emptyList());
    }

    @Test
    public void testRollHandlesErrorWhenGettingConfigFromNonController(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS), podOps,
                noException(), null,
                noException(), noException(), podId -> podId == 1 ? new KafkaRoller.ForceableProblem("could not get config exception") : null,
            brokerId -> succeededFuture(true), false, new DefaultAdminClientProvider(), false, 2);
        // The algorithm should carry on rolling the pods
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 3, 4, 1, 2));
    }

    @Test
    public void testRollHandlesErrorWhenGettingConfigFromController(VertxTestContext testContext) {
        int controller = 2;
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS), podOps,
            noException(), null,
            noException(), noException(), podId -> podId == controller ? new KafkaRoller.ForceableProblem("could not get config exception") : null,
            brokerId -> succeededFuture(true), false, new DefaultAdminClientProvider(), false, controller);
        // The algorithm should carry on rolling the pods
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, controller));
    }

    @Test
    public void testRollHandlesErrorWhenAlteringConfig(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS), podOps,
                noException(), null,
                noException(), podId -> new KafkaRoller.ForceableProblem("could not get alter exception"), noException(),
            brokerId -> succeededFuture(true), false, new DefaultAdminClientProvider(), false, 2);
        // The algorithm should carry on rolling the pods
        doSuccessfulRollingRestart(testContext, kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, 2));
    }

    @Test
    public void testSuccessfulAlteringConfigNotRoll(VertxTestContext testContext) {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS), podOps,
                noException(), null,
                noException(), noException(), noException(),
            brokerId -> succeededFuture(true), false, new DefaultAdminClientProvider(), false, 2);
        // The algorithm should carry on rolling the pods
        doSuccessfulConfigUpdate(testContext, kafkaRoller,
                emptyList());
    }

    @Test
    public void testControllerAndOneMoreNeverRollable(VertxTestContext testContext) throws InterruptedException {
        PodOperator podOps = mockPodOps(podId -> succeededFuture());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(null, null, addPodNames(REPLICAS),
            podOps,
            noException(), null, noException(), noException(), noException(),
            brokerId -> brokerId == 2 || brokerId == 3 ? succeededFuture(false) : succeededFuture(true),
                false, new DefaultAdminClientProvider(), false, 2);
        doFailingRollingRestart(testContext, kafkaRoller,
            asList(0, 1, 2, 3, 4),
            KafkaRoller.ForceableProblem.class, "Pod c-kafka-2 is controller and there are other pods to verify. Non-controller pods will be verified first",
            // We expect all non-controller pods to be rolled
            asList(0, 1, 4));
    }

    private TestingKafkaRoller rollerWithControllers(PodOperator podOps, int... controllers) {
        return new TestingKafkaRoller(null, null, addPodNames(KafkaRollerTest.REPLICAS), podOps,
                noException(), null, noException(), noException(), noException(),
            brokerId -> succeededFuture(true),
                false, new DefaultAdminClientProvider(), false, controllers);
    }

    private void doSuccessfulConfigUpdate(VertxTestContext testContext, TestingKafkaRoller kafkaRoller,
                                            List<Integer> expected) {
        Checkpoint async = testContext.checkpoint();
        kafkaRoller.rollingRestart(pod -> RestartReasons.empty())
                .onComplete(testContext.succeeding(v -> {
                    testContext.verify(() -> assertThat(restarted(), is(expected)));
                    assertNoUnclosedAdminClient(testContext, kafkaRoller);
                    async.flag();
                }));
    }

    private void doSuccessfulRollingRestart(VertxTestContext testContext, TestingKafkaRoller kafkaRoller,
                                    Collection<Integer> podsToRestart,
                                    List<Integer> expected) {
        doSuccessfulRollingRestart(testContext, kafkaRoller, podsToRestart, expected, null);
    }

    private void doSuccessfulRollingRestart(VertxTestContext testContext, TestingKafkaRoller kafkaRoller,
                                            Collection<Integer> podsToRestart,
                                            List<Integer> expected, Runnable onCompletion) {
        Checkpoint async = testContext.checkpoint();
        kafkaRoller.rollingRestart(pod -> {
            if (podsToRestart.contains(podName2Number(pod.getMetadata().getName()))) {
                return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
            } else {
                return RestartReasons.empty();
            }
        })
            .onComplete(testContext.succeeding(v -> {
                testContext.verify(() -> assertThat(restarted(), is(expected)));
                assertNoUnclosedAdminClient(testContext, kafkaRoller);
                if (onCompletion != null) {
                    onCompletion.run();
                }
                async.flag();
            }));
    }

    private void assertNoUnclosedAdminClient(VertxTestContext testContext, TestingKafkaRoller kafkaRoller) {
        if (!kafkaRoller.unclosedAdminClients.isEmpty()) {
            Throwable alloc = kafkaRoller.unclosedAdminClients.values().iterator().next();
            alloc.printStackTrace(System.out);
            testContext.failNow(new Throwable(kafkaRoller.unclosedAdminClients.size() + " unclosed AdminClient instances"));
        }
    }

    private void doFailingRollingRestart(VertxTestContext testContext, TestingKafkaRoller kafkaRoller,
                                 Collection<Integer> podsToRestart,
                                 Class<? extends Throwable> exception, String message,
                                 List<Integer> expectedRestart) throws InterruptedException {
        CountDownLatch async = new CountDownLatch(1);
        kafkaRoller.rollingRestart(pod -> {
            if (podsToRestart.contains(podName2Number(pod.getMetadata().getName()))) {
                return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
            } else {
                return RestartReasons.empty();
            }
        })
            .onComplete(testContext.failing(e -> testContext.verify(() -> {
                try {
                    assertThat(e.getClass() + " is not a subclass of " + exception.getName(), e, instanceOf(exception));
                    assertThat("The exception message was not as expected", e.getMessage(), is(message));
                    assertThat("The restarted pods were not as expected", restarted(), is(expectedRestart));
                    assertNoUnclosedAdminClient(testContext, kafkaRoller);
                    testContext.completeNow();
                } finally {
                    async.countDown();
                }
            })));
        async.await();
    }

    public List<Integer> restarted() {
        return restarted.stream().map(KafkaRollerTest::podName2Number).collect(Collectors.toList());
    }

    @BeforeEach
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
        when(podOps.readiness(any(), any(), any(), anyLong(), anyLong())).thenAnswer(invocationOnMock ->  {
            String podName = invocationOnMock.getArgument(2);
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

    private class TestingKafkaRoller extends KafkaRoller {

        int controllerCall;
        private final IdentityHashMap<Admin, Throwable> unclosedAdminClients;
        private final Function<List<Integer>, RuntimeException> acOpenException;
        private final Throwable acCloseException;
        private final Function<Integer, Future<Boolean>> canRollFn;
        private final Function<Integer, Throwable> controllerException;
        private final Function<Integer, ForceableProblem> alterConfigsException;
        private final Function<Integer, ForceableProblem> getConfigsException;
        private boolean delegateControllerCall;
        private boolean delegateAdminClientCall;
        private final int[] controllers;
        private List<String> tcpProbes = new ArrayList<>();

        @SuppressWarnings("checkstyle:ParameterNumber")
        private TestingKafkaRoller(Secret clusterCaCertSecret, Secret coKeySecret, List<String> podList,
                                   PodOperator podOps,
                                   Function<List<Integer>, RuntimeException> acOpenException,
                                   Throwable acCloseException,
                                   Function<Integer, Throwable> controllerException,
                                   Function<Integer, ForceableProblem> alterConfigsException,
                                   Function<Integer, ForceableProblem> getConfigsException,
                                   Function<Integer, Future<Boolean>> canRollFn,
                                   boolean delegateControllerCall,
                                   AdminClientProvider adminClientProvider,
                                   boolean delegateAdminClientCall, int... controllers) {
            super(
                    new Reconciliation("test", "Kafka", stsNamespace(), clusterName()),
                    KafkaRollerTest.vertx,
                    podOps,
                    500,
                    1000,
                    () -> new BackOff(10L, 2, 4),
                    podList,
                    clusterCaCertSecret,
                    coKeySecret,
                    adminClientProvider,
                    brokerId -> "",
                    "",
                    KafkaVersionTestUtils.getLatestVersion(),
                    true,
                    mock(KubernetesRestartEventPublisher.class)
            );
            this.delegateControllerCall = delegateControllerCall;
            this.delegateAdminClientCall = delegateAdminClientCall;
            this.controllers = controllers;
            this.controllerCall = 0;
            Objects.requireNonNull(acOpenException);
            this.acOpenException = acOpenException;
            this.controllerException = controllerException;
            this.alterConfigsException = alterConfigsException;
            this.getConfigsException = getConfigsException;
            this.acCloseException = acCloseException;
            this.canRollFn = canRollFn;
            this.unclosedAdminClients = new IdentityHashMap<>();
        }

        @Override
        protected Admin adminClient(List<Integer> bootstrapBrokers, boolean b) throws ForceableProblem, FatalProblem {
            if (delegateAdminClientCall) {
                return super.adminClient(bootstrapBrokers, b);
            }
            RuntimeException exception = acOpenException.apply(bootstrapBrokers);
            if (exception != null) {
                throw new ForceableProblem("An error while try to create the admin client", exception);
            }
            Admin ac = mock(AdminClient.class, invocation -> {
                if ("close".equals(invocation.getMethod().getName())) {
                    Admin mock = (Admin) invocation.getMock();
                    unclosedAdminClients.remove(mock);
                    if (acCloseException != null) {
                        throw acCloseException;
                    }
                    return null;
                }
                throw new RuntimeException("Not mocked " + invocation.getMethod());
            });
            unclosedAdminClients.put(ac, new Throwable("Pod " + bootstrapBrokers));
            return ac;
        }

        @Override
        protected KafkaAvailability availability(Admin ac) {
            return new KafkaAvailability(null, null) {
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
        int controller(PodRef podRef, long timeout, TimeUnit unit, RestartContext restartContext) throws Exception {
            if (delegateControllerCall) {
                return super.controller(podRef, timeout, unit, restartContext);
            }
            Throwable throwable = controllerException.apply(podRef.getPodId());
            if (throwable != null) {
                throw new ForceableProblem("An error while trying to determine the cluster controller from pod " + podRef.getPodName(), throwable);
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
        protected Config brokerConfig(PodRef podRef) throws ForceableProblem {
            ForceableProblem problem = getConfigsException.apply(podRef.getPodId());
            if (problem != null) {
                throw problem;
            } else return new Config(emptyList());
        }

        @Override
        protected Config brokerLogging(int brokerId) {
            return new Config(emptyList());
        }

        @Override
        protected void dynamicUpdateBrokerConfig(PodRef podRef, Admin ac, KafkaBrokerConfigurationDiff configurationDiff, KafkaBrokerLoggingConfigurationDiff logDiff) throws ForceableProblem {
            ForceableProblem problem = alterConfigsException.apply(podRef.getPodId());
            if (problem != null) {
                throw problem;
            }
        }

        @Override
        protected Future<Void> restart(Pod pod, RestartContext restartContext) {
            restarted.add(pod.getMetadata().getName());
            return succeededFuture();
        }

        @Override
        protected void tcpProbe(String hostname, int port) throws IOException {
            tcpProbes.add(hostname + ":" + port);
            throw new IOException("mock probe failure");
        }
    }

    // TODO Error when finding the next broker
}
