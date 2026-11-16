/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.kafka.config.model.Scope;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.Identity;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaRollerTest {

    private static final int REPLICAS = 5;

    private List<String> restarted;

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
    public void testRollWithNoController() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(REPLICAS, 0, 0), -1);
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void testRollWithPod2AsActiveController() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(0, 0, REPLICAS), 2);
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, 2));
    }

    @Test
    public void testRollWithControllerAndBrokers() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(3, 0, 3), 3);
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4, 5),
                asList(4, 5, 3, 0, 1, 2));
    }

    @Test
    public void pod0NotReadyAfterRollingBroker() {
        PodOperator podOps = mockPodOps(podId ->
                podId == 0 ? CompletableFuture.failedFuture(new TimeoutException("Timeout")) : CompletableFuture.completedFuture(null)
        );
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(REPLICAS, 0, 0), -1);
        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-0 to become ready",
                singletonList(0));
    }

    @Test
    public void pod1NotReadyAfterRolling() {
        PodOperator podOps = mockPodOps(podId ->
                podId == 1 ? CompletableFuture.failedFuture(new TimeoutException("Timeout")) : CompletableFuture.completedFuture(null)
        );
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(REPLICAS, 0, 0), -1);
        // On the first reconciliation we expect it to abort when it gets to pod 1
        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-1 to become ready",
                asList(1));
        // On the next reconciliation only pod 2 (controller) would need rolling, and we expect it to fail in the same way
        kafkaRoller = rollerWithActiveController(podOps, addPodNames(REPLICAS, 0, 0), -1);
        clearRestarted();
        doFailingRollingRestart(kafkaRoller,
                asList(2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-1 to become ready",
                emptyList());
    }

    @Test
    public void pod3NotReadyAfterRolling() {
        PodOperator podOps = mockPodOps(podId ->
                podId == 3 ? CompletableFuture.failedFuture(new TimeoutException("Timeout")) : CompletableFuture.completedFuture(null)
        );
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(REPLICAS, 0, 0), -1);
        // On the first reconciliation we expect it to abort when it gets to pod 3
        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-3 to become ready",
                asList(3));
        // On the next reconciliation, we expect it to fail in the same way
        kafkaRoller = rollerWithActiveController(podOps, addPodNames(REPLICAS, 0, 0), -1);
        clearRestarted();
        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-3 to become ready",
                emptyList());
    }

    @Test
    public void activeControllerNotReadyAfterRolling() {
        PodOperator podOps = mockPodOps(podId ->
                podId == 2 ? CompletableFuture.failedFuture(new TimeoutException("Timeout")) : CompletableFuture.completedFuture(null)
        );
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(0, 0, REPLICAS), 2);
        // On the first reconciliation we expect it to fail when rolling the controller (i.e. as rolling all the rest)
        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-2 to become ready",
                asList(0, 1, 3, 4, 2));
        // On the next reconciliation only pod 2 would need rolling, and we expect it to fail in the same way
        kafkaRoller = rollerWithActiveController(podOps, addPodNames(0, REPLICAS, 0), 2);
        clearRestarted();
        doFailingRollingRestart(kafkaRoller,
                singletonList(2),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-2 to become ready",
                singletonList(2));
    }

    @Test
    public void activeControllerNotReadyAfterRollingCombined() {
        PodOperator podOps = mockPodOps(podId ->
                podId == 2 ? CompletableFuture.failedFuture(new TimeoutException("Timeout")) : CompletableFuture.completedFuture(null)
        );
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(0, REPLICAS, 0), 2);
        // On the first reconciliation we expect it to fail when rolling the controller (i.e. as rolling all the rest)
        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-2 to become ready",
                asList(0, 1, 3, 4, 2));
        // On the next reconciliation only pod 2 would need rolling, and we expect it to fail in the same way
        kafkaRoller = rollerWithActiveController(podOps, addPodNames(0, REPLICAS, 0), 2);
        clearRestarted();
        doFailingRollingRestart(kafkaRoller,
                singletonList(2),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-2 to become ready",
                singletonList(2));
    }

    public Set<NodeRef> addPodNames(int brokerReplicas, int combinedReplicas, int controllerReplicas) {
        Set<NodeRef> podNames = new LinkedHashSet<>(brokerReplicas);
        for (int brokerId = 0; brokerId < brokerReplicas; brokerId++) {
            podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), brokerId), brokerId, "broker", false, true));
        }
        int maxCombinedId = brokerReplicas + combinedReplicas;
        for (int controllerId = brokerReplicas; controllerId < maxCombinedId; controllerId++) {
            podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), controllerId), controllerId, "combined", true, true));
        }
        int maxControllerId = maxCombinedId + controllerReplicas;
        for (int controllerId = maxCombinedId; controllerId < maxControllerId; controllerId++) {
            podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), controllerId), controllerId, "controller", true, false));
        }
        return podNames;
    }

    public Set<NodeRef> addDisconnectedPodNames(int replicas) {
        Set<NodeRef> podNames = new LinkedHashSet<>(replicas);

        podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), 10), 10, null, true, true));
        podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), 200), 200, null, true, true));
        podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), 30), 30, null, true, true));
        podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), 400), 400, null, true, true));
        podNames.add(new NodeRef(KafkaResources.kafkaPodName(clusterName(), 500), 500, null, true, true));
        return podNames;
    }

    @Test
    public void testRollWithDisconnectedBrokerPodNames() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addDisconnectedPodNames(REPLICAS), podOps,
                bootstrapBrokers -> bootstrapBrokers != null && bootstrapBrokers.stream().map(NodeRef::nodeId).toList().equals(singletonList(1)) ? new RuntimeException("Test Exception") : null,
                null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 30);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we never find the controller we get ascending order
        doSuccessfulRollingRestart(kafkaRoller,
                asList(10, 200, 30, 400, 500),
                asList(10, 200, 400, 500, 30));
    }


    @Test
    public void testRollHandlesErrorWhenOpeningBrokerAdminClient() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                bootstrapBrokers -> bootstrapBrokers != null && bootstrapBrokers.stream().map(NodeRef::nodeId).toList().equals(singletonList(1)) ? new RuntimeException("Test Exception") : null,
                null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we force restart the node if we can't create an admin client
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void testRollHandlesErrorWhenOpeningCombinedAdminClient() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, REPLICAS, 0), podOps,
                bootstrapBrokers -> bootstrapBrokers != null && bootstrapBrokers.stream().map(NodeRef::nodeId).toList().equals(singletonList(1)) ? new RuntimeException("Test Exception") : null,
                null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we force restart the node if we can't create an admin client
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void testHandlesErrorWhenOpenControllerAdminClient() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS), podOps,
                bootstrapBrokers -> bootstrapBrokers != null && bootstrapBrokers.stream().map(NodeRef::nodeId).toList().equals(singletonList(1)) ? new RuntimeException("Test Exception") : null,
                null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 4);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we force restart the node if we can't create an admin client
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void testRollHandlesErrorWhenGettingActiveControllerFromController() {
        int activeController = 2;
        int nonActiveController = 1;
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS), podOps,
                noException(), null,
                podId -> podId == nonActiveController ? new RuntimeException("Test Exception") : null, noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, activeController);
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 3, 4, 1, 2));
    }

    @Test
    public void testRollHandlesErrorWhenGettingActiveControllerFromActiveController() {
        int activeController = 2;
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS), podOps,
                noException(), null,
                podId -> podId == activeController ? new RuntimeException("Test Exception") : null, noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, activeController);
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, 2));
    }

    @Test
    public void testRollHandlesErrorWhenClosingBrokerAdminClient() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(),
                new RuntimeException("Test Exception"), noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should carry on rolling the pods (errors are logged)
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void testRollHandlesErrorWhenClosingControllerAdminClient() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS), podOps,
                noException(),
                new RuntimeException("Test Exception"), noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 3);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we did the active controller last order
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 4, 3));
    }

    @Test
    public void testBrokerNotInitiallyRollable() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        AtomicInteger count = new AtomicInteger(3);
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(), null, noException(), noException(),
                brokerId ->
                        brokerId == 1 ? CompletableFuture.completedFuture(count.getAndDecrement() == 0)
                                : CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 2, 3, 4, 1));
    }

    @Test
    public void testCombinedNotInitiallyRollable() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        // we expect canRoll to be called twice for each combined node
        AtomicInteger count = new AtomicInteger(5);
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, REPLICAS, 0), podOps,
                noException(), null, noException(), noException(),
                combinedId ->
                        combinedId == 1 ? CompletableFuture.completedFuture(count.getAndDecrement() < 0) : CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 2);
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 3, 4, 1, 2));
    }

    @Test
    public void testControllerNotInitiallyRollable() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        AtomicInteger count = new AtomicInteger(3);
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS), podOps,
                noException(), null, noException(), noException(),
                controllerId ->
                        (controllerId == 1) ? CompletableFuture.completedFuture(count.getAndDecrement() == 0) : CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 2);
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 3, 4, 1, 2));
    }

    @Test
    public void testBrokerNeverRollable() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(), null, noException(), noException(),
                brokerId ->
                        brokerId == 1 ? CompletableFuture.completedFuture(false)
                                : CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-1 cannot be updated right now.",
                // broker 1 never restarted
                asList(0, 2, 3, 4));
        kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(brokerId != 1),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        clearRestarted();
        doFailingRollingRestart(kafkaRoller,
                singletonList(1),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-1 cannot be updated right now.",
                // broker 1 never restarted
                emptyList());
    }

    @Test
    public void testCombinedNeverRollable() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, REPLICAS, 0), podOps,
                noException(), null, noException(), noException(),
                brokerId ->
                        brokerId == 1 ? CompletableFuture.completedFuture(false)
                                : CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-1 cannot be updated right now.",
                // broker 1 never restarted
                asList(0, 2, 3, 4));

        kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(brokerId != 1),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        clearRestarted();
        doFailingRollingRestart(kafkaRoller,
                singletonList(1),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-1 cannot be updated right now.",
                // broker 1 never restarted
                emptyList());
    }

    @Test
    public void testControllerNeverRollable() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS),
                podOps,
                noException(), null, noException(), noException(),
                brokerId ->
                        brokerId == 2 ? CompletableFuture.completedFuture(false)
                                : CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 2);
        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-2 cannot be updated right now.",
                // We expect all non-active controller pods to be rolled
                asList(0, 1, 3, 4));
        clearRestarted();
        kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0),
                podOps,
                noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(brokerId != 2),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 2);
        doFailingRollingRestart(kafkaRoller,
                singletonList(2),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-2 cannot be updated right now.",
                emptyList());
    }

    @Test
    public void testRollHandlesErrorWhenGettingBrokerConfig() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(), null,
                noException(), podId -> podId == 1 ? new KafkaRoller.ForceableProblem("could not get config exception") : null,
                brokerId -> CompletableFuture.completedFuture(true), new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should carry on rolling the pods
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 2, 3, 4, 1));
    }

    @Test
    public void testRollHandlesErrorWhenGettingCombinedConfig() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, REPLICAS, 0), podOps,
                noException(), null,
                noException(), podId -> podId == 1 ? new KafkaRoller.ForceableProblem("could not get config exception") : null,
                brokerId -> CompletableFuture.completedFuture(true), new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should carry on rolling the pods
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 2, 3, 4, 1));
    }

    @Test
    public void testRollHandlesErrorWhenGettingControllerConfig() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS), podOps,
                noException(), null,
                noException(), podId -> podId == 1 ? new KafkaRoller.ForceableProblem("could not get config exception") : null,
                brokerId -> CompletableFuture.completedFuture(true), new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should carry on rolling the pods
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 2, 3, 4, 1));
    }

    @Test
    public void testRollHandlesErrorWhenGettingConfigFromController() {
        int controller = 2;
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS), podOps,
                noException(), null,
                noException(), podId -> podId == controller ? new KafkaRoller.ForceableProblem("could not get config exception") : null,
                brokerId -> CompletableFuture.completedFuture(true), new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, controller);
        // The algorithm should carry on rolling the pods
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, controller));
    }


    @Test
    public void testSuccessfulAlteringConfigNotRoll() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(3, 3, 3), podOps,
                noException(), null,
                noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true), new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should not carry on rolling the pods
        doSuccessfulConfigUpdate(kafkaRoller,
                emptyList());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDynamicUpdateConfigsByScope() throws KafkaRoller.ForceableProblem, InterruptedException {
        Admin admin = mock(Admin.class);
        AlterConfigsResult alterResult = mock(AlterConfigsResult.class);
        when(alterResult.values()).thenReturn(Collections.emptyMap());
        when(admin.incrementalAlterConfigs(any(Map.class))).thenReturn(alterResult);
        NodeRef nodeRef = new NodeRef("broker-0", 0, "broker", false, true);

        KafkaConfigurationDiff diff = mock(KafkaConfigurationDiff.class);
        when(diff.getConfigDiff(Scope.PER_BROKER)).thenReturn(List.of(
                new AlterConfigOp(new ConfigEntry("num.io.threads", "12"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry("num.network.threads", null), AlterConfigOp.OpType.DELETE)));
        when(diff.getConfigDiff(Scope.CLUSTER_WIDE)).thenReturn(List.of(
                new AlterConfigOp(new ConfigEntry("compression.type", "lz4"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry("log.retention.bytes", null), AlterConfigOp.OpType.DELETE)));

        KafkaRoller.dynamicUpdateKafkaConfig(Reconciliation.DUMMY_RECONCILIATION, nodeRef, admin, diff);

        ArgumentCaptor<Map<ConfigResource, Collection<AlterConfigOp>>> captor = ArgumentCaptor.forClass(Map.class);
        verify(admin, times(2)).incrementalAlterConfigs(captor.capture());

        List<Map<ConfigResource, Collection<AlterConfigOp>>> allCalls = captor.getAllValues();
        ConfigResource perBrokerResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        ConfigResource clusterWideResource = new ConfigResource(ConfigResource.Type.BROKER, "");

        Map<ConfigResource, Collection<AlterConfigOp>> perBrokerCall = allCalls.stream()
                .filter(m -> m.containsKey(perBrokerResource)).findFirst().orElseThrow();
        Map<ConfigResource, Collection<AlterConfigOp>> clusterWideCall = allCalls.stream()
                .filter(m -> m.containsKey(clusterWideResource)).findFirst().orElseThrow();

        assertThat(perBrokerCall.get(perBrokerResource).size(), is(2));
        assertThat(clusterWideCall.get(clusterWideResource).size(), is(2));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDynamicUpdateClusterWideConfigSetPerBroker() throws KafkaRoller.ForceableProblem, InterruptedException {
        Admin admin = mock(Admin.class);
        AlterConfigsResult alterResult = mock(AlterConfigsResult.class);
        when(alterResult.values()).thenReturn(Collections.emptyMap());
        when(admin.incrementalAlterConfigs(any(Map.class))).thenReturn(alterResult);
        NodeRef nodeRef = new NodeRef("broker-0", 0, "broker", false, true);

        // Two cluster-wide configs were manually set per-broker (DYNAMIC_BROKER_CONFIG):
        // - compression.type is not in the CR -> DELETE goes to per-broker only
        // - log.retention.bytes is in the CR with a different value -> SET goes to cluster-wide, DELETE goes to per-broker
        KafkaConfigurationDiff diff = mock(KafkaConfigurationDiff.class);
        when(diff.getConfigDiff(Scope.PER_BROKER)).thenReturn(emptyList());
        when(diff.getConfigDiff(Scope.CLUSTER_WIDE)).thenReturn(List.of(
                new AlterConfigOp(new ConfigEntry("compression.type", null, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
                        false, false, emptyList(), ConfigEntry.ConfigType.UNKNOWN, null), AlterConfigOp.OpType.DELETE),
                new AlterConfigOp(new ConfigEntry("log.retention.bytes", "1000000", ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
                        false, false, emptyList(), ConfigEntry.ConfigType.UNKNOWN, null), AlterConfigOp.OpType.SET)));

        KafkaRoller.dynamicUpdateKafkaConfig(Reconciliation.DUMMY_RECONCILIATION, nodeRef, admin, diff);

        ArgumentCaptor<Map<ConfigResource, Collection<AlterConfigOp>>> captor = ArgumentCaptor.forClass(Map.class);
        verify(admin, times(2)).incrementalAlterConfigs(captor.capture());

        List<Map<ConfigResource, Collection<AlterConfigOp>>> allCalls = captor.getAllValues();
        ConfigResource perBrokerResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        ConfigResource clusterWideResource = new ConfigResource(ConfigResource.Type.BROKER, "");

        Map<ConfigResource, Collection<AlterConfigOp>> perBrokerCall = allCalls.stream()
                .filter(m -> m.containsKey(perBrokerResource)).findFirst().orElseThrow();
        Map<ConfigResource, Collection<AlterConfigOp>> clusterWideCall = allCalls.stream()
                .filter(m -> m.containsKey(clusterWideResource)).findFirst().orElseThrow();

        // Per-broker resource should get DELETEs for both configs
        List<AlterConfigOp> perBrokerOps = new ArrayList<>(perBrokerCall.get(perBrokerResource));
        assertThat(perBrokerOps.size(), is(2));
        assertThat(perBrokerOps.stream().allMatch(op -> op.opType() == AlterConfigOp.OpType.DELETE), is(true));

        // Cluster-wide resource should get only the SET for log.retention.bytes
        List<AlterConfigOp> clusterWideOps = new ArrayList<>(clusterWideCall.get(clusterWideResource));
        assertThat(clusterWideOps.size(), is(1));
        assertThat(clusterWideOps.getFirst().configEntry().name(), is("log.retention.bytes"));
        assertThat(clusterWideOps.getFirst().configEntry().value(), is("1000000"));
        assertThat(clusterWideOps.getFirst().opType(), is(AlterConfigOp.OpType.SET));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDynamicUpdateThrowsForceableProblemOnFailure() {
        ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        KafkaFutureImpl<Void> failedFuture = new KafkaFutureImpl<>();
        failedFuture.completeExceptionally(new RuntimeException("Config update failed"));

        AlterConfigsResult alterResult = mock(AlterConfigsResult.class);
        when(alterResult.values()).thenReturn(Map.of(brokerResource, failedFuture));

        Admin admin = mock(Admin.class);
        when(admin.incrementalAlterConfigs(any(Map.class))).thenReturn(alterResult);

        NodeRef nodeRef = new NodeRef("broker-0", 0, "broker", false, true);
        KafkaConfigurationDiff diff = mock(KafkaConfigurationDiff.class);
        when(diff.getConfigDiff(Scope.PER_BROKER)).thenReturn(List.of(
                new AlterConfigOp(new ConfigEntry("num.io.threads", "12"), AlterConfigOp.OpType.SET)));
        when(diff.getConfigDiff(Scope.CLUSTER_WIDE)).thenReturn(emptyList());

        KafkaRoller.ForceableProblem ex = assertThrows(KafkaRoller.ForceableProblem.class,
                () -> KafkaRoller.dynamicUpdateKafkaConfig(Reconciliation.DUMMY_RECONCILIATION, nodeRef, admin, diff));
        assertThat(ex.getMessage(), containsString("Error updating Kafka configuration for pod"));
    }

    @Test
    public void testSuccessfulRollingControllers() {
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 3, 3),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true), new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4, 5),
                asList(0, 1, 2, 3, 4, 5));
    }

    @Test
    public void testPostRestartDelayIsAppliedBeforeEachRestart() {
        long delayMs = 200;
        long delayNs = TimeUnit.MILLISECONDS.toNanos(delayMs);
        Map<Integer, List<Long>> canRollCheckedAtNs = new ConcurrentHashMap<>();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(3, 0, 0),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(),
                brokerId -> {
                    canRollCheckedAtNs.computeIfAbsent(brokerId, k -> new ArrayList<>()).add(System.nanoTime());
                    return CompletableFuture.completedFuture(true);
                },
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1, delayMs);

        long startedAtNs = System.nanoTime();
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2),
                asList(0, 1, 2));
        long finishedAtNs = System.nanoTime();

        // The check is done when the pod could be restarted and again at the end of the delay, before the restart
        for (int brokerId = 0; brokerId < 3; brokerId++) {
            List<Long> checks = canRollCheckedAtNs.get(brokerId);
            assertThat("Pod " + brokerId + " should be checked twice", checks.size(), is(2));
            assertThat(checks.get(1) - checks.get(0), greaterThanOrEqualTo(delayNs));
            assertThat(kafkaRoller.restartedAtNs.get(brokerId), greaterThanOrEqualTo(checks.get(1)));
        }

        // The first restart waits for the delay as well, each restart is at least a delay after the previous one and
        // there is no wait after the last restart
        assertThat(kafkaRoller.restartedAtNs.get(0) - startedAtNs, greaterThanOrEqualTo(delayNs));
        assertThat(kafkaRoller.restartedAtNs.get(1) - kafkaRoller.restartedAtNs.get(0), greaterThanOrEqualTo(delayNs));
        assertThat(kafkaRoller.restartedAtNs.get(2) - kafkaRoller.restartedAtNs.get(1), greaterThanOrEqualTo(delayNs));
        assertThat(finishedAtNs - kafkaRoller.restartedAtNs.get(2), lessThan(delayNs));
    }

    @Test
    public void testPostRestartDelayStartsAgainWhenCheckFailsAfterDelay() {
        long delayMs = 200;
        long delayNs = TimeUnit.MILLISECONDS.toNanos(delayMs);
        // The check passes, fails at the end of the delay, and passes at both ends of the next delay
        List<Boolean> canRollResults = List.of(true, false, true, true);
        AtomicInteger canRollCalls = new AtomicInteger();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(1, 0, 0),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(canRollResults.get(canRollCalls.getAndIncrement())),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1, delayMs);

        long startedAtNs = System.nanoTime();
        doSuccessfulRollingRestart(kafkaRoller,
                singletonList(0),
                singletonList(0));

        assertThat(canRollCalls.get(), is(4));
        // Two full delays were waited for, and the failed check was retried after the regular back-off, not after another delay
        assertThat(kafkaRoller.restartedAtNs.get(0) - startedAtNs, greaterThanOrEqualTo(2 * delayNs));
        assertThat(kafkaRoller.restartedAtNs.get(0) - startedAtNs, lessThan(3 * delayNs));
    }

    @Test
    public void testPostRestartDelayStartsAgainWhenRestartFailsAfterDelay() {
        long delayMs = 200;
        long delayNs = TimeUnit.MILLISECONDS.toNanos(delayMs);
        AtomicInteger canRollCalls = new AtomicInteger();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(1, 0, 0),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(),
                brokerId -> {
                    canRollCalls.incrementAndGet();
                    return CompletableFuture.completedFuture(true);
                },
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1, delayMs);
        // The first restart attempt after the delay fails
        kafkaRoller.restartFailures.put(0, 1);

        long startedAtNs = System.nanoTime();
        doSuccessfulRollingRestart(kafkaRoller,
                singletonList(0),
                singletonList(0));

        // The pod is checked at both ends of each of the two delays, and the retried restart waits for a new delay
        assertThat(kafkaRoller.restartAttempts.get(0), is(2));
        assertThat(canRollCalls.get(), is(4));
        assertThat(kafkaRoller.restartedAtNs.get(0) - startedAtNs, greaterThanOrEqualTo(2 * delayNs));
        assertThat(kafkaRoller.restartedAtNs.get(0) - startedAtNs, lessThan(3 * delayNs));
    }

    @Test
    public void testWaitingForPostRestartDelayOfOtherPodsDoesNotUseRetries() {
        long delayMs = 100;
        // Broker 1 can be rolled only once the three other brokers have been restarted. Each of them is restarted
        // after its own delay, so broker 1 is checked (and fails) at most once per delay. The test backoff allows only
        // three failures, and the retries alone (10, 20 and 40ms) would all be used up before the first restart: the
        // rolling update succeeds only if waiting for the delays of the other brokers does not use up the retries.
        Map<Integer, Integer> canRollCalls = new ConcurrentHashMap<>();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(4, 0, 0),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(),
                brokerId -> {
                    canRollCalls.merge(brokerId, 1, Integer::sum);
                    return CompletableFuture.completedFuture(brokerId != 1 || restarted().containsAll(List.of(0, 2, 3)));
                },
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1, delayMs);

        long startNanos = System.nanoTime();
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3),
                asList(0, 2, 3, 1));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        // Each restart waits for its own delay and only one pod waits at a time
        assertThat("The restarts should be serialized through the delay", elapsedMs, greaterThanOrEqualTo(4 * delayMs));
        // Broker 1 is checked at most once per delay of the other brokers, then at both ends of its own delay
        assertThat(canRollCalls.get(1), both(greaterThanOrEqualTo(2)).and(lessThanOrEqualTo(5)));
        for (int brokerId : List.of(0, 2, 3)) {
            assertThat("Pod " + brokerId + " should be checked twice", canRollCalls.get(brokerId), is(2));
        }
    }

    @Test
    public void testBrokerNeverRollableWithPostRestartDelay() {
        long delayMs = 100;
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(brokerId != 1),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1, delayMs);
        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-1 cannot be updated right now.",
                // broker 1 never restarted, the other brokers are restarted after their delays
                asList(0, 2, 3, 4));
    }

    @Test
    public void testRollingUpdateFailsAfterRetriesWhenCheckKeepsFailingAfterPostRestartDelay() {
        long delayMs = 100;
        // The check passes whenever the pod is considered after a retry, and fails at the end of each delay
        AtomicInteger canRollCalls = new AtomicInteger();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(1, 0, 0),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(canRollCalls.incrementAndGet() % 2 == 1),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1, delayMs);

        long startNanos = System.nanoTime();
        doFailingRollingRestart(kafkaRoller,
                singletonList(0),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-0 cannot be updated right now.",
                emptyList());

        // Each failed check at the end of a delay uses up one of the 4 attempts of the test backoff
        assertThat(canRollCalls.get(), is(8));
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos), greaterThanOrEqualTo(4 * delayMs));
    }

    @Test
    public void testPostRestartDelayIsReleasedWhenTheWaitingPodDisappears() {
        long delayMs = 100;
        Map<Integer, Integer> canRollCalls = new ConcurrentHashMap<>();
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        // Pod 0 is deleted once it has been found safe to restart and waits for the delay
        when(podOps.get(any(), eq(clusterName() + "-kafka-0"))).thenAnswer(invocation -> canRollCalls.containsKey(0) ? null
                : new PodBuilder().withNewMetadata().withNamespace(invocation.getArgument(0)).withName(invocation.getArgument(1)).endMetadata().build());
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(2, 0, 0), podOps, noException(), null, noException(), noException(),
                brokerId -> {
                    canRollCalls.merge(brokerId, 1, Integer::sum);
                    return CompletableFuture.completedFuture(true);
                },
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1, delayMs);

        long startNanos = System.nanoTime();
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1),
                singletonList(1));

        // Pod 1 waits for the delay of pod 0 and then for its own delay
        assertThat(canRollCalls, is(Map.of(0, 1, 1, 2)));
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos), greaterThanOrEqualTo(2 * delayMs));
    }

    @Test
    public void testActiveControllerIsNotForceRolledWhileWaitingForPostRestartDelayOfOtherPods() {
        // The delay is longer than all the retries of the active controller together (10 + 20 + 40 ms)
        long delayMs = 100;
        int activeController = 2;
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 3, 0),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, activeController, delayMs);

        // The active controller only needs to be reconfigured. It waits for the delays of the other pods without using
        // up its retries, so it is reconfigured at the end instead of being force-rolled
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1),
                asList(0, 1));
    }

    @Test
    public void testPostRestartDelayIsNotAppliedWhenPodsAreReconfiguredWithoutRestart() {
        long delayMs = 10_000;
        Set<Integer> canRollChecked = ConcurrentHashMap.newKeySet();
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(3, 0, 0),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(),
                brokerId -> {
                    canRollChecked.add(brokerId);
                    return CompletableFuture.completedFuture(true);
                },
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1, delayMs);

        long startedAtNs = System.nanoTime();
        doSuccessfulRollingRestart(kafkaRoller,
                emptyList(),
                emptyList());

        assertThat(canRollChecked, is(Set.of(0, 1, 2)));
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNs), lessThan(delayMs));
    }

    @Test
    public void testPostRestartDelayIsAppliedWhenDynamicUpdateFails() {
        long delayMs = 200;
        long delayNs = TimeUnit.MILLISECONDS.toNanos(delayMs);
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(3, 0, 0),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1, delayMs);
        kafkaRoller.dynamicUpdateFailsFor.add(1);

        long startedAtNs = System.nanoTime();
        doSuccessfulRollingRestart(kafkaRoller,
                emptyList(),
                // Only broker 1 falls back to a restart, after the delay
                singletonList(1));
        long finishedAtNs = System.nanoTime();

        assertThat(kafkaRoller.restartedAtNs.get(1) - startedAtNs, greaterThanOrEqualTo(delayNs));
        assertThat(finishedAtNs - startedAtNs, lessThan(2 * delayNs));
        // The failed dynamic update is not tried again at the end of the delay, the other brokers are updated once
        assertThat(kafkaRoller.dynamicUpdateAttempts, is(Map.of(0, 1, 1, 1, 2, 1)));
    }

    @Test
    public void testPostRestartDelayIsNotAppliedToUnresponsivePods() {
        long delayMs = 10_000;
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)),
                bootstrapBrokers -> new RuntimeException("Test Exception"),
                null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1, delayMs);

        long startedAtNs = System.nanoTime();
        // The pods are force-restarted because the admin client cannot be created, without waiting for the delay
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 2, 3, 4));

        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNs), lessThan(delayMs));
    }

    @Test
    public void testPostRestartDelayIsNotAppliedToStuckPods() {
        long delayMs = 10_000;
        PodOperator podOps = mockPodOpsWithStuckPod(podId -> CompletableFuture.completedFuture(null), "CrashLoopBackOff");
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1, delayMs);

        long startedAtNs = System.nanoTime();
        kafkaRoller.rollingRestart(pod -> RestartReasons.of(RestartReason.POD_HAS_OLD_REVISION))
                .whenComplete((v, err) -> {
                    assertThat(restarted(), is(asList(0, 1, 2, 3, 4)));
                    assertNoUnclosedAdminClient(kafkaRoller);
                }).toCompletableFuture().join();

        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNs), lessThan(delayMs));
    }

    @Test
    public void testPostRestartDelayIsNotAppliedWhenPodIsForceRolledAfterRetries() {
        long delayMs = 10_000;
        int activeController = 2;
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, 3),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)),
                noException(), null,
                podId -> podId == activeController ? new RuntimeException("Test Exception") : null, noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, activeController, delayMs);

        long startedAtNs = System.nanoTime();
        // Only the active controller is rolled: the error determining the active controller uses up its retries and
        // it is force-rolled without waiting for the delay
        doSuccessfulRollingRestart(kafkaRoller,
                singletonList(activeController),
                singletonList(activeController));

        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNs), lessThan(delayMs));
    }

    @Test
    public void testControllerNoQuorum() {
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, 3),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(false), new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-0 cannot be updated right now.",
                asList());
    }

    @Test
    public void testControllerAndOneMoreNeverRollable() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, REPLICAS),
                podOps,
                noException(), null, noException(), noException(),
                brokerId -> brokerId == 2 || brokerId == 3 ? CompletableFuture.completedFuture(false) : CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 2);
        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.ForceableProblem.class, "Pod c-kafka-2 is the active controller and there are other pods to verify first",
                // We expect all non-controller pods to be rolled
                asList(0, 1, 4));
    }

    @Test
    public void testBrokerInRecoveryState() {
        PodOperator podOps = mockPodOps(podId ->
                (podId == 0) ? CompletableFuture.failedFuture(new TimeoutException("Timeout")) : CompletableFuture.completedFuture(null)
        );

        Map<String, Object> recoveryState = new HashMap<>();
        recoveryState.put("remainingLogsToRecover", 10);
        recoveryState.put("remainingSegmentsToRecover", 100);
        BrokerState brokerstate = new BrokerState(2, recoveryState);

        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(3, 0, 3),
                podOps,
                noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                null, null, false, brokerstate, 3);

        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4, 5),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-0 is not ready because the Kafka node is performing log recovery. There are 10 logs and 100 segments left to recover.",
                asList(4, 5, 3, 1, 2));
    }

    @Test
    public void testControllerInRecoveryState() {
        PodOperator podOps = mockPodOps(podId ->
                (podId == 0) ? CompletableFuture.failedFuture(new TimeoutException("Timeout")) : CompletableFuture.completedFuture(null)
        );

        Map<String, Object> recoveryState = new HashMap<>();
        recoveryState.put("remainingLogsToRecover", 10);
        recoveryState.put("remainingSegmentsToRecover", 100);
        BrokerState brokerstate = new BrokerState(2, recoveryState);

        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, 3),
                podOps,
                noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                null, null, false, brokerstate, -1);

        doFailingRollingRestart(kafkaRoller,
                List.of(0, 1, 2),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-0 is not ready because the Kafka node is performing log recovery. There are 10 logs and 100 segments left to recover.",
                List.of(1, 2));
    }

    @Test
    public void testCombinedInRecoveryState() {
        PodOperator podOps = mockPodOps(podId ->
                (podId == 4) ? CompletableFuture.failedFuture(new TimeoutException("Timeout")) : CompletableFuture.completedFuture(null)
        );

        Map<String, Object> recoveryState = new HashMap<>();
        recoveryState.put("remainingLogsToRecover", 10);
        recoveryState.put("remainingSegmentsToRecover", 100);
        BrokerState brokerstate = new BrokerState(2, recoveryState);

        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(3, 3, 0),
                podOps,
                noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                null, null, false, brokerstate, 3);

        // It rolls brokers if only successfully rolled controllers
        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4, 5),
                KafkaRoller.UnforceableProblem.class, "Pod c-kafka-4 is not ready because the Kafka node is performing log recovery. There are 10 logs and 100 segments left to recover.",
                asList(5, 3));
    }

    @Test
    public void testBrokerInRunningState() {
        PodOperator podOps = mockPodOps(podId ->
                (podId == 0) ? CompletableFuture.failedFuture(new TimeoutException("Timeout")) : CompletableFuture.completedFuture(null)
        );

        BrokerState brokerstate = new BrokerState(3, null);

        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0),
                podOps,
                noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                null, null, false, brokerstate, -1);

        doFailingRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-0 to become ready",
                asList(0));
    }

    @Test
    public void testFailGettingBrokerState() {
        PodOperator podOps = mockPodOps(podId ->
                (podId == 0) ? CompletableFuture.failedFuture(new TimeoutException("Timeout")) : CompletableFuture.completedFuture(null)
        );

        BrokerState brokerstate = new BrokerState(-1, null);

        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0),
                podOps,
                noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                null, null, false, brokerstate, 1);

        doFailingRollingRestart(kafkaRoller,
                asList(0),
                KafkaRoller.FatalProblem.class, "Error while waiting for restarted pod c-kafka-0 to become ready",
                asList(0));
    }

    @Test
    public void testFailWhenPodIsReadyThrows() {
        PodOperator podOps = mockPodOps(podId ->
                (podId == 0) ? CompletableFuture.failedFuture(new KubernetesClientException("Failed")) : CompletableFuture.completedFuture(null)
        );

        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0),
                podOps,
                noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                null, null, false, null, 1);

        doFailingRollingRestart(kafkaRoller,
                List.of(),
                KubernetesClientException.class, "Failed",
                List.of());
    }

    @Test
    public void testRollWithAllRoles() {
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(3, 3, 3),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true), new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, 6);
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4, 5, 6, 7, 8),
                asList(3, 4, 5, 7, 8, 6, 0, 1, 2));
    }

    @Test
    public void testRollUnreadyPodFirstAllRoles() {
        AtomicBoolean pod1Unready = new AtomicBoolean(true);
        AtomicBoolean pod4Unready = new AtomicBoolean(true);
        AtomicBoolean pod7Unready = new AtomicBoolean(true);
        PodOperator podOps = mockPodOps(podId -> {
            //unready broker is 1
            if (podId == 1 && pod1Unready.getAndSet(false)) {
                return CompletableFuture.failedFuture(new TimeoutException("fail"));
            }
            //unready combined node is 4
            if (podId == 4 && pod4Unready.getAndSet(false)) {
                return CompletableFuture.failedFuture(new TimeoutException("fail"));
            }
            //unready controller is 7
            if (podId == 7 && pod7Unready.getAndSet(false)) {
                return CompletableFuture.failedFuture(new TimeoutException("fail"));
            }
            return CompletableFuture.completedFuture(null);
        });
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(3, 3, 3),
                podOps, noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true), new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4, 5, 6, 7, 8), // brokers, combined, controllers
                asList(7, 4, 3, 5, 6, 8, 1, 0, 2)); //Rolls in order: unready controllers, ready controllers, unready brokers, ready brokers
    }

    @ParameterizedTest(name = "testPodStuckWith-{0}")
    @MethodSource("stuckPodReasons")
    public void testPodStuckThrowsFatalProblem(String reason) {
        PodOperator podOps = mockPodOpsWithStuckPod(podId -> CompletableFuture.completedFuture(null), reason);
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(REPLICAS, 0, 0), -1);
        doFailingRollingRestart(kafkaRoller,
            asList(0, 1, 2, 3, 4),
            KafkaRoller.FatalProblem.class, "Pod is unschedulable or is not starting",
            emptyList());
    }

    @ParameterizedTest(name = "testPodStuckForceRestartWith-{0}")
    @MethodSource("stuckPodReasons")
    public void testPodStuckForceRestartWithOldRevision(String reason) {
        PodOperator podOps = mockPodOpsWithStuckPod(podId -> CompletableFuture.completedFuture(null), reason);
        TestingKafkaRoller kafkaRoller = rollerWithActiveController(podOps, addPodNames(REPLICAS, 0, 0), -1);
        kafkaRoller.rollingRestart(pod -> RestartReasons.of(RestartReason.POD_HAS_OLD_REVISION))
            .whenComplete((v, err) -> {
                assertThat(restarted(), is(asList(0, 1, 2, 3, 4)));
                assertNoUnclosedAdminClient(kafkaRoller);
            }).toCompletableFuture().join();
    }

    @Test
    public void testRestartTakesSnapshotOfRestartReasons() {
        PodOperator mockPodOps = mock(PodOperator.class);
        when(mockPodOps.restart(any(), any(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
        KubernetesRestartEventPublisher mockEventPublisher = mock(KubernetesRestartEventPublisher.class);

        String podName = ssName() + "-0";
        KafkaRoller kafkaRoller = new KafkaRoller(
                Reconciliation.DUMMY_RECONCILIATION,
                mockPodOps,
                1000,
                1000,
                0,
                () -> new BackOff(10L, 2, 4),
                Set.of(new NodeRef(podName, 0, "pool", false, true)),
                new Identity(null, null),
                new DefaultAdminClientProvider(),
                new DefaultKafkaAgentClientProvider(),
                id -> null,
                KafkaVersionTestUtils.getLatestVersion(),
                true,
                mockEventPublisher
        );

        Pod pod = new PodBuilder().withNewMetadata().withName(podName).withNamespace(stsNamespace()).endMetadata().build();
        KafkaRoller.RestartContext restartContext = new KafkaRoller.RestartContext(() -> new BackOff(10L, 2, 4));
        restartContext.restartReasons = RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);

        kafkaRoller.restart(pod, restartContext).toCompletableFuture().join();

        // Mutate restartContext.restartReasons immediately after restart was initiated
        restartContext.restartReasons.add(RestartReason.POD_STUCK);

        // Verify that the event publisher received only the snapshot (MANUAL_ROLLING_UPDATE) and not POD_STUCK
        ArgumentCaptor<RestartReasons> captor = ArgumentCaptor.forClass(RestartReasons.class);
        verify(mockEventPublisher, timeout(1000)).publishRestartEvents(any(), eq(pod), captor.capture());
        assertThat(captor.getValue().contains(RestartReason.MANUAL_ROLLING_UPDATE), is(true));
        assertThat(captor.getValue().contains(RestartReason.POD_STUCK), is(false));
    }

    private TestingKafkaRoller rollerWithActiveController(PodOperator podOps, Set<NodeRef> nodes, int activeController) {
        return new TestingKafkaRoller(nodes, podOps,
                noException(), null, noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, activeController);
    }

    private void doSuccessfulConfigUpdate(TestingKafkaRoller kafkaRoller,
                                          List<Integer> expected) {
        kafkaRoller.rollingRestart(pod -> RestartReasons.empty())
                .whenComplete((v, error) -> {
                    assertThat(restarted(), is(expected));
                    assertNoUnclosedAdminClient(kafkaRoller);
                }).toCompletableFuture().join();
    }

    private void doSuccessfulRollingRestart(TestingKafkaRoller kafkaRoller,
                                            Collection<Integer> podsToRestart,
                                            List<Integer> expected) {
        kafkaRoller
                .rollingRestart(pod -> {
                    if (podsToRestart.contains(podName2Number(pod.getMetadata().getName()))) {
                        return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
                    } else {
                        return RestartReasons.empty();
                    }
                })
                .whenComplete((v, err) -> {
                    assertThat(restarted(), is(expected));
                    assertNoUnclosedAdminClient(kafkaRoller);
                }).toCompletableFuture().join();
    }

    private void assertNoUnclosedAdminClient(TestingKafkaRoller kafkaRoller) {
        if (!kafkaRoller.unclosedAdminClients.isEmpty()) {
            Throwable alloc = kafkaRoller.unclosedAdminClients.values().iterator().next();
            alloc.printStackTrace(System.out);
            throw new RuntimeException(kafkaRoller.unclosedAdminClients.size() + " unclosed AdminClient instances");
        }
    }

    private void doFailingRollingRestart(TestingKafkaRoller kafkaRoller,
                                         Collection<Integer> podsToRestart,
                                         Class<? extends Throwable> exception, String message,
                                         List<Integer> expectedRestart) {
        try {
            kafkaRoller
                .rollingRestart(pod -> {
                    if (podsToRestart.contains(podName2Number(pod.getMetadata().getName()))) {
                        return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
                    } else {
                        return RestartReasons.empty();
                    }
                }).toCompletableFuture().join();
            throw new AssertionError("Expected exception " + exception.getName() + " but none was thrown");
        } catch (Exception e) {
            assertThat(e, instanceOf(CompletionException.class));
            assertThat(e.getCause().getClass() + " is not a subclass of " + exception.getName(), e.getCause(), instanceOf(exception));
            assertThat("The exception message was not as expected", e.getCause().getMessage(), is(message));
            assertThat("The restarted pods were not as expected", restarted(), is(expectedRestart));
            assertNoUnclosedAdminClient(kafkaRoller);
        }
    }

    public List<Integer> restarted() {
        return restarted.stream().map(KafkaRollerTest::podName2Number).collect(Collectors.toList());
    }

    @BeforeEach
    public void clearRestarted() {
        restarted = new ArrayList<>();
    }

    private PodOperator mockPodOps(Function<Integer, CompletableFuture<Void>> readiness) {
        PodOperator podOps = mock(PodOperator.class);
        when(podOps.get(any(), any())).thenAnswer(
                invocation -> new PodBuilder()
                        .withNewMetadata()
                        .withNamespace(invocation.getArgument(0))
                        .withName(invocation.getArgument(1))
                        .endMetadata()
                        .build()
        );

        when(podOps.readiness(any(), any(), any(), anyLong(), anyLong())).thenAnswer(invocationOnMock -> {
            String podName = invocationOnMock.getArgument(2);
            return readiness.apply(podName2Number(podName));
        });

        when(podOps.isReady(anyString(), anyString())).thenAnswer(invocationOnMock -> {
            String podName = invocationOnMock.getArgument(1);
            try {
                readiness.apply(podName2Number(podName)).join();
                return true;
            } catch (Exception e) {
                if (e instanceof CompletionException && e.getCause() instanceof TimeoutException) {
                    return false;
                } else {
                    throw e.getCause();
                }
            }
        });
        return podOps;
    }

    private PodOperator mockPodOpsWithStuckPod(Function<Integer, CompletableFuture<Void>> readiness, String reason) {
        PodOperator podOps = mockPodOps(readiness);

        when(podOps.get(any(), any())).thenAnswer(invocation ->
            new PodBuilder()
                .withNewMetadata()
                    .withNamespace(invocation.getArgument(0))
                    .withName(invocation.getArgument(1))
                .endMetadata()
                .withNewStatus()
                    .withContainerStatuses(
                        new ContainerStatusBuilder()
                            .withName("kafka")
                            .withNewState()
                                .withNewWaiting()
                                    .withReason(reason)
                                .endWaiting()
                            .endState()
                            .build())
                .endStatus()
            .build()
        );
        return podOps;
    }

    private static Stream<String> stuckPodReasons() {
        return Stream.of("CrashLoopBackOff", "ImagePullBackOff", "ContainerCreating", "InvalidImageName");
    }

    private class TestingKafkaRoller extends KafkaRoller {

        int controllerCall;
        private final Map<Integer, Long> restartedAtNs = new ConcurrentHashMap<>();
        // Number of restart attempts of each pod which fail before the restart succeeds
        private final Map<Integer, Integer> restartFailures = new ConcurrentHashMap<>();
        private final Map<Integer, Integer> restartAttempts = new ConcurrentHashMap<>();
        private final Set<Integer> dynamicUpdateFailsFor = ConcurrentHashMap.newKeySet();
        private final Map<Integer, Integer> dynamicUpdateAttempts = new ConcurrentHashMap<>();
        private volatile int lastConfigNodeId = -1;
        private final IdentityHashMap<Admin, Throwable> unclosedAdminClients;
        private final Function<Set<NodeRef>, RuntimeException> acOpenException;
        private final Throwable acCloseException;
        private final Function<Integer, CompletableFuture<Boolean>> canRollFn;
        private final Function<Integer, Throwable> controllerException;
        private final Function<Integer, ForceableProblem> getConfigsException;
        private final boolean delegateAdminClientCall;
        private final int activeController;
        private final BrokerState brokerState;

        @SuppressWarnings("checkstyle:ParameterNumber")
        private TestingKafkaRoller(Set<NodeRef> nodes,
                                   PodOperator podOps,
                                   Function<Set<NodeRef>, RuntimeException> acOpenException,
                                   Throwable acCloseException,
                                   Function<Integer, Throwable> controllerException,
                                   Function<Integer, ForceableProblem> getConfigsException,
                                   Function<Integer, CompletableFuture<Boolean>> canRollFn,
                                   AdminClientProvider adminClientProvider,
                                   KafkaAgentClientProvider kafkaAgentClientProvider,
                                   boolean delegateAdminClientCall, BrokerState brokerState, int activeController) {
            this(nodes, podOps, acOpenException, acCloseException, controllerException, getConfigsException, canRollFn,
                    adminClientProvider, kafkaAgentClientProvider, delegateAdminClientCall, brokerState, activeController, 0);
        }

        @SuppressWarnings("checkstyle:ParameterNumber")
        private TestingKafkaRoller(Set<NodeRef> nodes,
                                   PodOperator podOps,
                                   Function<Set<NodeRef>, RuntimeException> acOpenException,
                                   Throwable acCloseException,
                                   Function<Integer, Throwable> controllerException,
                                   Function<Integer, ForceableProblem> getConfigsException,
                                   Function<Integer, CompletableFuture<Boolean>> canRollFn,
                                   AdminClientProvider adminClientProvider,
                                   KafkaAgentClientProvider kafkaAgentClientProvider,
                                   boolean delegateAdminClientCall, BrokerState brokerState, int activeController,
                                   long postRestartDelayMs) {
            super(
                    new Reconciliation("test", "Kafka", stsNamespace(), clusterName()),
                    podOps,
                    500,
                    1000,
                    postRestartDelayMs,
                    () -> new BackOff(10L, 2, 4),
                    nodes,
                    new Identity(null, null),
                    adminClientProvider,
                    kafkaAgentClientProvider,
                    brokerId -> "compression.type=gzip",
                    KafkaVersionTestUtils.getLatestVersion(),
                    true,
                    mock(KubernetesRestartEventPublisher.class));
            this.delegateAdminClientCall = delegateAdminClientCall;
            this.activeController = activeController;
            this.controllerCall = 0;
            Objects.requireNonNull(acOpenException);
            this.acOpenException = acOpenException;
            this.controllerException = controllerException;
            this.getConfigsException = getConfigsException;
            this.acCloseException = acCloseException;
            this.canRollFn = canRollFn;
            this.unclosedAdminClients = new IdentityHashMap<>();
            this.brokerState = brokerState;
        }

        @Override
        KafkaAgentClient initKafkaAgentClient() {
            return mock(KafkaAgentClient.class, invocation -> {
                if ("getBrokerState".equals(invocation.getMethod().getName())) {
                    if (brokerState == null) {
                        return new BrokerState(-1, null);
                    }
                    return brokerState;
                }
                return null;
            });
        }

        @Override
        protected Admin adminClient(Set<NodeRef> nodes, boolean b) throws ForceableProblem {
            if (delegateAdminClientCall) {
                return super.adminClient(nodes, b);
            }
            RuntimeException exception = acOpenException.apply(nodes);
            if (exception != null) {
                throw new ForceableProblem("An error while try to create the admin client", exception);
            }
            Admin ac = mock(Admin.class, invocation -> {
                if ("close".equals(invocation.getMethod().getName())) {
                    Admin mock = (Admin) invocation.getMock();
                    unclosedAdminClients.remove(mock);
                    if (acCloseException != null) {
                        throw acCloseException;
                    }
                    return null;
                } else if ("incrementalAlterConfigs".equals(invocation.getMethod().getName())) {
                    // The roller thread asks for the config of the node right before updating it dynamically
                    int nodeId = lastConfigNodeId;
                    dynamicUpdateAttempts.merge(nodeId, 1, Integer::sum);
                    AlterConfigsResult result = mock(AlterConfigsResult.class);
                    if (dynamicUpdateFailsFor.contains(nodeId)) {
                        Map<ConfigResource, Collection<AlterConfigOp>> request = invocation.getArgument(0);
                        KafkaFutureImpl<Void> failedFuture = new KafkaFutureImpl<>();
                        failedFuture.completeExceptionally(new RuntimeException("Config update failed"));
                        when(result.values()).thenReturn(request.keySet().stream().collect(Collectors.toMap(Function.identity(), k -> failedFuture)));
                    } else {
                        when(result.values()).thenReturn(Collections.emptyMap());
                    }
                    return result;
                }
                throw new RuntimeException("Not mocked " + invocation.getMethod());
            });
            unclosedAdminClients.put(ac, new Throwable("Pod " + nodes));
            return ac;
        }

        @Override
        protected KafkaAvailability availability(Admin ac) {
            return new KafkaAvailability(null, null) {
                @Override
                protected CompletableFuture<Set<String>> topicNames() {
                    return CompletableFuture.completedFuture(Collections.emptySet());
                }

                @Override
                protected CompletableFuture<Collection<TopicDescription>> describeTopics(Set<String> names) {
                    return CompletableFuture.completedFuture(Collections.emptySet());
                }

                @Override
                CompletableFuture<Boolean> canRoll(int podId) {
                    return canRollFn.apply(podId);
                }
            };
        }

        @Override
        protected KafkaQuorumCheck quorumCheck(Admin ac, NodeRef ref) {
            Admin admin = mock(Admin.class);
            DescribeMetadataQuorumResult qrmResult = mock(DescribeMetadataQuorumResult.class);
            when(admin.describeMetadataQuorum()).thenReturn(qrmResult);
            KafkaFuture<QuorumInfo> kafkaFuture = KafkaFuture.completedFuture(null);
            when(qrmResult.quorumInfo()).thenReturn(kafkaFuture);
            return new KafkaQuorumCheck(null, admin, 0) {
                @Override
                CompletableFuture<Boolean> canRollController(int nodeId) {
                    return canRollFn.apply(nodeId);
                }

                @Override
                CompletableFuture<Integer> quorumLeaderId() {
                    return CompletableFuture.completedFuture(activeController);
                }
            };
        }

        @Override
        boolean deferController(NodeRef nodeRef, RestartContext restartContext) throws Exception {
            Throwable throwable = controllerException.apply(nodeRef.nodeId());
            if (throwable != null) {
                throw new ForceableProblem("An error while trying to determine the cluster controller from pod " + nodeRef.podName(), throwable);
            } else {
                return super.deferController(nodeRef, restartContext);
            }
        }

        @Override
        protected Config getConfig(NodeRef nodeRef, boolean isPureController) throws ForceableProblem {
            lastConfigNodeId = nodeRef.nodeId();
            ForceableProblem problem = getConfigsException.apply(nodeRef.nodeId());
            if (problem != null) {
                throw problem;
            } else return new Config(emptyList());
        }

        @Override
        protected CompletableFuture<Void> restart(Pod pod, RestartContext restartContext) {
            int podId = podName2Number(pod.getMetadata().getName());
            restartAttempts.merge(podId, 1, Integer::sum);
            if (restartFailures.getOrDefault(podId, 0) > 0) {
                restartFailures.merge(podId, -1, Integer::sum);
                return CompletableFuture.failedFuture(new RuntimeException("Test restart failure"));
            }
            restarted.add(pod.getMetadata().getName());
            restartedAtNs.put(podId, System.nanoTime());
            return CompletableFuture.completedFuture(null);
        }
    }
}