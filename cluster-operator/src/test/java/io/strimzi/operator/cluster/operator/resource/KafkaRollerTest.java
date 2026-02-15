/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
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
import io.strimzi.operator.common.auth.TlsPemIdentity;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
                null, noException(), noException(), noException(),
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
                null, noException(), noException(), noException(),
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
                null, noException(), noException(), noException(),
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
                null, noException(), noException(), noException(),
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
                podId -> podId == nonActiveController ? new RuntimeException("Test Exception") : null, noException(), noException(),
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
                podId -> podId == activeController ? new RuntimeException("Test Exception") : null, noException(), noException(),
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
                new RuntimeException("Test Exception"), noException(), noException(), noException(),
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
                new RuntimeException("Test Exception"), noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), noException(), podId -> podId == 1 ? new KafkaRoller.ForceableProblem("could not get config exception") : null,
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
                noException(), noException(), podId -> podId == 1 ? new KafkaRoller.ForceableProblem("could not get config exception") : null,
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
                noException(), noException(), podId -> podId == 1 ? new KafkaRoller.ForceableProblem("could not get config exception") : null,
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
                noException(), noException(), podId -> podId == controller ? new KafkaRoller.ForceableProblem("could not get config exception") : null,
                brokerId -> CompletableFuture.completedFuture(true), new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, controller);
        // The algorithm should carry on rolling the pods
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4),
                asList(0, 1, 3, 4, controller));
    }

    @Test
    public void testRollHandlesErrorWhenAlteringConfig() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(REPLICAS, 0, 0), podOps,
                noException(), null,
                noException(), podId -> new KafkaRoller.ForceableProblem("could not get alter exception"), noException(),
                brokerId -> CompletableFuture.completedFuture(true), new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should carry on rolling the pods
        doSuccessfulConfigUpdate(kafkaRoller,
                asList(0, 1, 2, 3, 4));
    }

    @Test
    public void testSuccessfulAlteringConfigNotRoll() {
        PodOperator podOps = mockPodOps(podId -> CompletableFuture.completedFuture(null));
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(3, 3, 3), podOps,
                noException(), null,
                noException(), noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true), new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        // The algorithm should not carry on rolling the pods
        doSuccessfulConfigUpdate(kafkaRoller,
                emptyList());
    }

    @Test
    public void testSuccessfulRollingControllers() {
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 3, 3),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true), new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4, 5),
                asList(0, 1, 2, 3, 4, 5));
    }

    @Test
    public void testControllerNoQuorum() {
        TestingKafkaRoller kafkaRoller = new TestingKafkaRoller(addPodNames(0, 0, 3),
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                noException(), null, noException(), noException(), noException(),
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
                mockPodOps(podId -> CompletableFuture.completedFuture(null)), noException(), null, noException(), noException(), noException(),
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
                podOps, noException(), null, noException(), noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true), new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, -1);
        doSuccessfulRollingRestart(kafkaRoller,
                asList(0, 1, 2, 3, 4, 5, 6, 7, 8), // brokers, combined, controllers
                asList(7, 4, 3, 5, 6, 8, 1, 0, 2)); //Rolls in order: unready controllers, ready controllers, unready brokers, ready brokers
    }

    private TestingKafkaRoller rollerWithActiveController(PodOperator podOps, Set<NodeRef> nodes, int activeController) {
        return new TestingKafkaRoller(nodes, podOps,
                noException(), null, noException(), noException(), noException(),
                brokerId -> CompletableFuture.completedFuture(true),
                new DefaultAdminClientProvider(), new DefaultKafkaAgentClientProvider(), false, null, activeController);
    }

    private void doSuccessfulConfigUpdate(TestingKafkaRoller kafkaRoller,
                                          List<Integer> expected) {
        kafkaRoller.rollingRestart(pod -> RestartReasons.empty())
                .whenComplete((v, error) -> {
                    assertThat(restarted(), is(expected));
                    assertNoUnclosedAdminClient(kafkaRoller);
                });
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
                });
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
        kafkaRoller
            .rollingRestart(pod -> {
                if (podsToRestart.contains(podName2Number(pod.getMetadata().getName()))) {
                    return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
                } else {
                    return RestartReasons.empty();
                }
            })
            .whenComplete((r, error) -> {
                assertThat(error.getClass() + " is not a subclass of " + exception.getName(), error, instanceOf(exception));
                assertThat("The exception message was not as expected", error.getMessage(), is(message));
                assertThat("The restarted pods were not as expected", restarted(), is(expectedRestart));
                assertNoUnclosedAdminClient(kafkaRoller);
            });
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
            return readiness.apply(podName2Number(podName));
        });
        return podOps;
    }

    private class TestingKafkaRoller extends KafkaRoller {

        int controllerCall;
        private final IdentityHashMap<Admin, Throwable> unclosedAdminClients;
        private final Function<Set<NodeRef>, RuntimeException> acOpenException;
        private final Throwable acCloseException;
        private final Function<Integer, CompletableFuture<Boolean>> canRollFn;
        private final Function<Integer, Throwable> controllerException;
        private final Function<Integer, ForceableProblem> alterConfigsException;
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
                                   Function<Integer, ForceableProblem> alterConfigsException,
                                   Function<Integer, ForceableProblem> getConfigsException,
                                   Function<Integer, CompletableFuture<Boolean>> canRollFn,
                                   AdminClientProvider adminClientProvider,
                                   KafkaAgentClientProvider kafkaAgentClientProvider,
                                   boolean delegateAdminClientCall, BrokerState brokerState, int activeController) {
            super(
                    new Reconciliation("test", "Kafka", stsNamespace(), clusterName()),
                    podOps,
                    500,
                    1000,
                    () -> new BackOff(10L, 2, 4),
                    nodes,
                    new TlsPemIdentity(null, null),
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
            this.alterConfigsException = alterConfigsException;
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
            ForceableProblem problem = getConfigsException.apply(nodeRef.nodeId());
            if (problem != null) {
                throw problem;
            } else return new Config(emptyList());
        }

        @Override
        protected void dynamicUpdateKafkaConfig(NodeRef nodeRef, Admin ac, KafkaConfigurationDiff configurationDiff) throws ForceableProblem {
            ForceableProblem problem = alterConfigsException.apply(nodeRef.nodeId());
            if (problem != null) {
                throw problem;
            }
        }

        @Override
        protected CompletableFuture<Void> restart(Pod pod, RestartContext restartContext) {
            restarted.add(pod.getMetadata().getName());
            return CompletableFuture.completedFuture(null);
        }
    }
}