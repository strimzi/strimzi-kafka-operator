/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodConditionBuilder;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.kafka.config.model.Scope;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.assembly.ReconcilerUtils;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.Identity;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("checkstyle:ClassFanOutComplexity")
public class KafkaRollerTest {
    private static final String CLUSTER = "c";
    private static final String CONFIG_NAME = "compression.type";
    private static final String CONFIG_VALUE = "gzip";
    // The desired config of every node. The nodes' current config is the same, so there is nothing to reconfigure.
    private static final String DESIRED_CONFIG = CONFIG_NAME + "=" + CONFIG_VALUE;

    @Test
    public void testRollsControllersAndCombinedThenActiveControllerThenBrokers() {
        int activeController = 4;
        Set<NodeRef> nodes = createNodes(
                broker(0), broker(1), broker(2),
                controller(3), controller(4), controller(5),
                combined(6), combined(7));

        MockPodOperator pods = new MockPodOperator();
        Admin admin = mockAdmin(activeController, List.of(3, 4, 5, 6, 7));

        createRoller(nodes, pods.operator(), admin)
                .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                .toCompletableFuture().join();

        assertThat(pods.restarted(), is(List.of(3, 5, 6, 7, 4, 0, 1, 2)));
        // The broker and the controller admin clients are both created and have to be closed
        verify(admin, times(2)).close(any(Duration.class));
    }

    @Test
    public void testRollsUnreadyNodesFirst() {
        Set<NodeRef> nodes = createNodes(broker(0), broker(1), controller(2), controller(3), controller(4));

        MockPodOperator pods = new MockPodOperator().unready(1, 3);
        Admin admin = mockAdmin(4, List.of(2, 3, 4));

        createRoller(nodes, pods.operator(), admin)
                .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                .toCompletableFuture().join();

        assertThat(pods.restarted(), is(List.of(3, 2, 4, 1, 0)));
    }

    @Test
    public void testNodeNotReadyAfterRestartAbortsRolling() {
        Set<NodeRef> nodes = createNodes(controller(0), controller(1), controller(2), broker(3), broker(4));

        MockPodOperator pods = new MockPodOperator().notReadyAfterRestart(1);
        Admin admin = mockAdmin(2, List.of(0, 1, 2));

        CompletionException e = assertThrows(CompletionException.class, () ->
                createRoller(nodes, pods.operator(), admin)
                        .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                        .toCompletableFuture().join());

        assertThat(e.getCause(), instanceOf(KafkaRoller.FatalProblem.class));
        assertThat(e.getCause().getMessage(), is("Error while waiting for restarted pod c-kafka-1 to become ready"));
        // Controller 0 was restarted and controller 1 failed. Controller 2 and the brokers were never restarted.
        assertThat(pods.restarted(), is(List.of(0, 1)));
        // Only the controller admin client was created, as the brokers were never reached, and it is closed on failure
        verify(admin).close(any(Duration.class));
    }

    /**
     * Admin client is mocked with failure for different node roles
     *
     * @param description               Description of the case, used as the name of the test
     * @param nodes                     The createNodes in the cluster
     * @param controllerClientFails     Whether it is the controller or the broker Admin client that cannot be created
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adminClientCreationFailures")
    public void testNodeIsRestartedWhenAdminClientCannotBeCreated(String description, Set<NodeRef> nodes, boolean controllerClientFails) {
        MockPodOperator pods = new MockPodOperator();
        Admin admin = mockAdmin(0, List.of(0, 1, 2));
        AdminClientProvider adminClientProvider = mock(AdminClientProvider.class);
        if (controllerClientFails) {
            when(adminClientProvider.createControllerAdminClient(any(), any(), any())).thenThrow(new RuntimeException("Test exception"));
            when(adminClientProvider.createAdminClient(any(), any(), any())).thenReturn(admin);
        } else {
            when(adminClientProvider.createAdminClient(any(), any(), any())).thenThrow(new RuntimeException("Test exception"));
            when(adminClientProvider.createControllerAdminClient(any(), any(), any())).thenReturn(admin);
        }

        // Starts with empty restart reasons and KafkaRoller adds one because of the failure.
        Map<String, RestartReasons> reasons = new HashMap<>();
        createRoller(nodes, pods.operator(), adminClientProvider, DESIRED_CONFIG)
                .rollingRestart(pod -> {
                    RestartReasons podReasons = RestartReasons.empty();
                    reasons.put(pod.getMetadata().getName(), podReasons);
                    return podReasons;
                })
                .toCompletableFuture().join();

        // The restart is forced, so the active controller is not deferred and the nodes are rolled in order
        assertThat(pods.restarted(), is(List.of(0, 1, 2)));
        List.of(0, 1, 2).forEach(nodeId ->
                assertThat(reasons.get(KafkaResources.kafkaPodName(CLUSTER, nodeId)).getReasons(), is(Set.of(RestartReason.POD_UNRESPONSIVE))));
    }

    private static Stream<Arguments> adminClientCreationFailures() {
        return Stream.of(
                Arguments.of("controller nodes, controller admin client fails", createNodes(controller(0), controller(1), controller(2)), true),
                Arguments.of("broker nodes, broker admin client fails", createNodes(broker(0), broker(1), broker(2)), false),
                Arguments.of("combined nodes, controller admin client fails", createNodes(combined(0), combined(1), combined(2)), true),
                Arguments.of("combined nodes, broker admin client fails", createNodes(combined(0), combined(1), combined(2)), false));
    }

    @Test
    public void testRollingFailsWhenActiveControllerCannotBeDetermined() {
        Set<NodeRef> nodes = createNodes(controller(0), controller(1), controller(2), broker(3));

        MockPodOperator pods = new MockPodOperator();
        Admin admin = mockAdmin(2, List.of(0, 1, 2));
        KafkaFutureImpl<QuorumInfo> failedQuorumInfo = new KafkaFutureImpl<>();
        failedQuorumInfo.completeExceptionally(new RuntimeException("Test exception"));
        DescribeMetadataQuorumResult failedQuorumResult = mock(DescribeMetadataQuorumResult.class);
        when(failedQuorumResult.quorumInfo()).thenReturn(failedQuorumInfo);
        when(admin.describeMetadataQuorum()).thenReturn(failedQuorumResult);

        CompletionException e = assertThrows(CompletionException.class, () ->
                createRoller(nodes, pods.operator(), admin)
                        .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                        .toCompletableFuture().join());

        assertThat(e.getCause(), instanceOf(KafkaRoller.UnforceableProblem.class));
        assertThat(e.getCause().getMessage(), is("An error while trying to determine the active controller"));
        assertThat(pods.restarted(), is(List.of()));
    }

    @Test
    public void testRollingSucceedsWhenAdminClientsCannotBeClosed() {
        Set<NodeRef> nodes = createNodes(controller(0), controller(1), broker(2));

        MockPodOperator pods = new MockPodOperator();
        Admin admin = mockAdmin(1, List.of(0, 1));
        doThrow(new RuntimeException("Test exception")).when(admin).close(any(Duration.class));

        createRoller(nodes, pods.operator(), admin)
                .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                .toCompletableFuture().join();

        assertThat(pods.restarted(), is(List.of(0, 1, 2)));
        // Both the broker and the controller admin clients were closed, even though closing the first one failed
        verify(admin, times(2)).close(any(Duration.class));
    }

    @Test
    public void testControllerNotInitiallyRollable() {
        MockPodOperator pods = new MockPodOperator();
        Admin admin = mockAdmin(2, List.of(0, 1, 2));
        // Controller 1 has fallen behind the active controller.
        // controllers 0 and 2 cannot be restarted while controller 1 is behind.
        mockLaggingController(admin, () -> pods.restarted().isEmpty(), 2, List.of(0, 1, 2), 1);

        createRoller(createNodes(controller(0), controller(1), controller(2)), pods.operator(), admin)
                .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                .toCompletableFuture().join();
        // Once 1 (the lagging controller) is restarted and caught up, the other controllers can be restarted
        assertThat(pods.restarted(), is(List.of(1, 0, 2)));
    }

    @Test
    public void testCombinedNotInitiallyRollableBecauseOfQuorum() {
        MockPodOperator pods = new MockPodOperator();
        Admin admin = mockAdmin(2, List.of(0, 1, 2));
        // Controller 1 has fallen behind the active controller.
        // controllers 0 and 2 cannot be restarted while controller 1 is behind.
        mockLaggingController(admin, () -> pods.restarted().isEmpty(), 2, List.of(0, 1, 2), 1);

        createRoller(createNodes(combined(0), combined(1), combined(2)), pods.operator(), admin)
                .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                .toCompletableFuture().join();

        // Once 1 (the lagging controller) is restarted and caught up, the other controllers can be restarted
        assertThat(pods.restarted(), is(List.of(1, 0, 2)));
    }

    @Test
    public void testControllersNeverRollable() {
        MockPodOperator pods = new MockPodOperator();
        Admin admin =  mockAdmin(2, List.of(0, 1, 2));
        // Controller 1 is behind the active controller and never catches up
        mockLaggingController(admin, () -> true, 2, List.of(0, 1, 2), 1);

        CompletionException e = assertThrows(CompletionException.class, () ->
                createRoller(createNodes(controller(0), controller(1), controller(2)), pods.operator(), admin)
                        .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                        .toCompletableFuture().join());

        assertThat(e.getCause(), instanceOf(KafkaRoller.UnforceableProblem.class));
        assertThat(e.getCause().getMessage(), is("Pod c-kafka-0 cannot be updated right now."));
        // Only 1 (the lagging controller) is restarted.
        assertThat(pods.restarted(), is(List.of(1)));
    }

    @Test
    public void testCombinedNeverRollableBecauseOfQuorum() {
        MockPodOperator pods = new MockPodOperator();
        Admin admin = mockAdmin(2, List.of(0, 1, 2));
        mockLaggingController(admin, () -> true, 2, List.of(0, 1, 2), 1);

        CompletionException e = assertThrows(CompletionException.class, () ->
                createRoller(createNodes(combined(0), combined(1), combined(2)), pods.operator(), admin)
                        .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                        .toCompletableFuture().join());

        assertThat(e.getCause(), instanceOf(KafkaRoller.UnforceableProblem.class));
        assertThat(e.getCause().getMessage(), is("Pod c-kafka-0 cannot be updated right now."));
        // Only 1 (the lagging controller) is restarted.
        assertThat(pods.restarted(), is(List.of(1)));
    }

    /**
     * Makes one controller fall behind the active controller for as long as the given condition holds. A majority of
     * controllers is then only caught up if the lagging controller itself is the one being restarted, so any other
     * controller cannot be rolled in the meantime.
     */
    private static void mockLaggingController(Admin admin, BooleanSupplier lagging, int activeController, Collection<Integer> controllerIds, int laggingId) {
        when(admin.describeMetadataQuorum()).thenAnswer(invocation -> {
            List<QuorumInfo.ReplicaState> voters = controllerIds.stream()
                    .map(id -> {
                        QuorumInfo.ReplicaState voter = mock(QuorumInfo.ReplicaState.class);
                        when(voter.replicaId()).thenReturn(id);
                        if (id == laggingId && lagging.getAsBoolean()) {
                            when(voter.lastCaughtUpTimestamp()).thenReturn(OptionalLong.of(1000L));
                        } else {
                            when(voter.lastCaughtUpTimestamp()).thenReturn(OptionalLong.of(10000L));
                        }
                        return voter;
                    })
                    .toList();

            QuorumInfo quorumInfo = mock(QuorumInfo.class);
            when(quorumInfo.leaderId()).thenReturn(activeController);
            when(quorumInfo.voters()).thenReturn(voters);
            DescribeMetadataQuorumResult quorumResult = mock(DescribeMetadataQuorumResult.class);
            when(quorumResult.quorumInfo()).thenReturn(KafkaFuture.completedFuture(quorumInfo));
            return quorumResult;
        });
    }

    @Test
    public void testCombinedNotInitiallyRollableBecauseOfAvailability() {
        MockPodOperator pods = new MockPodOperator();
        Admin admin = mockAdmin(2, List.of(0, 1, 2));
        mockUnderReplicatedTopic(admin, () -> pods.restarted().isEmpty(), 0);

        createRoller(createNodes(combined(0), combined(1), combined(2)), pods.operator(), admin)
                .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                .toCompletableFuture().join();

        assertThat(pods.restarted(), is(List.of(1, 0, 2)));
    }

    @Test
    public void testBrokerNotInitiallyRollable() {
        MockPodOperator pods = new MockPodOperator();
        Admin admin = mockAdmin(2, List.of());
        mockUnderReplicatedTopic(admin, () -> pods.restarted().isEmpty(), 0);

        createRoller(createNodes(broker(0), broker(1), broker(2)), pods.operator(), admin)
                .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                .toCompletableFuture().join();

        assertThat(pods.restarted(), is(List.of(1, 2, 0)));
    }

    @Test
    public void testBrokerNeverRollable() {
        MockPodOperator pods = new MockPodOperator();
        Admin admin = mockAdmin(2, List.of());
        // Broker 0 is in the in-sync replicas of a topic that has no more in-sync
        mockUnderReplicatedTopic(admin, () -> true, 0);

        CompletionException e = assertThrows(CompletionException.class, () ->
                createRoller(createNodes(broker(0), broker(1), broker(2)), pods.operator(), admin)
                        .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                        .toCompletableFuture().join());

        assertThat(e.getCause(), instanceOf(KafkaRoller.UnforceableProblem.class));
        assertThat(e.getCause().getMessage(), is("Pod c-kafka-0 cannot be updated right now."));
        // The other brokers have no replicas of the topic
        assertThat(pods.restarted(), is(List.of(1, 2)));
    }

    @Test
    public void testCombinedNeverRollableBecauseOfAvailability() {
        MockPodOperator pods = new MockPodOperator();
        Admin admin = mockAdmin(2, List.of(0, 1, 2));
        mockUnderReplicatedTopic(admin, () -> true, 0);

        CompletionException e = assertThrows(CompletionException.class, () ->
                createRoller(createNodes(combined(0), combined(1), combined(2)), pods.operator(), admin)
                        .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                        .toCompletableFuture().join());

        assertThat(e.getCause(), instanceOf(KafkaRoller.UnforceableProblem.class));
        assertThat(e.getCause().getMessage(), is("Pod c-kafka-0 cannot be updated right now."));
        assertThat(pods.restarted(), is(List.of(1, 2)));
    }


    /**
     * Adds a topic that has the given broker in its in-sync replicas, with the minimum in-sync replicas as the
     * only other member. Restarting the broker would drop the topic below the minimum, so the broker cannot be
     * rolled for as long as the given condition holds.
     */
    private static void mockUnderReplicatedTopic(Admin admin, BooleanSupplier underReplicated, int blockedId) {
        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of("t")));
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(listTopicsResult);

        when(admin.describeTopics(anyCollection())).thenAnswer(invocation -> {
            List<Node> replicas = List.of(new Node(blockedId, "host", 9092), new Node(100, "host", 9092), new Node(101, "host", 9092));
            List<Node> isr = underReplicated.getAsBoolean() ? replicas.subList(0, 2) : replicas;
            TopicDescription topic = new TopicDescription("t", false, List.of(new TopicPartitionInfo(0, replicas.getFirst(), replicas, isr)));
            DescribeTopicsResult result = mock(DescribeTopicsResult.class);
            when(result.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("t", topic)));
            return result;
        });
    }

    @Test
    public void testNodeInLogRecoveryIsNotRolled() {
        MockPodOperator pods = new MockPodOperator().neverReady(0);
        BrokerState recovery = new BrokerState(2, Map.of("remainingLogsToRecover", 10, "remainingSegmentsToRecover", 100));

        CompletionException e = assertThrows(CompletionException.class, () ->
                createRoller(createNodes(broker(0), broker(1), broker(2)), pods.operator(), mockAdmin(2, List.of()), recovery)
                        .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                        .toCompletableFuture().join());

        assertThat(e.getCause(), instanceOf(KafkaRoller.UnforceableProblem.class));
        assertThat(e.getCause().getMessage(), is("Pod c-kafka-0 is not ready because the Kafka node is performing log recovery. There are 10 logs and 100 segments left to recover."));
        assertThat(pods.restarted(), is(List.of(1, 2)));
    }

    @Test
    public void testNotReadyBrokerInRunningStateIsRolled() {
        MockPodOperator pods = new MockPodOperator().neverReady(0);
        BrokerState running = new BrokerState(3, null);

        CompletionException e = assertThrows(CompletionException.class, () ->
                createRoller(createNodes(broker(0), broker(1)), pods.operator(), mockAdmin(2, List.of()), running)
                        .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                        .toCompletableFuture().join());

        assertThat(e.getCause(), instanceOf(KafkaRoller.FatalProblem.class));
        assertThat(e.getCause().getMessage(), is("Error while waiting for restarted pod c-kafka-0 to become ready"));
        assertThat(pods.restarted(), is(List.of(0)));
    }

    @Test
    public void testNotReadyBrokerWhoseStateCannotBeReadIsRolled() {
        MockPodOperator pods = new MockPodOperator().neverReady(0);
        BrokerState unreadable = new BrokerState(-1, null);

        CompletionException e = assertThrows(CompletionException.class, () ->
                createRoller(createNodes(broker(0), broker(1)), pods.operator(), mockAdmin(2, List.of()), unreadable)
                        .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                        .toCompletableFuture().join());

        assertThat(e.getCause(), instanceOf(KafkaRoller.FatalProblem.class));
        assertThat(e.getCause().getMessage(), is("Error while waiting for restarted pod c-kafka-0 to become ready"));
        assertThat(pods.restarted(), is(List.of(0)));
    }

    @Test
    public void testRollingFailsWhenReadinessCannotBeChecked() {
        Set<NodeRef> nodes = createNodes(controller(0), broker(1));

        MockPodOperator pods = new MockPodOperator().readinessCheckFails(0, new KubernetesClientException("Failed"));
        Admin admin = mockAdmin(0, List.of(0));

        CompletionException e = assertThrows(CompletionException.class, () ->
                createRoller(nodes, pods.operator(), admin)
                        .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                        .toCompletableFuture().join());

        assertThat(e.getCause(), instanceOf(KubernetesClientException.class));
        assertThat(e.getCause().getMessage(), is("Failed"));
        assertThat(pods.restarted(), is(List.of()));
    }

    /**
     * Tests with different pod stuck statuses
     *
     * @param description   Description of the way the pod is stuck, used as the name of the test
     * @param status        Status of the stuck pod
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("stuckPodStatuses")
    public void testStuckPodAbortsRolling(String description, PodStatus status) {
        Set<NodeRef> nodes = createNodes(controller(0), broker(1));

        MockPodOperator pods = new MockPodOperator().withStatus(0, status);
        Admin admin = mockAdmin(0, List.of(0));

        CompletionException e = assertThrows(CompletionException.class, () ->
                createRoller(nodes, pods.operator(), admin)
                        .rollingRestart(pod -> RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE))
                        .toCompletableFuture().join());

        assertThat(e.getCause(), instanceOf(KafkaRoller.FatalProblem.class));
        assertThat(e.getCause().getMessage(), is("Pod is unschedulable or is not starting"));
        assertThat(pods.restarted(), is(List.of()));
    }

    /**
     * Tests with different pod stuck statuses
     *
     * @param description   Description of the way the pod is stuck, used as the name of the test
     * @param status        Status of the stuck pod
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("stuckPodStatuses")
    public void testStuckPodWithOldRevisionIsRestarted(String description, PodStatus status) {
        Set<NodeRef> nodes = createNodes(controller(0), broker(1));

        MockPodOperator pods = new MockPodOperator().withStatus(0, status);
        Admin admin = mockAdmin(0, List.of(0));

        createRoller(nodes, pods.operator(), admin)
                .rollingRestart(pod -> RestartReasons.of(RestartReason.POD_HAS_OLD_REVISION))
                .toCompletableFuture().join();

        assertThat(pods.restarted(), is(List.of(0, 1)));
    }

    private static Stream<Arguments> stuckPodStatuses() {
        return Stream.of(
                waitingContainer("CrashLoopBackOff"),
                waitingContainer("ImagePullBackOff"),
                waitingContainer("ContainerCreating"),
                waitingContainer("InvalidImageName"),
                Arguments.of("Pending and unschedulable", new PodStatusBuilder()
                        .withPhase("Pending")
                        .withConditions(new PodConditionBuilder()
                                .withType("PodScheduled")
                                .withStatus("False")
                                .withReason("Unschedulable")
                                .build())
                        .build()));
    }

    private static Arguments waitingContainer(String reason) {
        return Arguments.of("Kafka container waiting: " + reason, new PodStatusBuilder()
                .withContainerStatuses(new ContainerStatusBuilder()
                        .withName("kafka")
                        .withNewState()
                        .withNewWaiting()
                        .withReason(reason)
                        .endWaiting()
                        .endState()
                        .build())
                .build());
    }

    @Test
    public void testConfigIsUpdatedDynamicallyWithoutRestart() {
        MockPodOperator pods = new MockPodOperator();
        Admin admin = mockAdmin(1, List.of(1, 2));
        AlterConfigsResult alterResult = mock(AlterConfigsResult.class);
        when(alterResult.values()).thenReturn(Map.of());
        when(admin.incrementalAlterConfigs(any())).thenReturn(alterResult);
        String desiredConfig = DESIRED_CONFIG + "\nsasl.login.refresh.buffer.seconds=100";

        createRoller(createNodes(broker(0), controller(1), combined(2)), pods.operator(), ResourceUtils.adminClientProvider(admin), desiredConfig)
                .rollingRestart(pod -> RestartReasons.empty())
                .toCompletableFuture().join();

        for (int nodeId : List.of(0, 1, 2)) {
            verify(admin).incrementalAlterConfigs(Map.of(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(nodeId)),
                    List.of(new AlterConfigOp(new ConfigEntry("sasl.login.refresh.buffer.seconds", "100"), AlterConfigOp.OpType.SET))));
        }
        verify(admin, times(3)).incrementalAlterConfigs(any());
        assertThat(pods.restarted(), is(List.of()));
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

    /**
     * Mocks Admin client with failed describeConfig for different node roles
     *
     * @param description   Description of the case, used as the name of the test
     * @param nodes         The nodes in the cluster
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("clusterNodes")
    public void testNodeIsRestartedWhenItsConfigCannotBeRead(String description, Set<NodeRef> nodes) {
        int failingNodeId = 1;
        MockPodOperator pods = new MockPodOperator();
        Admin admin = mockAdmin(2, List.of(0, 1, 2));

        when(admin.describeConfigs(anyCollection())).thenAnswer(invocation -> {
            Map<ConfigResource, KafkaFuture<Config>> futures = new HashMap<>();
            Map<ConfigResource, Config> configs = new HashMap<>();
            for (ConfigResource resource : invocation.<Collection<ConfigResource>>getArgument(0)) {
                KafkaFutureImpl<Config> future = new KafkaFutureImpl<>();
                if (resource.type() == ConfigResource.Type.BROKER && resource.name().equals(String.valueOf(failingNodeId))) {
                    future.completeExceptionally(new RuntimeException("Test exception"));
                } else {
                    Config config = new Config(List.of(new ConfigEntry(CONFIG_NAME, CONFIG_VALUE)));
                    configs.put(resource, config);
                    future.complete(config);
                }
                futures.put(resource, future);
            }
            DescribeConfigsResult result = mock(DescribeConfigsResult.class);
            when(result.values()).thenReturn(futures);
            when(result.all()).thenReturn(KafkaFuture.completedFuture(configs));
            return result;
        });

        Map<String, RestartReasons> reasons = new HashMap<>();
        createRoller(nodes, pods.operator(), admin)
                .rollingRestart(pod -> {
                    RestartReasons podReasons = RestartReasons.empty();
                    reasons.put(pod.getMetadata().getName(), podReasons);
                    return podReasons;
                })
                .toCompletableFuture().join();

        assertThat(pods.restarted(), is(List.of(failingNodeId)));
        assertThat(reasons.get(KafkaResources.kafkaPodName(CLUSTER, failingNodeId)).getReasons(), is(Set.of(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART)));
    }

    private static Stream<Arguments> clusterNodes() {
        return Stream.of(
                Arguments.of("controller nodes", createNodes(controller(0), controller(1), controller(2))),
                Arguments.of("broker nodes", createNodes(broker(0), broker(1), broker(2))),
                Arguments.of("combined nodes", createNodes(combined(0), combined(1), combined(2))));
    }

    @Test
    public void testRestartTakesSnapshotOfRestartReasons() {
        MockPodOperator pods = new MockPodOperator();
        Admin admin = mockAdmin(0, List.of());
        KubernetesRestartEventPublisher eventPublisher = mock(KubernetesRestartEventPublisher.class);

        RestartReasons pod0Reasons = RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
        when(pods.operator().restart(any(), any(), anyLong())).thenAnswer(invocation -> {
            // Mutate restartContext.restartReasons immediately after restart was initiated
            pod0Reasons.add(RestartReason.POD_STUCK);
            return CompletableFuture.completedFuture(null);
        });

        createRoller(createNodes(broker(0)), pods.operator(), admin, eventPublisher)
                .rollingRestart(pod -> pod0Reasons)
                .toCompletableFuture().join();

        Pod pod0 = new PodBuilder().withNewMetadata().withName(KafkaResources.kafkaPodName(CLUSTER, 0)).withNamespace("ns").endMetadata().build();

        // Verify that the event publisher received only the snapshot (MANUAL_ROLLING_UPDATE) and not POD_STUCK
        ArgumentCaptor<RestartReasons> captor = ArgumentCaptor.forClass(RestartReasons.class);
        verify(eventPublisher, timeout(1000)).publishRestartEvents(any(), eq(pod0), captor.capture());
        assertThat(captor.getValue().contains(RestartReason.MANUAL_ROLLING_UPDATE), is(true));
        assertThat(captor.getValue().contains(RestartReason.POD_STUCK), is(false));
    }

    private static KafkaRoller createRoller(Set<NodeRef> nodes, PodOperator podOps, Admin admin) {
        return createRoller(nodes, podOps, ResourceUtils.adminClientProvider(admin), DESIRED_CONFIG);
    }

    private static KafkaRoller createRoller(Set<NodeRef> nodes, PodOperator podOps, Admin admin, BrokerState brokerState) {
        return createRoller(nodes, podOps, ResourceUtils.adminClientProvider(admin), DESIRED_CONFIG, brokerState);
    }

    private static KafkaRoller createRoller(Set<NodeRef> nodes, PodOperator podOps, Admin admin, KubernetesRestartEventPublisher eventsPublisher) {
        return createRoller(nodes, podOps, ResourceUtils.adminClientProvider(admin), DESIRED_CONFIG, new BrokerState(1, null), eventsPublisher);
    }

    private static KafkaRoller createRoller(Set<NodeRef> nodes, PodOperator podOps, AdminClientProvider adminClientProvider, String desiredConfig) {
        // By default, the broker is starting, so it is not performing a log recovery
        return createRoller(nodes, podOps, adminClientProvider, desiredConfig, new BrokerState(1, null));
    }

    private static KafkaRoller createRoller(Set<NodeRef> nodes, PodOperator podOps, AdminClientProvider adminClientProvider, String desiredConfig, BrokerState brokerState) {
        return createRoller(nodes, podOps, adminClientProvider, desiredConfig, brokerState, mock(KubernetesRestartEventPublisher.class));
    }

    private static KafkaRoller createRoller(Set<NodeRef> nodes, PodOperator podOps, AdminClientProvider adminClientProvider, String desiredConfig, BrokerState brokerState, KubernetesRestartEventPublisher eventsPublisher) {
        KafkaAgentClient agentClient = mock(KafkaAgentClient.class);
        when(agentClient.getBrokerState(anyString())).thenReturn(brokerState);

        return new KafkaRoller(
                new Reconciliation("test", "Kafka", "ns", CLUSTER),
                podOps,
                500,
                1000,
                () -> new BackOff(10L, 2, 4),
                nodes,
                new Identity(null, null),
                adminClientProvider,
                ResourceUtils.kafkaAgentClientProvider(agentClient),
                nodeId -> desiredConfig,
                KafkaVersionTestUtils.getLatestVersion(),
                true,
                eventsPublisher);
    }

    private static NodeRef broker(int nodeId) {
        return new NodeRef(KafkaResources.kafkaPodName(CLUSTER, nodeId), nodeId, "brokers", false, true);
    }

    private static NodeRef controller(int nodeId) {
        return new NodeRef(KafkaResources.kafkaPodName(CLUSTER, nodeId), nodeId, "controllers", true, false);
    }

    private static NodeRef combined(int nodeId) {
        return new NodeRef(KafkaResources.kafkaPodName(CLUSTER, nodeId), nodeId, "combined", true, true);
    }

    private static Set<NodeRef> createNodes(NodeRef... nodes) {
        return new LinkedHashSet<>(List.of(nodes));
    }

    /**
     * Mocks the pod operator. By default, all pods exist and are ready, and restarting a pod completes immediately.
     * The restarted pods are recorded in the order they were restarted.
     */
    private static final class MockPodOperator {
        private final List<Integer> restarted = new ArrayList<>();
        private final Set<Integer> unready = new HashSet<>();
        private final Set<Integer> neverReady = new HashSet<>();
        private final Set<Integer> notReadyAfterRestart = new HashSet<>();
        private final Map<Integer, RuntimeException> readinessCheckFailures = new HashMap<>();
        private final Map<Integer, PodStatus> statuses = new HashMap<>();
        private final Map<Integer, Map<String, String>> labels = new HashMap<>();

        /**
         * Pods that are not ready when the rolling starts, but become ready while the createRoller waits for them. So
         * they are considered unready for ordering the createNodes, but they can be restarted.
         */
        MockPodOperator unready(int... nodeIds) {
            Arrays.stream(nodeIds).forEach(unready::add);
            return this;
        }

        /**
         * Pods that are not ready and never become ready, whether they are restarted or not.
         */
        MockPodOperator neverReady(int... nodeIds) {
            Arrays.stream(nodeIds).forEach(neverReady::add);
            return this;
        }

        /**
         * Pods that are ready until they are restarted, and then never become ready again.
         */
        MockPodOperator notReadyAfterRestart(int... nodeIds) {
            Arrays.stream(nodeIds).forEach(notReadyAfterRestart::add);
            return this;
        }

        /**
         * Pods for which checking the readiness fails with the given error, instead of just reporting that the pod
         * is not ready.
         */
        MockPodOperator readinessCheckFails(int nodeId, RuntimeException error) {
            readinessCheckFailures.put(nodeId, error);
            return this;
        }

        /**
         * Pods that have the given status, for example the status of a pod that is stuck. By default, pods have no status.
         */
        MockPodOperator withStatus(int nodeId, PodStatus status) {
            statuses.put(nodeId, status);
            return this;
        }

        /**
         * Stores restarted createNodes in the order
         * @return IDs of the restarted pods, in the order they were restarted
         */
        List<Integer> restarted() {
            return restarted;
        }

        PodOperator operator() {
            PodOperator podOps = mock(PodOperator.class);

            when(podOps.get(anyString(), anyString())).thenAnswer(invocation -> {
                String podName = invocation.getArgument(1);
                Integer nodeId = ReconcilerUtils.getPodIndexFromPodName(podName);
                return new PodBuilder()
                        .withNewMetadata()
                            .withNamespace(invocation.getArgument(0))
                            .withName(podName)
                            .withLabels(labels.getOrDefault(nodeId, Map.of()))
                        .endMetadata()
                        .withStatus(statuses.get(nodeId))
                        .build();
            });

            // Used by the KafkaRoller to order the nodes
            when(podOps.isReady(anyString(), anyString())).thenAnswer(invocation -> {
                Integer nodeId = ReconcilerUtils.getPodIndexFromPodName(invocation.getArgument(1));
                if (readinessCheckFailures.containsKey(nodeId)) {
                    throw readinessCheckFailures.get(nodeId);
                }
                return !unready.contains(nodeId) && !neverReady.contains(nodeId);
            });

            // Used by the KafkaRoller to wait for the pods. It is called on the KafkaRoller thread, which is also the thread
            // that restarts the pods, so the restarted list is safe to read.
            when(podOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenAnswer(invocation -> {
                Integer nodeId =  ReconcilerUtils.getPodIndexFromPodName(invocation.getArgument(2));
                boolean ready = !neverReady.contains(nodeId)
                        && !(notReadyAfterRestart.contains(nodeId) && restarted.contains(nodeId));
                return ready
                        ? CompletableFuture.completedFuture(null)
                        : CompletableFuture.failedFuture(new TimeoutException("Timeout"));
            });

            when(podOps.restart(any(), any(Pod.class), anyLong())).thenAnswer(invocation -> {
                Pod pod = invocation.getArgument(1);
                restarted.add(ReconcilerUtils.getPodIndexFromPodName(pod.getMetadata().getName()));
                return CompletableFuture.completedFuture(null);
            });

            return podOps;
        }
    }


    /**
     * Mocks an Admin client of a healthy cluster: the nodes report the desired config, there are no topics
     * and all the given controllers are caught up with the active controller.
     *
     * @param activeController  Id of the active controller (the quorum leader)
     * @param controllerIds     Ids of all the createNodes with the controller role
     */
    private static Admin mockAdmin(int activeController, Collection<Integer> controllerIds) {
        Admin admin = mock(Admin.class);

        // Used by the KafkaRoller to get node config and by KafkaAvailability to get topic configs
        when(admin.describeConfigs(anyCollection())).thenAnswer(invocation -> {
            Map<ConfigResource, KafkaFuture<Config>> futures = new HashMap<>();
            Map<ConfigResource, Config> configs = new HashMap<>();
            for (ConfigResource resource : invocation.<Collection<ConfigResource>>getArgument(0)) {
                // Topics are only used to make a broker unrollable, which needs min.insync.replicas
                ConfigEntry entry = resource.type() == ConfigResource.Type.TOPIC
                        ? new ConfigEntry("min.insync.replicas", "2")
                        : new ConfigEntry(CONFIG_NAME, CONFIG_VALUE);
                Config config = new Config(List.of(entry));
                configs.put(resource, config);
                futures.put(resource, KafkaFuture.completedFuture(config));
            }
            DescribeConfigsResult result = mock(DescribeConfigsResult.class);
            when(result.values()).thenReturn(futures);
            when(result.all()).thenReturn(KafkaFuture.completedFuture(configs));
            return result;
        });

        // Used by KafkaAvailability. There are no topics, so no broker is blocked from being rolled.
        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of()));
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(listTopicsResult);

        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        when(describeTopicsResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of()));
        when(admin.describeTopics(anyCollection())).thenReturn(describeTopicsResult);

        // Used by KafkaQuorumCheck. All controllers are caught up with the leader, so the quorum is healthy.
        List<QuorumInfo.ReplicaState> voters = controllerIds.stream()
                .map(id -> {
                    QuorumInfo.ReplicaState voter = mock(QuorumInfo.ReplicaState.class);
                    when(voter.replicaId()).thenReturn(id);
                    when(voter.lastCaughtUpTimestamp()).thenReturn(OptionalLong.of(1000L));
                    return voter;
                })
                .toList();
        QuorumInfo quorumInfo = mock(QuorumInfo.class);
        when(quorumInfo.leaderId()).thenReturn(activeController);
        when(quorumInfo.voters()).thenReturn(voters);
        DescribeMetadataQuorumResult quorumResult = mock(DescribeMetadataQuorumResult.class);
        when(quorumResult.quorumInfo()).thenReturn(KafkaFuture.completedFuture(quorumInfo));
        when(admin.describeMetadataQuorum()).thenReturn(quorumResult);

        return admin;
    }
}