/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.strimzi.operator.common.auth.TlsPemIdentity.DUMMY_IDENTITY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ZooKeeperRollerTest {
    private final static Labels DUMMY_SELECTOR = Labels.fromMap(Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, "name", Labels.STRIMZI_NAME_LABEL, "name-zookeeper"));
    private final static List<Pod> PODS = List.of(
            new PodBuilder()
                    .withNewMetadata()
                        .withName("name-zookeeper-0")
                    .endMetadata()
                    .withNewSpec()
                    .endSpec()
                    .build(),
            new PodBuilder()
                    .withNewMetadata()
                    .withName("name-zookeeper-1")
                    .endMetadata()
                    .withNewSpec()
                    .endSpec()
                    .build(),
            new PodBuilder()
                    .withNewMetadata()
                    .withName("name-zookeeper-2")
                    .endMetadata()
                    .withNewSpec()
                    .endSpec()
                    .build()
    );

    @Test
    public void testAllPodsAreRolled(VertxTestContext context)  {
        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(PODS));

        ZookeeperLeaderFinder leaderFinder = mock(ZookeeperLeaderFinder.class);
        when(leaderFinder.findZookeeperLeader(any(), any(), any())).thenReturn(Future.succeededFuture(ZookeeperLeaderFinder.UNKNOWN_LEADER));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, 3, DUMMY_SELECTOR, pod -> List.of("Should restart"), DUMMY_IDENTITY)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(roller.podRestarts.size(), is(3));
                    assertThat(roller.podRestarts.contains("name-zookeeper-0"), is(true));
                    assertThat(roller.podRestarts.contains("name-zookeeper-1"), is(true));
                    assertThat(roller.podRestarts.contains("name-zookeeper-2"), is(true));

                    context.completeNow();
                })));
    }

    @Test
    public void testNonReadyPodsAreRestartedFirst(VertxTestContext context) {
        final String leaderPodReady = "name-zookeeper-2";
        final String followerPodReady = "name-zookeeper-0";
        final String followerPodNonReady = "name-zookeeper-1";

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.isReady(any(), eq(followerPodReady))).thenReturn(true);
        when(podOperator.isReady(any(), eq(followerPodNonReady))).thenReturn(false);
        when(podOperator.isReady(any(), eq(leaderPodReady))).thenReturn(true);
        when(podOperator.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(PODS));
        when(podOperator.readiness(any(), any(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ZookeeperLeaderFinder leaderFinder = mock(ZookeeperLeaderFinder.class);
        when(leaderFinder.findZookeeperLeader(any(), any(), any())).thenReturn(Future.succeededFuture(leaderPodReady));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        Function<Pod, List<String>> shouldRoll = pod -> List.of("Pod was manually annotated to be rolled");
        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, 3, DUMMY_SELECTOR, shouldRoll, DUMMY_IDENTITY)
              .onComplete(context.succeeding(v -> context.verify(() -> {
                  assertThat(roller.podRestarts.size(), is(3));
                  assertThat(roller.podRestarts, contains(followerPodNonReady, followerPodReady, leaderPodReady));
                  context.completeNow();
              })));
    }

    @Test
    public void testNonReadinessOfPodCanPreventAllPodRestarts(VertxTestContext context)  {
        final String followerPodNonReady = "name-zookeeper-1";
        final String leaderPodNeedsRestart = "name-zookeeper-2";
        final String followerPodNeedsRestart = "name-zookeeper-0";
        final Set<String> needsRestart = Set.of(followerPodNeedsRestart, leaderPodNeedsRestart);
        Function<Pod, List<String>> shouldRestart = pod -> {
            if (needsRestart.contains(pod.getMetadata().getName()))  {
                return List.of("Should restart");
            } else {
                return List.of();
            }
        };
        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.isReady(any(), eq(followerPodNeedsRestart))).thenReturn(true);
        when(podOperator.isReady(any(), eq(followerPodNonReady))).thenReturn(false);
        when(podOperator.isReady(any(), eq(leaderPodNeedsRestart))).thenReturn(true);
        when(podOperator.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(PODS));
        when(podOperator.readiness(any(), any(), any(), anyLong(), anyLong())).thenReturn(Future.failedFuture("failure"));


        ZookeeperLeaderFinder leaderFinder = mock(ZookeeperLeaderFinder.class);
        when(leaderFinder.findZookeeperLeader(any(), any(), any())).thenReturn(Future.succeededFuture(leaderPodNeedsRestart));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, 3, DUMMY_SELECTOR, shouldRestart, DUMMY_IDENTITY)
              .onComplete(context.failing(v -> context.verify(() -> {
                  assertThat(roller.podRestarts.size(), is(0));
                  context.completeNow();
              })));
    }

    @Test
    public void testNonReadinessOfLeaderCanPreventAllPodRestarts(VertxTestContext context)  {
        final String followerPod1NeedsRestart = "name-zookeeper-1";
        final String leaderPodNeedsRestartNonReady = "name-zookeeper-2";
        final String followerPod2NeedsRestart = "name-zookeeper-0";
        final Set<String> needsRestart = Set.of(followerPod2NeedsRestart, leaderPodNeedsRestartNonReady);
        Function<Pod, List<String>> shouldRestart = pod -> {
            if (needsRestart.contains(pod.getMetadata().getName()))  {
                return List.of("Should restart");
            } else {
                return List.of();
            }
        };
        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.isReady(any(), eq(followerPod2NeedsRestart))).thenReturn(true);
        when(podOperator.isReady(any(), eq(followerPod1NeedsRestart))).thenReturn(true);
        when(podOperator.isReady(any(), eq(leaderPodNeedsRestartNonReady))).thenReturn(false);
        when(podOperator.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(PODS));
        when(podOperator.readiness(any(), any(), eq(leaderPodNeedsRestartNonReady), anyLong(), anyLong())).thenReturn(Future.failedFuture("failure"));

        ZookeeperLeaderFinder leaderFinder = mock(ZookeeperLeaderFinder.class);
        when(leaderFinder.findZookeeperLeader(any(), any(), any())).thenReturn(Future.succeededFuture(leaderPodNeedsRestartNonReady));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, 3, DUMMY_SELECTOR, shouldRestart, DUMMY_IDENTITY)
              .onComplete(context.failing(v -> context.verify(() -> {
                  assertThat(roller.podRestarts.size(), is(0));
                  context.completeNow();
              })));
    }

    @Test
    public void testNoPodsAreRolled(VertxTestContext context)  {
        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(PODS));

        ZookeeperLeaderFinder leaderFinder = mock(ZookeeperLeaderFinder.class);
        when(leaderFinder.findZookeeperLeader(any(), any(), any())).thenReturn(Future.succeededFuture(ZookeeperLeaderFinder.UNKNOWN_LEADER));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, 3, DUMMY_SELECTOR, pod -> null, DUMMY_IDENTITY)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(roller.podRestarts.size(), is(0));

                    context.completeNow();
                })));
    }

    @Test
    public void testLeaderIsLast(VertxTestContext context)  {
        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(PODS));
        when(podOperator.readiness(any(), any(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ZookeeperLeaderFinder leaderFinder = mock(ZookeeperLeaderFinder.class);
        when(leaderFinder.findZookeeperLeader(any(), any(), any())).thenReturn(Future.succeededFuture("name-zookeeper-1"));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, 3, DUMMY_SELECTOR, pod -> List.of("Should restart"), DUMMY_IDENTITY)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(roller.podRestarts.size(), is(3));
                    assertThat(roller.podRestarts.removeLast(), is("name-zookeeper-1"));
                    assertThat(roller.podRestarts.contains("name-zookeeper-2"), is(true));
                    assertThat(roller.podRestarts.contains("name-zookeeper-0"), is(true));

                    context.completeNow();
                })));
    }

    @Test
    public void testOnlySomePodsAreRolled(VertxTestContext context)  {
        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(PODS));
        when(podOperator.readiness(any(), any(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ZookeeperLeaderFinder leaderFinder = mock(ZookeeperLeaderFinder.class);
        when(leaderFinder.findZookeeperLeader(any(), any(), any())).thenReturn(Future.succeededFuture(ZookeeperLeaderFinder.UNKNOWN_LEADER));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        Function<Pod, List<String>> shouldRoll = pod -> {
            if (!"name-zookeeper-1".equals(pod.getMetadata().getName()))  {
                return List.of("Should restart");
            } else {
                return List.of();
            }
        };

        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, 3, DUMMY_SELECTOR, shouldRoll, DUMMY_IDENTITY)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(roller.podRestarts.size(), is(2));
                    assertThat(roller.podRestarts.contains("name-zookeeper-0"), is(true));
                    assertThat(roller.podRestarts.contains("name-zookeeper-2"), is(true));

                    context.completeNow();
                })));
    }

    @Test
    public void testPodOrdering()   {
        ZooKeeperRoller.ZookeeperPodContext readyContext0 = new ZooKeeperRoller.ZookeeperPodContext("pod-0", null, true, true);
        ZooKeeperRoller.ZookeeperPodContext readyContext1 = new ZooKeeperRoller.ZookeeperPodContext("pod-1", null, true, true);
        ZooKeeperRoller.ZookeeperPodContext readyContext2 = new ZooKeeperRoller.ZookeeperPodContext("pod-2", null, true, true);

        ZooKeeperRoller.ZookeeperPodContext unReadyContext0 = new ZooKeeperRoller.ZookeeperPodContext("pod-0", null, true, false);
        ZooKeeperRoller.ZookeeperPodContext unReadyContext1 = new ZooKeeperRoller.ZookeeperPodContext("pod-1", null, true, false);
        ZooKeeperRoller.ZookeeperPodContext unReadyContext2 = new ZooKeeperRoller.ZookeeperPodContext("pod-2", null, true, false);

        ZooKeeperRoller.ZookeeperPodContext missingPodContext1 = new ZooKeeperRoller.ZookeeperPodContext("pod-1", null, false, false);
        ZooKeeperRoller.ZookeeperPodContext missingPodContext2 = new ZooKeeperRoller.ZookeeperPodContext("pod-2", null, false, false);

        // Test all ready pods ordering
        List<String> rollingOrder = rollingOrder(List.of(readyContext0, readyContext1, readyContext2));
        assertThat(rollingOrder.get(0), is("pod-0"));
        assertThat(rollingOrder.get(1), is("pod-1"));
        assertThat(rollingOrder.get(2), is("pod-2"));

        // Test unready pods ordering
        rollingOrder = rollingOrder(List.of(readyContext0, unReadyContext1, readyContext2));
        assertThat(rollingOrder.get(0), is("pod-1"));
        assertThat(rollingOrder.get(1), is("pod-0"));
        assertThat(rollingOrder.get(2), is("pod-2"));

        // Test missing pods ordering
        rollingOrder = rollingOrder(List.of(readyContext0, missingPodContext1, readyContext2));
        assertThat(rollingOrder.get(0), is("pod-1"));
        assertThat(rollingOrder.get(1), is("pod-0"));
        assertThat(rollingOrder.get(2), is("pod-2"));

        // Test missing and unready pods ordering
        rollingOrder = rollingOrder(List.of(readyContext0, missingPodContext1, unReadyContext2));
        assertThat(rollingOrder.get(0), is("pod-1"));
        assertThat(rollingOrder.get(1), is("pod-2"));
        assertThat(rollingOrder.get(2), is("pod-0"));

        // Test 2 missing pods ordering
        rollingOrder = rollingOrder(List.of(readyContext0, missingPodContext1, missingPodContext2));
        assertThat(rollingOrder.get(0), is("pod-1"));
        assertThat(rollingOrder.get(1), is("pod-2"));
        assertThat(rollingOrder.get(2), is("pod-0"));

        // Test 2 unready pods ordering
        rollingOrder = rollingOrder(List.of(unReadyContext0, unReadyContext1, readyContext2));
        assertThat(rollingOrder.get(0), is("pod-0"));
        assertThat(rollingOrder.get(1), is("pod-1"));
        assertThat(rollingOrder.get(2), is("pod-2"));
    }

    /**
     * Utility method to help with testing of the rolling order.
     *
     * @param podContexts   List of Pod Context representing pods which should be rolled
     *
     * @return  List of pod names in the order in which they should be rolled
     */
    private static List<String> rollingOrder(List<ZooKeeperRoller.ZookeeperPodContext> podContexts)  {
        ZooKeeperRoller.ZookeeperClusterRollContext context = new ZooKeeperRoller.ZookeeperClusterRollContext();

        for (ZooKeeperRoller.ZookeeperPodContext podContext : podContexts) {
            context.add(podContext);
        }

        return context.getPodContextsWithNonExistingAndNonReadyFirst().stream().map(ZooKeeperRoller.ZookeeperPodContext::getPodName).toList();
    }

    static class MockZooKeeperRoller extends ZooKeeperRoller   {
        Deque<String> podRestarts = new ArrayDeque<>();

        public MockZooKeeperRoller(PodOperator podOperator, ZookeeperLeaderFinder leaderFinder, long operationTimeoutMs) {
            super(podOperator, leaderFinder, operationTimeoutMs);
        }

        @Override
        Future<Void> restartPod(Reconciliation reconciliation, String podName, List<String> reasons) {
            podRestarts.add(podName);
            return Future.succeededFuture();
        }
    }
}
