/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;
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
    private final static Labels DUMMY_SELECTOR = Labels.fromMap(Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, "my-cluster", Labels.STRIMZI_NAME_LABEL, "my-cluster-zookeeper"));
    private final static List<Pod> PODS = List.of(
            new PodBuilder()
                    .withNewMetadata()
                        .withName("my-cluster-zookeeper-0")
                    .endMetadata()
                    .withNewSpec()
                    .endSpec()
                    .build(),
            new PodBuilder()
                    .withNewMetadata()
                    .withName("my-cluster-zookeeper-1")
                    .endMetadata()
                    .withNewSpec()
                    .endSpec()
                    .build(),
            new PodBuilder()
                    .withNewMetadata()
                    .withName("my-cluster-zookeeper-2")
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
        when(leaderFinder.findZookeeperLeader(any(), any(), any(), any())).thenReturn(Future.succeededFuture(ZookeeperLeaderFinder.UNKNOWN_LEADER));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, DUMMY_SELECTOR, pod -> List.of("Should restart"), new Secret(), new Secret())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(roller.podRestarts.size(), is(3));
                    assertThat(roller.podRestarts.contains("my-cluster-zookeeper-0"), is(true));
                    assertThat(roller.podRestarts.contains("my-cluster-zookeeper-1"), is(true));
                    assertThat(roller.podRestarts.contains("my-cluster-zookeeper-2"), is(true));

                    context.completeNow();
                })));
    }

    @Test
    public void testNonReadyPodsAreRestartedFirst(VertxTestContext context) {
        final String leaderPodReady = "my-cluster-zookeeper-2";
        final String followerPodReady = "my-cluster-zookeeper-0";
        final String followerPodNonReady = "my-cluster-zookeeper-1";

        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.isReady(any(), eq(followerPodReady))).thenReturn(true);
        when(podOperator.isReady(any(), eq(followerPodNonReady))).thenReturn(false);
        when(podOperator.isReady(any(), eq(leaderPodReady))).thenReturn(true);
        when(podOperator.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(PODS));
        when(podOperator.readiness(any(), any(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ZookeeperLeaderFinder leaderFinder = mock(ZookeeperLeaderFinder.class);
        when(leaderFinder.findZookeeperLeader(any(), any(), any(), any())).thenReturn(Future.succeededFuture(leaderPodReady));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        Function<Pod, List<String>> shouldRoll = pod -> List.of("Pod was manually annotated to be rolled");
        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, DUMMY_SELECTOR, shouldRoll, new Secret(), new Secret())
              .onComplete(context.succeeding(v -> context.verify(() -> {
                  assertThat(roller.podRestarts.size(), is(3));
                  assertThat(roller.podRestarts, contains(followerPodNonReady, followerPodReady, leaderPodReady));
                  context.completeNow();
              })));
    }

    @Test
    public void testNonReadinessOfPodCanPreventAllPodRestarts(VertxTestContext context)  {
        final String followerPodNonReady = "my-cluster-zookeeper-1";
        final String leaderPodNeedsRestart = "my-cluster-zookeeper-2";
        final String followerPodNeedsRestart = "my-cluster-zookeeper-0";
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
        when(leaderFinder.findZookeeperLeader(any(), any(), any(), any())).thenReturn(Future.succeededFuture(leaderPodNeedsRestart));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, DUMMY_SELECTOR, shouldRestart, new Secret(), new Secret())
              .onComplete(context.failing(v -> context.verify(() -> {
                  assertThat(roller.podRestarts.size(), is(0));
                  context.completeNow();
              })));
    }

    @Test
    public void testNonReadinessOfLeaderCanPreventAllPodRestarts(VertxTestContext context)  {
        final String followerPod1NeedsRestart = "my-cluster-zookeeper-1";
        final String leaderPodNeedsRestartNonReady = "my-cluster-zookeeper-2";
        final String followerPod2NeedsRestart = "my-cluster-zookeeper-0";
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
        when(leaderFinder.findZookeeperLeader(any(), any(), any(), any())).thenReturn(Future.succeededFuture(leaderPodNeedsRestartNonReady));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, DUMMY_SELECTOR, shouldRestart, new Secret(), new Secret())
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
        when(leaderFinder.findZookeeperLeader(any(), any(), any(), any())).thenReturn(Future.succeededFuture(ZookeeperLeaderFinder.UNKNOWN_LEADER));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, DUMMY_SELECTOR, pod -> null, new Secret(), new Secret())
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
        when(leaderFinder.findZookeeperLeader(any(), any(), any(), any())).thenReturn(Future.succeededFuture("my-cluster-zookeeper-1"));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, DUMMY_SELECTOR, pod -> List.of("Should restart"), new Secret(), new Secret())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(roller.podRestarts.size(), is(3));
                    assertThat(roller.podRestarts.removeLast(), is("my-cluster-zookeeper-1"));
                    assertThat(roller.podRestarts.contains("my-cluster-zookeeper-2"), is(true));
                    assertThat(roller.podRestarts.contains("my-cluster-zookeeper-0"), is(true));

                    context.completeNow();
                })));
    }

    @Test
    public void testOnlySomePodsAreRolled(VertxTestContext context)  {
        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(PODS));
        when(podOperator.readiness(any(), any(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ZookeeperLeaderFinder leaderFinder = mock(ZookeeperLeaderFinder.class);
        when(leaderFinder.findZookeeperLeader(any(), any(), any(), any())).thenReturn(Future.succeededFuture(ZookeeperLeaderFinder.UNKNOWN_LEADER));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        Function<Pod, List<String>> shouldRoll = pod -> {
            if (!"my-cluster-zookeeper-1".equals(pod.getMetadata().getName()))  {
                return List.of("Should restart");
            } else {
                return List.of();
            }
        };

        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, DUMMY_SELECTOR, shouldRoll, new Secret(), new Secret())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(roller.podRestarts.size(), is(2));
                    assertThat(roller.podRestarts.contains("my-cluster-zookeeper-0"), is(true));
                    assertThat(roller.podRestarts.contains("my-cluster-zookeeper-2"), is(true));

                    context.completeNow();
                })));
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
