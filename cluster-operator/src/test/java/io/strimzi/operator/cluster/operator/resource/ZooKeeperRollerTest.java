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
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
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

        Checkpoint async = context.checkpoint();
        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, DUMMY_SELECTOR, pod -> List.of("Should restart"), new Secret(), new Secret())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(roller.podRestarts.size(), is(3));
                    assertThat(roller.podRestarts.contains("my-cluster-zookeeper-0"), is(true));
                    assertThat(roller.podRestarts.contains("my-cluster-zookeeper-1"), is(true));
                    assertThat(roller.podRestarts.contains("my-cluster-zookeeper-2"), is(true));

                    async.flag();
                })));
    }

    @Test
    public void testNoPodsAreRolled(VertxTestContext context)  {
        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(PODS));

        ZookeeperLeaderFinder leaderFinder = mock(ZookeeperLeaderFinder.class);
        when(leaderFinder.findZookeeperLeader(any(), any(), any(), any())).thenReturn(Future.succeededFuture(ZookeeperLeaderFinder.UNKNOWN_LEADER));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        Checkpoint async = context.checkpoint();
        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, DUMMY_SELECTOR, pod -> null, new Secret(), new Secret())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(roller.podRestarts.size(), is(0));

                    async.flag();
                })));
    }

    @Test
    public void testLeaderIsLast(VertxTestContext context)  {
        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(PODS));

        ZookeeperLeaderFinder leaderFinder = mock(ZookeeperLeaderFinder.class);
        when(leaderFinder.findZookeeperLeader(any(), any(), any(), any())).thenReturn(Future.succeededFuture("my-cluster-zookeeper-1"));

        MockZooKeeperRoller roller = new MockZooKeeperRoller(podOperator, leaderFinder, 300_00L);

        Checkpoint async = context.checkpoint();
        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, DUMMY_SELECTOR, pod -> List.of("Should restart"), new Secret(), new Secret())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(roller.podRestarts.size(), is(3));
                    assertThat(roller.podRestarts.removeLast(), is("my-cluster-zookeeper-1"));
                    assertThat(roller.podRestarts.contains("my-cluster-zookeeper-2"), is(true));
                    assertThat(roller.podRestarts.contains("my-cluster-zookeeper-0"), is(true));

                    async.flag();
                })));
    }

    @Test
    public void testOnlySomePodsAreRolled(VertxTestContext context)  {
        PodOperator podOperator = mock(PodOperator.class);
        when(podOperator.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(PODS));

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

        Checkpoint async = context.checkpoint();
        roller.maybeRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, DUMMY_SELECTOR, shouldRoll, new Secret(), new Secret())
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(roller.podRestarts.size(), is(2));
                    assertThat(roller.podRestarts.contains("my-cluster-zookeeper-0"), is(true));
                    assertThat(roller.podRestarts.contains("my-cluster-zookeeper-2"), is(true));

                    async.flag();
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
