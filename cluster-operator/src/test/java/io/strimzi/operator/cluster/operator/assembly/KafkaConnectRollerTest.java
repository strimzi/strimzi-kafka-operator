/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.MockSharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.PodRevision;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
@SuppressWarnings("unchecked")
public class KafkaConnectRollerTest {
    private final static String NAME = "my-connect";
    private final static String NAMESPACE = "my-namespace";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final Reconciliation RECONCILIATION = new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME);
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private static final KafkaConnect CONNECT = new KafkaConnectBuilder()
            .withNewMetadata()
                .withName(NAME)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
            .endSpec()
            .build();

    private static final KafkaConnectCluster CLUSTER = KafkaConnectCluster.fromCrd(RECONCILIATION, CONNECT, VERSIONS, SHARED_ENV_PROVIDER);

    private static final List<String> POD_NAMES = List.of("my-connect-connect-0", "my-connect-connect-1", "my-connect-connect-2");
    private static final Pod READY_POD = new PodBuilder()
            .withNewMetadata()
                .withName("my-connect-connect-0")
                .withAnnotations(Map.of(PodRevision.STRIMZI_REVISION_ANNOTATION, "avfc1874"))
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .withNewStatus()
                .addNewCondition()
                    .withType("Ready")
                    .withStatus("True")
                .endCondition()
            .endStatus()
            .build();
    private static final Pod UNREADY_POD = new PodBuilder(READY_POD)
            .withNewStatus()
                .addNewCondition()
                    .withType("Ready")
                    .withStatus("False")
                .endCondition()
            .endStatus()
            .build();

    @Test
    public void testRollingOrderWithAllPodsReady()  {
        List<Pod> pods = List.of(
                renamePod(READY_POD, "my-connect-connect-0"),
                renamePod(READY_POD, "my-connect-connect-1"),
                renamePod(READY_POD, "my-connect-connect-2")
        );

        KafkaConnectRoller roller = new KafkaConnectRoller(RECONCILIATION, CLUSTER, 1_000L, null);
        Queue<String> rollingOrder = roller.prepareRollingOrder(POD_NAMES, pods);

        assertThat(rollingOrder.size(), is(3));
        assertThat(rollingOrder.poll(), is("my-connect-connect-0"));
        assertThat(rollingOrder.poll(), is("my-connect-connect-1"));
        assertThat(rollingOrder.poll(), is("my-connect-connect-2"));
    }

    @Test
    public void testRollingWithSomePodsOnly()  {
        List<Pod> pods = List.of(
                renamePod(READY_POD, "my-connect-connect-0"),
                renamePod(READY_POD, "my-connect-connect-1"),
                renamePod(READY_POD, "my-connect-connect-2")
        );

        KafkaConnectRoller roller = new KafkaConnectRoller(RECONCILIATION, CLUSTER, 1_000L, null);
        Queue<String> rollingOrder = roller.prepareRollingOrder(List.of("my-connect-connect-1"), pods);

        assertThat(rollingOrder.size(), is(1));
        assertThat(rollingOrder.poll(), is("my-connect-connect-1"));
    }

    @Test
    public void testRollingOrderWithMissingPod()  {
        List<Pod> pods = List.of(
                renamePod(READY_POD, "my-connect-connect-0"),
                renamePod(READY_POD, "my-connect-connect-2")
        );

        KafkaConnectRoller roller = new KafkaConnectRoller(RECONCILIATION, CLUSTER, 1_000L, null);
        Queue<String> rollingOrder = roller.prepareRollingOrder(POD_NAMES, pods);

        assertThat(rollingOrder.size(), is(3));
        assertThat(rollingOrder.poll(), is("my-connect-connect-1"));
        assertThat(rollingOrder.poll(), is("my-connect-connect-0"));
        assertThat(rollingOrder.poll(), is("my-connect-connect-2"));
    }

    @Test
    public void testRollingOrderWithUnreadyPod()  {
        List<Pod> pods = List.of(
                renamePod(READY_POD, "my-connect-connect-0"),
                renamePod(UNREADY_POD, "my-connect-connect-1"),
                renamePod(READY_POD, "my-connect-connect-2")
        );

        KafkaConnectRoller roller = new KafkaConnectRoller(RECONCILIATION, CLUSTER, 1_000L, null);
        Queue<String> rollingOrder = roller.prepareRollingOrder(POD_NAMES, pods);

        assertThat(rollingOrder.size(), is(3));
        assertThat(rollingOrder.poll(), is("my-connect-connect-1"));
        assertThat(rollingOrder.poll(), is("my-connect-connect-0"));
        assertThat(rollingOrder.poll(), is("my-connect-connect-2"));
    }

    @Test
    public void testRollingOrderWithUnreadyAndMissingPod()  {
        List<Pod> pods = List.of(
                renamePod(READY_POD, "my-connect-connect-0"),
                renamePod(UNREADY_POD, "my-connect-connect-1")
        );

        KafkaConnectRoller roller = new KafkaConnectRoller(RECONCILIATION, CLUSTER, 1_000L, null);
        Queue<String> rollingOrder = roller.prepareRollingOrder(POD_NAMES, pods);

        assertThat(rollingOrder.size(), is(3));
        assertThat(rollingOrder.poll(), is("my-connect-connect-2"));
        assertThat(rollingOrder.poll(), is("my-connect-connect-1"));
        assertThat(rollingOrder.poll(), is("my-connect-connect-0"));
    }

    @Test
    public void testMaybeRollPodNoChange(VertxTestContext context)  {
        StrimziPodSet podSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName("my-connect-connect")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podToMap(READY_POD))
                .endSpec()
                .build();

        PodOperator mockPodOps = mock(PodOperator.class);
        when(mockPodOps.getAsync(eq(NAMESPACE), eq("my-connect-connect-0"))).thenReturn(Future.succeededFuture(READY_POD));
        when(mockPodOps.readiness(any(), eq(NAMESPACE), eq("my-connect-connect-0"), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        KafkaConnectRoller roller = new KafkaConnectRoller(RECONCILIATION, CLUSTER, 1_000L, mockPodOps);

        Checkpoint async = context.checkpoint();
        roller.maybeRollPod(pod -> KafkaConnectRoller.needsRollingRestart(podSet, pod), "my-connect-connect-0")
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockPodOps, never()).deleteAsync(any(), any(), any(), anyBoolean());

                    async.flag();
                })));
    }

    @Test
    public void testMaybeRollPodMissingPod(VertxTestContext context)  {
        StrimziPodSet podSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName("my-connect-connect")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podToMap(READY_POD))
                .endSpec()
                .build();

        PodOperator mockPodOps = mock(PodOperator.class);
        when(mockPodOps.getAsync(eq(NAMESPACE), eq("my-connect-connect-0"))).thenReturn(Future.succeededFuture(null));
        when(mockPodOps.readiness(any(), eq(NAMESPACE), eq("my-connect-connect-0"), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        KafkaConnectRoller roller = new KafkaConnectRoller(RECONCILIATION, CLUSTER, 1_000L, mockPodOps);

        Checkpoint async = context.checkpoint();
        roller.maybeRollPod(pod -> KafkaConnectRoller.needsRollingRestart(podSet, pod), "my-connect-connect-0")
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockPodOps, never()).deleteAsync(any(), any(), any(), anyBoolean());

                    async.flag();
                })));
    }

    @Test
    public void testMaybeRollPodNeedRolling(VertxTestContext context)  {
        StrimziPodSet podSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName("my-connect-connect")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podToMap(READY_POD))
                .endSpec()
                .build();

        PodOperator mockPodOps = mock(PodOperator.class);
        when(mockPodOps.getAsync(eq(NAMESPACE), eq("my-connect-connect-0"))).thenReturn(Future.succeededFuture(changeRevision(READY_POD, "skso1919")));
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), eq("my-connect-connect-0"), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), eq("my-connect-connect-0"), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        KafkaConnectRoller roller = new KafkaConnectRoller(RECONCILIATION, CLUSTER, 1_000L, mockPodOps);

        Checkpoint async = context.checkpoint();
        roller.maybeRollPod(pod -> KafkaConnectRoller.needsRollingRestart(podSet, pod), "my-connect-connect-0")
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockPodOps, times(1)).deleteAsync(any(), eq(NAMESPACE), eq("my-connect-connect-0"), eq(false));

                    async.flag();
                })));
    }

    @Test
    public void testMaybeRollPodFailsWhenNotReady(VertxTestContext context)  {
        StrimziPodSet podSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName("my-connect-connect")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podToMap(READY_POD))
                .endSpec()
                .build();

        PodOperator mockPodOps = mock(PodOperator.class);
        when(mockPodOps.getAsync(eq(NAMESPACE), eq("my-connect-connect-0"))).thenReturn(Future.succeededFuture(null));
        when(mockPodOps.readiness(any(), eq(NAMESPACE), eq("my-connect-connect-0"), anyLong(), anyLong())).thenReturn(Future.failedFuture(new RuntimeException("Timeout")));

        KafkaConnectRoller roller = new KafkaConnectRoller(RECONCILIATION, CLUSTER, 1_000L, mockPodOps);

        Checkpoint async = context.checkpoint();
        roller.maybeRollPod(pod -> KafkaConnectRoller.needsRollingRestart(podSet, pod), "my-connect-connect-0")
                .onComplete(context.failing(v -> context.verify(() -> {
                    verify(mockPodOps, never()).deleteAsync(any(), any(), any(), anyBoolean());

                    assertThat(v.getMessage(), is("Timeout"));

                    async.flag();
                })));
    }

    @Test
    public void testMaybeRollInOrder(VertxTestContext context)  {
        StrimziPodSet podSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName("my-connect-connect")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podToMap(renamePod(READY_POD, "my-connect-connect-0")),
                            PodSetUtils.podToMap(renamePod(READY_POD, "my-connect-connect-1")),
                            PodSetUtils.podToMap(renamePod(READY_POD, "my-connect-connect-2")))
                .endSpec()
                .build();

        PodOperator mockPodOps = mock(PodOperator.class);

        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(renamePod(READY_POD, "my-connect-connect-0"), renamePod(READY_POD, "my-connect-connect-2"))));

        ArgumentCaptor<String> getAsyncCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPodOps.getAsync(eq(NAMESPACE), getAsyncCaptor.capture())).thenAnswer(i -> {
            if ("my-connect-connect-1".equals(i.getArgument(1)))    {
                return Future.succeededFuture(null);
            } else {
                return Future.succeededFuture(renamePod(READY_POD, i.getArgument(1)));
            }
        });

        ArgumentCaptor<String> readinessCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPodOps.readiness(any(), eq(NAMESPACE), readinessCaptor.capture(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        KafkaConnectRoller roller = new KafkaConnectRoller(RECONCILIATION, CLUSTER, 1_000L, mockPodOps);

        Checkpoint async = context.checkpoint();
        roller.maybeRoll(POD_NAMES, pod -> KafkaConnectRoller.needsRollingRestart(podSet, pod))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockPodOps, never()).deleteAsync(any(), any(), any(), anyBoolean());

                    List<String> getAsync = getAsyncCaptor.getAllValues();
                    assertThat(getAsync.size(), is(3));
                    assertThat(getAsync.get(0), is("my-connect-connect-1"));
                    assertThat(getAsync.get(1), is("my-connect-connect-0"));
                    assertThat(getAsync.get(2), is("my-connect-connect-2"));

                    List<String> readiness = readinessCaptor.getAllValues();
                    assertThat(readiness.size(), is(3));
                    assertThat(readiness.get(0), is("my-connect-connect-1"));
                    assertThat(readiness.get(1), is("my-connect-connect-0"));
                    assertThat(readiness.get(2), is("my-connect-connect-2"));

                    async.flag();
                })));
    }

    @Test
    public void testMaybeRollNotReady(VertxTestContext context)  {
        StrimziPodSet podSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName("my-connect-connect")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podToMap(renamePod(READY_POD, "my-connect-connect-0")),
                            PodSetUtils.podToMap(renamePod(READY_POD, "my-connect-connect-1")),
                            PodSetUtils.podToMap(renamePod(READY_POD, "my-connect-connect-2")))
                .endSpec()
                .build();

        PodOperator mockPodOps = mock(PodOperator.class);

        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(renamePod(READY_POD, "my-connect-connect-0"), renamePod(READY_POD, "my-connect-connect-2"))));

        when(mockPodOps.getAsync(eq(NAMESPACE), any())).thenAnswer(i -> Future.succeededFuture(renamePod(READY_POD, i.getArgument(1))));

        when(mockPodOps.readiness(any(), eq(NAMESPACE), any(), anyLong(), anyLong())).thenReturn(Future.failedFuture(new RuntimeException("Timeout")));

        KafkaConnectRoller roller = new KafkaConnectRoller(RECONCILIATION, CLUSTER, 1_000L, mockPodOps);

        Checkpoint async = context.checkpoint();
        roller.maybeRoll(POD_NAMES, pod -> KafkaConnectRoller.needsRollingRestart(podSet, pod))
                .onComplete(context.failing(v -> context.verify(() -> {
                    assertThat(v.getMessage(), is("Timeout"));
                    async.flag();
                })));
    }

    @Test
    public void testRegularRollingUpdate()   {
        Pod pod1 = new PodBuilder()
                .withNewMetadata()
                    .withName("name")
                    .withAnnotations(Map.of(PodRevision.STRIMZI_REVISION_ANNOTATION, "avfc1874"))
                .endMetadata()
                .build();

        Pod pod2 = new PodBuilder()
                .withNewMetadata()
                    .withName("name")
                    .withAnnotations(Map.of(PodRevision.STRIMZI_REVISION_ANNOTATION, "skso1919"))
                .endMetadata()
                .build();

        StrimziPodSet podSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName("name")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podToMap(pod1))
                .endSpec()
                .build();

        assertThat(KafkaConnectRoller.needsRollingRestart(podSet, pod1).shouldRestart(), is(false));
        assertThat(KafkaConnectRoller.needsRollingRestart(podSet, pod2).shouldRestart(), is(true));
    }

    /**
     * Utility method for easily renaming pods
     *
     * @param pod   Pod
     * @param name  New name
     *
     * @return  Renamed pod
     */
    private static Pod renamePod(Pod pod, String name) {
        return new PodBuilder(pod)
                .editMetadata()
                    .withName(name)
                .endMetadata()
                .build();
    }

    /**
     * Utility method for easily changing pods revision
     *
     * @param pod   Pod
     * @param revision  New revision
     *
     * @return  Renamed pod
     */
    private static Pod changeRevision(Pod pod, String revision) {
        return new PodBuilder(pod)
                .editMetadata()
                    .withAnnotations(Map.of(PodRevision.STRIMZI_REVISION_ANNOTATION, revision))
                .endMetadata()
                .build();
    }
}
