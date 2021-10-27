/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.BrokerActionContext.State;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

import static io.strimzi.operator.cluster.operator.resource.RollingTestUtils.await;
import static io.strimzi.operator.cluster.operator.resource.RollingTestUtils.awaitThrows;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaRollerTest {

    private static LinkedHashMap<Integer, State> actualStates(KafkaRoller kafkaRoller) {
        return kafkaRoller.queue().stream().collect(Collectors.toMap(BrokerActionContext::podId, BrokerActionContext::state, (x, y) -> x, LinkedHashMap::new));
    }

    private static LinkedHashMap<Integer, State> expectedStates(int p0, State st0, int p1, State st1, int p2, State st2) {
        var result = new LinkedHashMap<Integer, State>();
        result.put(p0, st0);
        result.put(p1, st1);
        result.put(p2, st2);
        return result;
    }

    public static final String NAMESPACE = Reconciliation.DUMMY_RECONCILIATION.namespace();
    public static final String CLUSTER = Reconciliation.DUMMY_RECONCILIATION.name();
    private static Vertx vertx;
    private final KafkaVersion latestVersion = KafkaVersionTestUtils.getLatestVersion();

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    static class Sum {
        long total = 0;
        public long add(long t0) {
            total += t0;
            return t0;
        }
    }

    @Test
    public void testHappyPathNoop() {
        Function<Pod, List<String>> podNeedsRestart = pod -> List.of();
        doHappyPath(brokerId -> false, podNeedsRestart);
    }

    @Test
    public void testHappyPathReconfigureAll() {
        Function<Pod, List<String>> podNeedsRestart = pod -> List.of();
        doHappyPath(brokerId -> true, podNeedsRestart);
    }

    @Test
    public void testHappyPathReconfigure0() {
        Function<Pod, List<String>> podNeedsRestart = pod -> List.of();
        doHappyPath(brokerId -> brokerId == 0, podNeedsRestart);
    }

    @Test
    public void testHappyPathReconfigure1() {
        Function<Pod, List<String>> podNeedsRestart = pod -> List.of();
        doHappyPath(brokerId -> brokerId == 1, podNeedsRestart);
    }

    @Test
    public void testHappyPathReconfigure2() {
        Function<Pod, List<String>> podNeedsRestart = pod -> List.of();
        doHappyPath(brokerId -> brokerId == 2, podNeedsRestart);
    }

    @Test
    public void testHappyPathRestart0() {
        Function<Pod, List<String>> podNeedsRestart = pod -> pod.getMetadata().getName().endsWith("-0") ? List.of("reason") : List.of();
        doHappyPath(brokerId -> false, podNeedsRestart);
    }

    @Test
    public void testHappyPathRestart1() {
        Function<Pod, List<String>> podNeedsRestart = pod -> pod.getMetadata().getName().endsWith("-1") ? List.of("reason") : List.of();
        doHappyPath(brokerId -> false, podNeedsRestart);
    }

    @Test
    public void testHappyPathRestart2() {
        Function<Pod, List<String>> podNeedsRestart = pod -> pod.getMetadata().getName().endsWith("-2") ? List.of("reason") : List.of();
        doHappyPath(brokerId -> false, podNeedsRestart);
    }

    @Test
    public void testHappyPathRestartAll() {
        Function<Pod, List<String>> podNeedsRestart = pod -> List.of("reason");
        doHappyPath(brokerId -> false, podNeedsRestart);
    }

    @SuppressWarnings("checkstyle:MethodLength")
    private void doHappyPath(IntPredicate reconfigure, Function<Pod, List<String>> podNeedsRestart) {
        String kafkaConfig = "";
        String loggersConfig = "";

        var podOps = mock(PodOperator.class);
        KafkaAvailability ka = mock(KafkaAvailability.class);

        boolean reconfig0 = reconfigure.test(0);
        boolean reconfig1 = reconfigure.test(1);
        boolean reconfig2 = reconfigure.test(2);
        var pod0 = mockHappyPathPod(BrokerActionContextTest.pod(0, true, true), false, podOps, ka, reconfig0, kafkaConfig, loggersConfig);
        var pod1 = mockHappyPathPod(BrokerActionContextTest.pod(1, true, true), true, podOps, ka, reconfig1, kafkaConfig, loggersConfig);
        var pod2 = mockHappyPathPod(BrokerActionContextTest.pod(2, true, true), false, podOps, ka, reconfig2, kafkaConfig, loggersConfig);
        var pods = asList(pod0, pod1, pod2);
        boolean restart0 = !podNeedsRestart.apply(pod0).isEmpty();
        boolean restart1 = !podNeedsRestart.apply(pod1).isEmpty();
        boolean restart2 = !podNeedsRestart.apply(pod2).isEmpty();

        KafkaRoller kafkaRoller = new KafkaRoller(Reconciliation.DUMMY_RECONCILIATION,
                vertx,
                podOps,
                1L,
                NAMESPACE,
                CLUSTER,
                3,
                null,
                null,
                ka,
                kafkaConfig,
                loggersConfig,
                latestVersion,
                true,
                podNeedsRestart);

        // TODO an abstracted way to assert that the time between last classify and restart is 0?
        // (i.e. we're not deciding to restart based on stale observations)

        Sum totalTime = new Sum();
        // We need to classify all the pods first, because we want to deal with UNREADY once first
        // (to avoid taking out a READY pod on each reconciliation and eventually borking the cluster)
        await(kafkaRoller.buildQueue());
        assertEquals(expectedStates(
                        0, State.NEEDS_CLASSIFY,
                        1, State.NEEDS_CLASSIFY,
                        2, State.NEEDS_CLASSIFY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                0, State.READY,
                1, State.NEEDS_CLASSIFY,
                2, State.NEEDS_CLASSIFY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.READY,
                        1, State.CONTROLLER,
                        2, State.NEEDS_CLASSIFY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.READY,
                        1, State.CONTROLLER,
                        2, State.READY),
                actualStates(kafkaRoller));

        // Now expect broker 0 to be restarted
        if (restart0 || reconfig0) {
            assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
            assertEquals(expectedStates(
                            0, restart0 ? State.NEEDS_RESTART : State.NEEDS_RECONFIG,
                            1, State.CONTROLLER,
                            2, State.READY),
                    actualStates(kafkaRoller));
        }

        assertEquals(restart0 ? 2_000L :  0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.AWAITING_READY,
                        1, State.CONTROLLER,
                        2, State.READY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.AWAITING_LEADERSHIP,
                        1, State.CONTROLLER,
                        2, State.READY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.DONE,
                        1, State.CONTROLLER,
                        2, State.READY),
                actualStates(kafkaRoller));

        // Now expect broker 2 to be restarted
        if (restart2 || reconfig2) {
            assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));

            assertEquals(expectedStates(
                            0, State.DONE,
                            1, State.CONTROLLER,
                            2, restart2 ? State.NEEDS_RESTART : State.NEEDS_RECONFIG),
                    actualStates(kafkaRoller));
        }

        assertEquals(restart2 ? 2_000L : 0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.DONE,
                        1, State.CONTROLLER,
                        2, State.AWAITING_READY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.DONE,
                        1, State.CONTROLLER,
                        2, State.AWAITING_LEADERSHIP),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.DONE,
                        1, State.CONTROLLER,
                        2, State.DONE),
                actualStates(kafkaRoller));

        if (restart1 || reconfig1) {
            // Now expect broker 1 to be restarted
            assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
            assertEquals(expectedStates(
                            0, State.DONE,
                            1, restart1 ? State.NEEDS_RESTART : State.NEEDS_RECONFIG,
                            2, State.DONE),
                    actualStates(kafkaRoller));
        }

        assertEquals(restart1 ? 2_000L : 0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.DONE,
                        1, State.AWAITING_READY,
                        2, State.DONE),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.DONE,
                        1, State.AWAITING_LEADERSHIP,
                        2, State.DONE),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.DONE,
                        1, State.DONE,
                        2, State.DONE),
                actualStates(kafkaRoller));

        assertNull(await(kafkaRoller.progressOneContext()));
        //assertEquals(reconfigure ? 0L : 6_000L, totalTime.total);

        for (int brokerId = 0; brokerId < 3; brokerId++) {
            boolean restart = !podNeedsRestart.apply(pods.get(brokerId)).isEmpty();
            boolean reconfig = reconfigure.test(brokerId);
            verify(ka, times(!restart && reconfig ? 1 : 0)).alterConfigs(argThat(new AlterConfigsArgMatcher(brokerId)));
            verify(ka, times(!restart || reconfig ? 1 : 0)).brokerConfigDiffs(eq(brokerId), any(), any(), any());
        }
    }

    private Pod mockHappyPathPod(Pod pod,
                                 boolean isController,
                                 PodOperator mockPodOps,
                                 KafkaAvailability mockKa,
                                 boolean reconfigure,
                                 String kafkaConfig,
                                 String loggersConfig) {
        String name = pod.getMetadata().getName();
        int brokerId = Integer.parseInt(name.substring(name.lastIndexOf("-") + 1));
        when(mockPodOps.get(eq(NAMESPACE), eq(KafkaCluster.kafkaPodName(CLUSTER, brokerId)))).thenReturn(pod);
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaCluster.kafkaPodName(CLUSTER, brokerId)))).thenReturn(Future.succeededFuture(pod));
        when(mockPodOps.readiness(eq(Reconciliation.DUMMY_RECONCILIATION), eq(NAMESPACE), eq(KafkaCluster.kafkaPodName(CLUSTER, brokerId)), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPodOps.restart(eq(Reconciliation.DUMMY_RECONCILIATION), eq(pod), anyLong())).thenReturn(Future.succeededFuture());
        KafkaBrokerConfigurationDiff broker0ConfigDiff = mock(KafkaBrokerConfigurationDiff.class);
        when(broker0ConfigDiff.getDiffSize()).thenReturn(reconfigure ? 1 : 0);
        when(broker0ConfigDiff.canBeUpdatedDynamically()).thenReturn(reconfigure);
        when(broker0ConfigDiff.alterConfigOps()).thenReturn(List.of(new AlterConfigOp(new ConfigEntry("foo", "bar"), AlterConfigOp.OpType.SET)));
        KafkaBrokerLoggingConfigurationDiff broker0LoggersDiff = mock(KafkaBrokerLoggingConfigurationDiff.class);
        when(broker0LoggersDiff.getDiffSize()).thenReturn(reconfigure ? 1 : 0);
        when(broker0LoggersDiff.alterConfigOps()).thenReturn(List.of(new AlterConfigOp(new ConfigEntry("foo", "bar"), AlterConfigOp.OpType.SET)));
        when(mockKa.brokerConfigDiffs(eq(brokerId), eq(latestVersion), eq(kafkaConfig), eq(loggersConfig))).thenReturn(Future.succeededFuture(new KafkaAvailability.ConfigDiff(broker0ConfigDiff, broker0LoggersDiff)));
        when(mockKa.controller(eq(brokerId))).thenReturn(Future.succeededFuture(isController));
        when(mockKa.canRoll(eq(brokerId), any(Set.class))).thenReturn(Future.succeededFuture(true));
        when(mockKa.partitionsWithPreferredButNotCurrentLeader(eq(brokerId))).thenReturn(Future.succeededFuture(Set.of()));
        when(mockKa.alterConfigs(argThat(new AlterConfigsArgMatcher(brokerId)))).thenReturn(Future.succeededFuture(alterConfigsResult(brokerId)));

        return pod;
    }

    private Map<ConfigResource, Throwable> alterConfigsResult(int brokerId) {
        Map<ConfigResource, Throwable> result = new java.util.HashMap<>();
        result.put(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(brokerId)), null);
        result.put(new ConfigResource(ConfigResource.Type.BROKER_LOGGER, Integer.toString(brokerId)), null);
        return result;
    }

    private static class AlterConfigsArgMatcher implements ArgumentMatcher<Map<ConfigResource, Collection<AlterConfigOp>>> {
        private final int brokerId;

        public AlterConfigsArgMatcher(int brokerId) {
            this.brokerId = brokerId;
        }

        @Override
        public boolean matches(Map<ConfigResource, Collection<AlterConfigOp>> map) {
            return map != null && map.keySet().equals(Set.of(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(brokerId)),
                    new ConfigResource(ConfigResource.Type.BROKER_LOGGER, Integer.toString(brokerId))));
        }
    }

    @Test
    public void testUnschedulablePod() {
        var reconfigure = false;
        Function<Pod, List<String>> podNeedsRestart = pod -> List.of();
        String kafkaConfig = "";
        String loggersConfig = "";

        var podOps = mock(PodOperator.class);
        KafkaAvailability ka = mock(KafkaAvailability.class);

        var pod0 = mockHappyPathPod(BrokerActionContextTest.pod(0, true, true), false, podOps, ka, reconfigure, kafkaConfig, loggersConfig);
        var pod1 = mockHappyPathPod(BrokerActionContextTest.pod(1, true, true), true, podOps, ka, reconfigure, kafkaConfig, loggersConfig);
        var pod2 = mockHappyPathPod(BrokerActionContextTest.pod(2, false, false), false, podOps, ka, reconfigure, kafkaConfig, loggersConfig);
        var pods = asList(pod0, pod1, pod2);

        KafkaRoller kafkaRoller = new KafkaRoller(Reconciliation.DUMMY_RECONCILIATION,
                vertx,
                podOps,
                1L,
                NAMESPACE,
                CLUSTER,
                3,
                null,
                null,
                ka,
                kafkaConfig,
                loggersConfig,
                latestVersion,
                true,
                podNeedsRestart);

        // TODO an abstracted way to assert that the time between last classify and restart is 0?
        // (i.e. we're not deciding to restart based on stale observations)

        Sum totalTime = new Sum();
        // We need to classify all the pods first, because we want to deal with UNREADY once first
        // (to avoid taking out a READY pod on each reconciliation and eventually borking the cluster)
        await(kafkaRoller.buildQueue());
        assertEquals(expectedStates(
                        0, State.NEEDS_CLASSIFY,
                        1, State.NEEDS_CLASSIFY,
                        2, State.NEEDS_CLASSIFY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.READY,
                        1, State.NEEDS_CLASSIFY,
                        2, State.NEEDS_CLASSIFY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.READY,
                        1, State.CONTROLLER,
                        2, State.NEEDS_CLASSIFY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.READY,
                        1, State.CONTROLLER,
                        2, State.UNSCHEDULABLE),
                actualStates(kafkaRoller));

        var e = awaitThrows(kafkaRoller.progressOneContext());
        assertEquals("Unschedulable pod 2", e.getMessage());

    }

    @Test
    public void testTransientlyUnready() {
        var reconfigure = false;
        Function<Pod, List<String>> podNeedsRestart = pod -> List.of();
        String kafkaConfig = "";
        String loggersConfig = "";

        var podOps = mock(PodOperator.class);
        KafkaAvailability ka = mock(KafkaAvailability.class);

        var pod0 = mockHappyPathPod(BrokerActionContextTest.pod(0, true, true), false, podOps, ka, reconfigure, kafkaConfig, loggersConfig);
        var pod1 = mockHappyPathPod(BrokerActionContextTest.pod(1, true, true), true, podOps, ka, reconfigure, kafkaConfig, loggersConfig);
        var pod2 = mockHappyPathPod(BrokerActionContextTest.pod(2, true, false), false, podOps, ka, reconfigure, kafkaConfig, loggersConfig);
        var pods = asList(pod0, pod1, pod2);

        KafkaRoller kafkaRoller = new KafkaRoller(Reconciliation.DUMMY_RECONCILIATION,
                vertx,
                podOps,
                1L,
                NAMESPACE,
                CLUSTER,
                3,
                null,
                null,
                ka,
                kafkaConfig,
                loggersConfig,
                latestVersion,
                true,
                podNeedsRestart);

        // TODO an abstracted way to assert that the time between last classify and restart is 0?
        // (i.e. we're not deciding to restart based on stale observations)

        Sum totalTime = new Sum();
        // We need to classify all the pods first, because we want to deal with UNREADY once first
        // (to avoid taking out a READY pod on each reconciliation and eventually borking the cluster)
        await(kafkaRoller.buildQueue());
        assertEquals(expectedStates(
                        0, State.NEEDS_CLASSIFY,
                        1, State.NEEDS_CLASSIFY,
                        2, State.NEEDS_CLASSIFY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.READY,
                        1, State.NEEDS_CLASSIFY,
                        2, State.NEEDS_CLASSIFY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.READY,
                        1, State.CONTROLLER,
                        2, State.NEEDS_CLASSIFY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.READY,
                        1, State.CONTROLLER,
                        2, State.UNREADY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.READY,
                        1, State.CONTROLLER,
                        2, State.NEEDS_RESTART),
                actualStates(kafkaRoller));

        pod2.setStatus(BrokerActionContextTest.podStatus(true, false));

        assertEquals(2_000L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.READY,
                        1, State.CONTROLLER,
                        2, State.AWAITING_READY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.READY,
                        1, State.CONTROLLER,
                        2, State.AWAITING_READY),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.READY,
                        1, State.CONTROLLER,
                        2, State.AWAITING_READY),
                actualStates(kafkaRoller));

        pod2.setStatus(BrokerActionContextTest.podStatus(true, true));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.READY,
                        1, State.CONTROLLER,
                        2, State.AWAITING_LEADERSHIP),
                actualStates(kafkaRoller));

        assertEquals(0L, totalTime.add(await(kafkaRoller.progressOneContext())));
        assertEquals(expectedStates(
                        0, State.READY,
                        1, State.CONTROLLER,
                        2, State.DONE),
                actualStates(kafkaRoller));

        // TODO check that we classify broker 0 and broker 1 again
        var e = awaitThrows(kafkaRoller.progressOneContext());
        assertEquals("Unschedulable pod 2", e.getMessage());

    }


    // TODO Case: single transiently UNREADY broker
    // TODO Case: single persistently UNREADY broker
    // TODO Case: Unable to open an Admin client
    // TODO Case: Unable to get configs from one broker
    // TODO Case: Broker doesn't become READY after restart
    // TODO Case: Broker transiently doesn't regain leadership ofter restart
    // TODO Case: Broker persistently doesn't regain leadership ofter restart


    // TODO Case: ordering of queue with different context states
    // TODO: Scheduling of tasks (in different states). In particular where are the times defined and when is backoff exponential?
    // TODO: Parallelism
    // TODO: that cluster-level invariants aren't violated
    // TODO: threading
    // TODO: global behaviour around retrying and bounding the total time.

}
