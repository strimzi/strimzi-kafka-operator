package io.strimzi.operator.cluster.operator.resource;/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.Scope;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.operator.resource.BrokerActionContext.State;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

class BrokerActionContextTest {

    public static final String NAMESPACE = Reconciliation.DUMMY_RECONCILIATION.namespace();
    public static final int POD_ID = 0;
    public static final String POD_NAME = "test-kafka-0";
    private static Vertx vertx;

    @BeforeAll
    public static void startVertx() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void stopVertx() {
        vertx.close();
    }



    // TODO test repeated UNREADY has exponential backoff behaviour

    @Test
    public void inNeedsClassify_PodOpsError() {
        // TODO Case: KA throw AdminException
        PodOperator podOps = mock(PodOperator.class);
        when(podOps.get(eq(NAMESPACE), eq(POD_NAME))).thenThrow(new RuntimeException("Test ex"));
        KafkaAvailability ka = mock(KafkaAvailability.class);

        BrokerActionContext context = new BrokerActionContext(vertx,
                podOps,
                100,
                true,
                Reconciliation.DUMMY_RECONCILIATION,
                POD_ID,
                POD_NAME,
                KafkaVersionTestUtils.getLatestVersion(),
                "",
                "",
                pod -> List.of("just because"),
                ka,
                Set.of());

        assertEquals(State.NEEDS_CLASSIFY, context.state());
        assertEquals(0, context.numSelfTransitions());

        RollingTestUtils.await(context.progressOne());

        assertEquals(State.NEEDS_CLASSIFY, context.state());
        assertEquals(1, context.numSelfTransitions());
        verify(podOps, atLeastOnce()).get(eq(NAMESPACE), eq(POD_NAME));
        verifyZeroInteractions(ka);
    }

    // Test error in KA.controller

    private void transitionFromNeedsClassify(boolean podReady, boolean controller) {
        PodOperator podOps = mock(PodOperator.class);
        when(podOps.get(eq(NAMESPACE), eq(POD_NAME))).thenReturn(pod(POD_ID, true, podReady));
        KafkaAvailability ka = mock(KafkaAvailability.class);
        when(ka.controller(eq(POD_ID))).thenReturn(Future.succeededFuture(controller));

        BrokerActionContext context = new BrokerActionContext(vertx,
                podOps,
                100,
                true,
                Reconciliation.DUMMY_RECONCILIATION,
                POD_ID,
                POD_NAME,
                KafkaVersionTestUtils.getLatestVersion(),
                "",
                "",
                pod -> List.of("just because"),
                ka,
                Set.of());

        assertEquals(State.NEEDS_CLASSIFY, context.state());
        assertEquals(0, context.numSelfTransitions());

        RollingTestUtils.await(context.progressOne());

        assertEquals(!podReady ? State.UNREADY : controller ? State.CONTROLLER : State.READY, context.state());
        assertEquals(0, context.numSelfTransitions());
        verify(podOps, times(1)).get(eq(NAMESPACE), eq(POD_NAME));
        verify(ka, times(podReady ? 1 : 0)).controller(eq(POD_ID));
    }

    public static Pod pod(int id, boolean scheduled, boolean ready) {
        if (!scheduled && ready) {
            throw new IllegalArgumentException();
        }
        return new PodBuilder()
                .withNewMetadata()
                    .withNamespace(Reconciliation.DUMMY_RECONCILIATION.namespace())
                    .withName(KafkaCluster.kafkaPodName(Reconciliation.DUMMY_RECONCILIATION.name(), id))
                .endMetadata()
                .withStatus(podStatus(scheduled, ready))
            .build();
    }

    public static PodStatus podStatus(boolean scheduled, boolean ready) {
        return new PodStatusBuilder()
                .withPhase(scheduled && ready ? "Ready" : "Pending")
                .addNewCondition()
                    .withType("PodScheduled")
                    .withStatus(scheduled ? "True" : "False")
                    .withReason(scheduled ? null : "Unschedulable")
                .endCondition()
                    .addNewCondition()
                    .withType("Ready")
                    .withStatus(ready ? "True" : "False")
                .endCondition()
            .build();
    }

    @Test
    public void inNeedsClassify_podReadyAndNotController() {
        transitionFromNeedsClassify(true, false);
    }

    @Test
    public void inNeedsClassify_podReadyAndController() {
        transitionFromNeedsClassify(true, true);
    }

    @Test
    public void inNeedsClassify_podNotReady() {
        transitionFromNeedsClassify(false, false);
        transitionFromNeedsClassify(false, true);
    }

    private BrokerActionContext transitionFromReadyOrController(State initialState,
                                                                Future<Pod> podFuture,
                                                                List<String> restartReasons,
                                                                String config,
                                                                String loggers,
                                                                Future<KafkaAvailability.ConfigDiff> configDiff,
                                                                boolean allowsReconfig,
                                                                State expectedState) {
        // TODO Case: the no-op cases
        // TODO Case: PodOps timeout
        // TODO Case: PodOps error
        // TODO Case: KA throw AdminException
        PodOperator podOps = mock(PodOperator.class);
        when(podOps.getAsync(eq(NAMESPACE), eq(POD_NAME))).thenReturn(
                podFuture);
        KafkaAvailability ka = mock(KafkaAvailability.class);
        when(ka.brokerConfigDiffs(eq(POD_ID), any(), any(), any())).thenReturn(configDiff);

        BrokerActionContext context = new BrokerActionContext(
                initialState,
                vertx,
                podOps,
                100,
                allowsReconfig,
                Reconciliation.DUMMY_RECONCILIATION,
                POD_ID,
                POD_NAME,
                KafkaVersionTestUtils.getLatestVersion(),
                config,
                loggers,
                pod -> restartReasons,
                ka,
                Set.of());

        assertEquals(initialState, context.state());
        assertEquals(0, context.numSelfTransitions());

        RollingTestUtils.await(context.progressOne());

        assertEquals(expectedState, context.state());
        // TODO assertions on times
        return context;
    }

    public static Stream<State> readyOrController() {
        return Stream.of(State.READY, State.CONTROLLER);
    }

    private KafkaBrokerConfigurationDiff brokerConfigDiff(String config, String value, Scope scope) {
        ConfigModel myConfigModel = new ConfigModel();
        myConfigModel.setScope(scope);
        return new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                List.of(new AlterConfigOp(new ConfigEntry(config, value), AlterConfigOp.OpType.SET)),
                Map.of(config, myConfigModel));
    }

    private KafkaBrokerLoggingConfigurationDiff loggerDiff(String s) {
        return new KafkaBrokerLoggingConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                new Config(List.of(new ConfigEntry("com.example", "INFO"))),
                s);
    }

    private KafkaAvailability.ConfigDiff unreconfigurableBrokerDiff() {
        return new KafkaAvailability.ConfigDiff(
                brokerConfigDiff("my.config", "foo", Scope.READ_ONLY),
                loggerDiff("com.example=INFO"));
    }

    private KafkaAvailability.ConfigDiff reconfigurableBrokerDiff() {
        return new KafkaAvailability.ConfigDiff(
                brokerConfigDiff("my.config", "foo", Scope.PER_BROKER),
                loggerDiff("com.example=INFO"));
    }

    private KafkaAvailability.ConfigDiff loggerOnlyDiff() {
        return new KafkaAvailability.ConfigDiff(
                brokerConfigDiff("my.config", "foo", Scope.PER_BROKER),
                new KafkaBrokerLoggingConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                        new Config(List.of(new ConfigEntry("com.example", "INFO"))),
                        "com.example=DEBUG"));
    }

    private KafkaAvailability.ConfigDiff noopDiff() {
        return new KafkaAvailability.ConfigDiff(
                new KafkaBrokerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                        List.of(),
                        Map.of()),
                new KafkaBrokerLoggingConfigurationDiff(List.of()));
    }


    @ParameterizedTest
    @MethodSource("readyOrController")
    public void inReadyOrController_unschedulable(State initalState) {
        transitionFromReadyOrController(initalState,
                Future.succeededFuture(pod(POD_ID, false, false)),
                List.of(),
                "", "",
                Future.succeededFuture(loggerOnlyDiff()),
                true,
                State.UNSCHEDULABLE);
    }

    @ParameterizedTest
    @MethodSource("readyOrController")
    public void inReadyOrController_errorGettingPod(State initalState) {
        var context = transitionFromReadyOrController(initalState,
                Future.failedFuture(new RuntimeException("test error")),
                List.of(),
                "", "",
                Future.succeededFuture(loggerOnlyDiff()),
                true,
                State.NEEDS_CLASSIFY);
        assertEquals(10_000, context.actualDelayMs(), "Expect a delay before retrying");
    }

    @ParameterizedTest
    @MethodSource("readyOrController")
    public void inReadyOrController_errorGettingConfigs(State initalState) {
        var context = transitionFromReadyOrController(initalState,
                Future.succeededFuture(pod(POD_ID, true, true)),
                List.of(),
                "", "",
                Future.failedFuture(new AdminClientException("Something went wrong", null)),
                true,
                State.NEEDS_CLASSIFY);
        assertEquals(10_000, context.actualDelayMs(), "Expect a delay before retrying");
    }


    @ParameterizedTest
    @MethodSource("readyOrController")
    public void inReadyOrController_needsRestart(State initalState) {
        var context = transitionFromReadyOrController(initalState,
                Future.succeededFuture(pod(POD_ID, true, true)),
                List.of("some reason"),
                "", "",
                Future.succeededFuture(unreconfigurableBrokerDiff()),
                true,
                State.NEEDS_RESTART);
        assertEquals(0, context.actualDelayMs(), "Expect immediate transition");
    }

    @ParameterizedTest
    @MethodSource("readyOrController")
    public void inReadyOrController_needsBrokerReconfigAndAllowed(State initalState) {
        var context = transitionFromReadyOrController(initalState,
                Future.succeededFuture(pod(POD_ID, true, true)),
                List.of(),
                "", "",
                Future.succeededFuture(reconfigurableBrokerDiff()),
                true,
                State.NEEDS_RECONFIG);
        assertEquals(0, context.actualDelayMs(), "Expect immediate transition");
    }

    @ParameterizedTest
    @MethodSource("readyOrController")
    public void inReadyOrController_needsBrokerReconfigButNotAllowed(State initalState) {
        var context = transitionFromReadyOrController(initalState,
                Future.succeededFuture(pod(POD_ID, true, true)),
                List.of(),
                "", "",
                Future.succeededFuture(reconfigurableBrokerDiff()),
                false,
                State.NEEDS_RESTART);
        assertEquals(0, context.actualDelayMs(), "Expect immediate transition");
    }

    @ParameterizedTest
    @MethodSource("readyOrController")
    public void inReadyOrController_needsLoggersReconfigAndAllowed(State initalState) {
        var context = transitionFromReadyOrController(initalState,
                Future.succeededFuture(pod(POD_ID, true, true)),
                List.of(),
                "", "",
                Future.succeededFuture(loggerOnlyDiff()),
                true,
                State.NEEDS_RECONFIG);
        assertEquals(0, context.actualDelayMs(), "Expect immediate transition");
    }

    @ParameterizedTest
    @MethodSource("readyOrController")
    public void inReadyOrController_needsLoggersReconfigButNotAllowed(State initalState) {
        var context = transitionFromReadyOrController(initalState,
                Future.succeededFuture(pod(POD_ID, true, true)),
                List.of(),
                "", "",
                Future.succeededFuture(loggerOnlyDiff()),
                false,
                State.NEEDS_RESTART);
        assertEquals(0, context.actualDelayMs(), "Expect immediate transition");
    }

    @ParameterizedTest
    @MethodSource("readyOrController")
    public void inReadyOrController_noActionNeeded(State initalState) {
        var context = transitionFromReadyOrController(initalState,
                Future.succeededFuture(pod(POD_ID, true, true)),
                List.of(),
                "my.config=foo", "com.example=DEBUG",
                Future.succeededFuture(noopDiff()),
                false,
                State.AWAITING_READY);
        assertEquals(0, context.actualDelayMs(), "Expect immediate transition");
    }

    @Test
    public void inAwaitingRestart() {
        // TODO Case: cannot roll right now
        // TODO Case: blocked by parallelism
        // TODO Case: error throw by PodOps.restart
        PodOperator podOps = mock(PodOperator.class);
        when(podOps.restart(any(), any(), anyLong())).thenReturn(
                Future.succeededFuture());
        KafkaAvailability ka = mock(KafkaAvailability.class);
        when(ka.canRoll(eq(POD_ID), any())).thenReturn(Future.succeededFuture(true));

        Set<Integer> restartingBrokers = new HashSet<>();

        BrokerActionContext context = new BrokerActionContext(
                State.NEEDS_RESTART,
                vertx,
                podOps,
                100,
                false,
                Reconciliation.DUMMY_RECONCILIATION,
                POD_ID,
                POD_NAME,
                KafkaVersionTestUtils.getLatestVersion(),
                "my.config=foo",
                "com.example=DEBUG",
                pod -> List.of(),
                ka,
                restartingBrokers);

        assertEquals(State.NEEDS_RESTART, context.state());
        assertEquals(0, context.numSelfTransitions());

        RollingTestUtils.await(context.progressOne());

        assertEquals(State.AWAITING_READY, context.state());
        assertTrue(restartingBrokers.contains(POD_ID));
        // TODO assertions on times
        assertEquals(2_000, context.actualDelayMs(),
                "Expect a short delay, since kube can't recreate a pod that fast");
    }

    @Test
    public void inAwaitingReconfigure() {
        // TODO Case: cannot roll right now
        // TODO Case: blocked by parallelism
        // TODO Case: error throw by PodOps.restart
        PodOperator podOps = mock(PodOperator.class);
        when(podOps.restart(any(), any(), anyLong())).thenReturn(
                Future.succeededFuture());
        KafkaAvailability ka = mock(KafkaAvailability.class);
        Map<ConfigResource, Throwable> result = new java.util.HashMap<>();
        result.put(new ConfigResource(ConfigResource.Type.BROKER, "" + POD_ID), null);
        result.put(new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "" + POD_ID), null);
        when(ka.alterConfigs(any())).thenReturn(Future.succeededFuture(
                result));

        BrokerActionContext context = new BrokerActionContext(
                State.NEEDS_RECONFIG,
                vertx,
                podOps,
                100,
                false,
                Reconciliation.DUMMY_RECONCILIATION,
                POD_ID,
                POD_NAME,
                KafkaVersionTestUtils.getLatestVersion(),
                "my.config=foo",
                "com.example=DEBUG",
                pod -> List.of(),
                ka,
                Set.of()); // TODO it is right that we could reconfigure many brokers at the same time??
        context.configDiff = noopDiff().configDiff;
        context.loggersDiff = loggerOnlyDiff().loggersDiff;

        assertEquals(State.NEEDS_RECONFIG, context.state());
        assertEquals(0, context.numSelfTransitions());

        RollingTestUtils.await(context.progressOne());

        assertEquals(State.AWAITING_READY, context.state());

        // TODO we need a better post-condition after making a reconfig
        assertEquals(0, context.actualDelayMs(),
                "Expect a short delay, since kube can't recreate a pod that fast");
    }
    // TODO AWAITING_READY -> AWAITING_LEADERSHIP
    // TODO election of preferred leaders (AWAITING_LEADERSHIP -> DONE)
    // TODO behaviour around log recovery

}