/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.internal.readiness.Readiness;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.common.config.ConfigResource;

import static io.strimzi.operator.cluster.operator.resource.BrokerActionContext.State.AWAITING_LEADERSHIP;
import static io.strimzi.operator.cluster.operator.resource.BrokerActionContext.State.AWAITING_READY;
import static io.strimzi.operator.cluster.operator.resource.BrokerActionContext.State.CONTROLLER;
import static io.strimzi.operator.cluster.operator.resource.BrokerActionContext.State.DONE;
import static io.strimzi.operator.cluster.operator.resource.BrokerActionContext.State.NEEDS_CLASSIFY;
import static io.strimzi.operator.cluster.operator.resource.BrokerActionContext.State.NEEDS_RECONFIG;
import static io.strimzi.operator.cluster.operator.resource.BrokerActionContext.State.NEEDS_RESTART;
import static io.strimzi.operator.cluster.operator.resource.BrokerActionContext.State.READY;
import static io.strimzi.operator.cluster.operator.resource.BrokerActionContext.State.START;
import static io.strimzi.operator.cluster.operator.resource.BrokerActionContext.State.UNREADY;
import static io.strimzi.operator.cluster.operator.resource.BrokerActionContext.State.UNSCHEDULABLE;

/**
 * <p>Implements a state machine for performing actions on an individual broker.</p>
 * <p>State machine states are defined by {@link State}, with transitions as follows:</p>
 * <code><pre>
 *                 START
 *                   ↓
 *                UNKNOWN
 *                   ↕
 * any of (UNREADY, READY, CONTROLLER) which can all inter-transition
 *               ↙       ↘                          ↓
 * NEEDS_RECONFIG    →    NEEDS_RESTART       UNSCHEDULABLE
 *               ↘       ↙
 *            AWAITING_READY
 *                   ↓
 *          AWAITING_LEADERSHIP
 *                   ↓
 *                 DONE
 * </pre></code>
 * <p>In addition self transitions are allowed in all states except START, DONE</p>
 * <p>State transitions are managed by {@link BrokerActionContext#makeTransition(State, long)}.</p>
 */
class BrokerActionContext {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(BrokerActionContext.class);

    static void assertEventLoop(Vertx vertx) {
        // This doesn't work, see javadoc of isEventLoopContext
        if (!vertx.getOrCreateContext().isEventLoopContext()) {
            throw new IllegalStateException("Not executing on event loop thread");
        }
    }

    enum State {
        // NOTE: The order of these declarations matters, because it's used for the ordering of the priority queue
        // in KafkaRoller to guarantee things like UNREADY pods always being restarted first.
        /** The start state. */
        START,
        /** The pod is unschedulable (a final, but unsuccessful) state*/
        UNSCHEDULABLE,
        /** Waiting to observe leadership following an action */
        AWAITING_LEADERSHIP,
        /** Waiting to observe readiness following an action */
        AWAITING_READY,
        /** A pod needs restart */
        NEEDS_RESTART,
        /** A pod needs reconfig (may transition to NEEDS_RESTART if the reconfig fails) */
        NEEDS_RECONFIG,
        /** A pod is unready (and should be rolled in the first batch) */
        UNREADY,
        /** An unknown state (e.g. due to an error during {@link BrokerActionContext#classify()}, or it not having been called yet). */
        NEEDS_CLASSIFY,
        /** A pod is ready (and should be rolled in the 2nd batch) */
        READY,
        /** A pod is the controller (and should be rolled in the final singleton batch) */
        CONTROLLER,
        /** The final successful state */
        DONE;
        void assertIsOneOf(State... states) {
            List<State> list = Arrays.asList(states);
            if (!list.contains(this)) {
                throw new IllegalStateException("Should be in one of states " + list + " but in state " + this);
            }
        }
        public boolean isEndState() {
            return this == DONE || this == UNSCHEDULABLE;
        }
    }

    /** Immutable class for passing between threads */
    static class Transition {
        private final State state;
        private final long delayMs;

        public Transition(State state, long delayMs) {
            this.state = state;
            this.delayMs = delayMs;
        }
    }

    // final state
    private final static long BASE_DELAY_MS = 2048;
    private final int podId;
    private final String podName;
    private final Reconciliation reconciliation;
    private final String kafkaLogging;
    private final String kafkaConfig;
    private final KafkaVersion kafkaVersion;
    private final Vertx vertx;
    private final PodOperator podOperations;
    private final long operationTimeoutMs;
    private final Function<Pod, List<String>> podNeedsRestart;
    private final boolean allowReconfiguration;
    private final KafkaAvailability kafkaAvailability;
    // TODO the following should be encapsulated (context shouldn't know about parallelism, only roller should)
    private final Set<Integer> restartingBrokers;
    private final int parallelism = 2;

    // Mutable state accessed on multiple threads
    private volatile Pod pod;
    /** The time wrt epoch after which we can consider an action */
    private volatile long notBefore;

    // Mutable state accessed only on event loop thread
    /** The current state */
    private State state;
    /** The number of self transitions while in this state */
    private int numSelfTransitions;
    /** #ms since epoch that we've been in the current state */
    private long stateEpoch;
    /** Assigned in #buildPlan() */
    /* test */ KafkaBrokerConfigurationDiff configDiff;
    /** Assigned in #buildPlan() */
    /* test */ KafkaBrokerLoggingConfigurationDiff loggersDiff;
    //        private int numReconfigsLeft = 3;
    private int numRetriesLeft = 3;
    //        private Future<Boolean> canRoll;

    private long actualDelayMs;
    private Future<Map<ConfigResource, Throwable>> alterConfigResult;

    public BrokerActionContext(Vertx vertx,
                               PodOperator podOperations,
                               long operationTimeoutMs,
                               boolean allowReconfiguration,
                               Reconciliation reconciliation,
                               int podId,
                               String podName,
                               KafkaVersion kafkaVersion,
                               String kafkaConfig,
                               String kafkaLogging,
                               Function<Pod, List<String>> podNeedsRestart,
                               KafkaAvailability kafkaAvailability,
                               Set<Integer> restartingBrokers) {
        this(NEEDS_CLASSIFY, vertx, podOperations, operationTimeoutMs, allowReconfiguration, reconciliation, podId,
                podName, kafkaVersion, kafkaConfig, kafkaLogging, podNeedsRestart, kafkaAvailability, restartingBrokers);
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    BrokerActionContext(State initialState,
                        Vertx vertx,
                        PodOperator podOperations,
                        long operationTimeoutMs,
                        boolean allowReconfiguration,
                        Reconciliation reconciliation,
                        int podId,
                        String podName,
                        KafkaVersion kafkaVersion,
                        String kafkaConfig,
                        String kafkaLogging,
                        Function<Pod, List<String>> podNeedsRestart,
                        KafkaAvailability kafkaAvailability,
                        Set<Integer> restartingBrokers) {
        assertEventLoop(vertx);
        this.vertx = vertx;
        this.podOperations = podOperations;
        this.operationTimeoutMs = operationTimeoutMs;
        this.allowReconfiguration = allowReconfiguration;
        this.reconciliation = reconciliation;
        this.podId = podId;
        this.podName = podName;
        this.kafkaVersion = kafkaVersion;
        this.kafkaConfig = kafkaConfig;
        this.kafkaLogging = kafkaLogging;
        this.podNeedsRestart = podNeedsRestart;
        this.kafkaAvailability = kafkaAvailability;
        this.restartingBrokers = restartingBrokers;
        this.state = initialState;
        this.actualDelayMs = 0;
        this.stateEpoch = this.notBefore = System.currentTimeMillis();
    }

    public State state() {
        return state;
    }

    public int numSelfTransitions() {
        return numSelfTransitions;
    }

    public long notBefore() {
        return notBefore;
    }

    public int podId() {
        return podId;
    }

    public long actualDelayMs() {
        return actualDelayMs;
    }

    @Override
    public String toString() {
        return "BrokerActionContext(" +
                "podId=" + podId +
                ", state=" + state +
                ", notBefore=" + Instant.ofEpochMilli(notBefore) +
                ", numRetriesLeft=" + numRetriesLeft +
                ')';
    }

    // Outstanding TODO list:
    // * Finish writing classify() and decide how to handle the UNKNOWN state
    // * Error handling
    //      Can we use self-transition to represent an error and count the number of self-transitions
    //      This will make it harder to do wait until semantics though.
    //      Which themselves come with a need for a timeout.
    //      And we need to prevent timeout explosion
    // * Blocking (await)

    /**
     * Try to perform a single state transition.
     *
     * @return a future which completes on the event loop context when the transition is complete
     */
    Future<Void> progressOne() {
        assertEventLoop(vertx);
        var state = this.state;
        LOGGER.debugCr(reconciliation, "Making one transition of {}", this);
        switch (state) {
            case START:
            case NEEDS_CLASSIFY:
                return classify();
            case UNREADY:
                makeTransition(NEEDS_RESTART, 0);
                return Future.succeededFuture();
            case READY:
            case CONTROLLER:
                return buildPlan();
            case NEEDS_RECONFIG:
                return dynamicUpdateBrokerConfig();
            case NEEDS_RESTART:
                return maybeRestart();
            case AWAITING_READY:
                return checkForReadiness();
            case AWAITING_LEADERSHIP:
                return checkForLeadership();
            case DONE:
                return Future.succeededFuture();
            default:
                return Future.failedFuture(new IllegalStateException(String.valueOf(state)));
        }
    }

    //        private volatile KafkaFuture<Map<TopicPartition, Optional<Throwable>>> electLeaders = null;
//
//        private Transition electPreferredLeader(Set<TopicPartition> partitions) {
//            if (electLeaders == null) {
//                this.electLeaders = adminClient().electLeaders(ElectionType.PREFERRED, partitions).partitions();
//            }
//            if (electLeaders.isDone()) {
//                try {
//                    electLeaders.getNow(null).forEach((key, errOption) -> {
//                        if (errOption.isPresent()) {
//                            Throwable cause = errOption.get();
//                            LOGGER.traceCr(reconciliation, "{}: Failed to elect preferred leader {} for partition {} due to {}", reconciliation, podId, key, cause);
//                        }
//                    });
//                    electLeaders = null;
//                    return new Transition(AWAITING_LEADERSHIP, 20_000);
//                } catch (ExecutionException | InterruptedException e) {
//                    // note: getNow() does not actually throw InterruptedException, it's a mistake in the Kafka API.
//                    electLeaders = null;
//                    return new Transition(AWAITING_LEADERSHIP, 20_000);
//                }
//            } else {
//                return new Transition(AWAITING_LEADERSHIP, 20_000);
//            }
//
//        }

    private State makeTransition(Transition transition) {
        LOGGER.debugCr(reconciliation, "Transition {} to {} with delay {}ms", this, transition.state, transition.delayMs);
        return makeTransition(transition.state, transition.delayMs);
    }

    /**
     * Transition to a new state.
     * Transitions which would cause multiple restarts are not allowed.
     */
    public State makeTransition(State newState, long delay) {
        assertEventLoop(vertx);
        // Prevent restarting any pod more than once
        // TODO reinstate this check!
//        if (this.state.ordinal() >= NEEDS_RESTART.ordinal()
//                && newState.ordinal() <= NEEDS_RESTART.ordinal()) {
//            throw new IllegalStateException("Illegal state transition " + this.state + " -> " + newState);
//        }
        if (this.state == newState) {
            this.actualDelayMs *= 2;
            this.numSelfTransitions++;
        } else {
            this.actualDelayMs = delay;
            this.numSelfTransitions = 0;
            this.stateEpoch = System.currentTimeMillis();
        }
        this.notBefore = System.currentTimeMillis() + actualDelayMs;
        this.state = newState;
        return state;
    }

    /**
     * Determine whether this broker is READY, UNREADY or CONTROLLER
     */
    Future<Void> classify() {
        assertEventLoop(vertx);
        state.assertIsOneOf(START, NEEDS_CLASSIFY, READY, UNREADY, CONTROLLER);
        return vertx.<Transition>executeBlocking(p -> {
            Transition transition = null;
            try {
                LOGGER.debugCr(reconciliation, "Get pod for broker {}", podId);
                this.pod = podOperations.get(reconciliation.namespace(), podName);
                if (isUnschedulable(pod)) {
                    LOGGER.debugCr(reconciliation, "Pod {} is unschedulable", podId);
                    transition = new Transition(UNSCHEDULABLE, 0);
                } else if (Readiness.isPodReady(pod)) {
                    LOGGER.debugCr(reconciliation, "Broker {} is ready. Determine whether it is in log recovery", podId);
                    if (isInLogRecovery()) {
                        LOGGER.debugCr(reconciliation, "Broker {} is in log recovery", podId);
                        transition = new Transition(NEEDS_CLASSIFY, 10_000); // retry in a while
                    } else {
                        LOGGER.debugCr(reconciliation, "Broker {} is not in log recovery", podId);
                        transition = new Transition(READY, 0);
                    }
                }
                p.complete(transition);
            } catch (Exception e) {
                p.tryFail(e);
            }
        }).compose(transition -> {
            if (transition == null) { // not ready
                return Future.succeededFuture(new Transition(UNREADY, 0));
            } else {
                LOGGER.debugCr(reconciliation, "Determine whether broker {} is controller", podId);
                return kafkaAvailability.controller(podId).map(isController -> {
                    if (isController) {
                        LOGGER.debugCr(reconciliation, "Broker {} is controller", podId);
                        return new Transition(CONTROLLER, 0);
                    } else {
                        LOGGER.debugCr(reconciliation, "Broker {} is not controller", podId);
                        return transition;
                        // TODO would need the map(this::makeTransition) to handle the failure case.
                    }
                });
            }
        }).otherwise(error -> {
            LOGGER.debugCr(reconciliation, "Error classifying broker {}", podId, error);
            return new Transition(NEEDS_CLASSIFY, 10_000); // retry in a while
        }).map(this::makeTransition).map((Void) null);
    }

    boolean isInLogRecovery() {
        return false; // TODO implement this
    }

    Future<Void> buildPlan() {
        assertEventLoop(vertx);
        state.assertIsOneOf(READY, CONTROLLER);
        return podOperations.getAsync(reconciliation.namespace(), podName)
                .compose(pod -> {
                    assertEventLoop(vertx);
                    List<String> reasonToRestartPod = Objects.requireNonNull(podNeedsRestart.apply(pod));
                    boolean podStuck = pod != null && isUnschedulable(pod);
                    if (podStuck && !reasonToRestartPod.contains("Pod has old generation")) {
                        // If the pod is unschedulable then deleting it, or trying to open an Admin client to it will make no difference
                        // Treat this as fatal because if it's not possible to schedule one pod then it's likely that proceeding
                        // and deleting a different pod in the meantime will likely result in another unschedulable pod.
                        return Future.succeededFuture(new Transition(State.UNSCHEDULABLE, 0));
                    }
                    // Unless the annotation is present, check the pod is at least ready.
                    boolean needsRestart = !reasonToRestartPod.isEmpty();

                    if (needsRestart) {
                        LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted. Reason: {}", podId, reasonToRestartPod);
                        return Future.succeededFuture(new Transition(NEEDS_RESTART, 0));
                    } else {
                        // Always get the broker config. This request gets sent to that specific broker, so it's a proof that we can
                        // connect to the broker and that it's capable of responding.
                        return kafkaAvailability.brokerConfigDiffs(podId, kafkaVersion, kafkaConfig, kafkaLogging).map(diffResult -> {
                            this.configDiff = diffResult.configDiff;
                            this.loggersDiff = diffResult.loggersDiff;
                            if (configDiff.getDiffSize() > 0) {
                                if (configDiff.canBeUpdatedDynamically()) {
                                    LOGGER.debugCr(reconciliation, "Pod {} needs to be reconfigured.", podId);
                                    return new Transition(allowReconfiguration ? NEEDS_RECONFIG : NEEDS_RESTART, 0);
                                } else {
                                    LOGGER.debugCr(reconciliation, "Pod {} needs to be restarted, because reconfiguration cannot be done dynamically", podId);
                                    return new Transition(NEEDS_RESTART, 0);
                                }
                            } else if (loggersDiff.getDiffSize() > 0) {
                                LOGGER.debugCr(reconciliation, "Pod {} logging needs to be reconfigured.", podId);
                                return new Transition(allowReconfiguration ? NEEDS_RECONFIG : NEEDS_RESTART, 0);
                            } else {
                                return new Transition(AWAITING_READY, 0);
                            }
                        });
                    }
                }).otherwise(error -> {
                    LOGGER.errorCr(reconciliation, "Error classifying action on broker {}", podId);
                    return new Transition(NEEDS_CLASSIFY, 10_000);
                }).map(
                        this::makeTransition
                ).map((Void) null);
    }

    Future<Void> dynamicUpdateBrokerConfig() {
        assertEventLoop(vertx);
        state.assertIsOneOf(NEEDS_RECONFIG);
        if (loggersDiff == null || configDiff == null) {
            throw new IllegalStateException();
        }
        return vertx.<Transition>executeBlocking(p -> {
            Transition transition = null;
            try {
                if (this.alterConfigResult == null) {
                    Map<ConfigResource, Collection<AlterConfigOp>> updatedConfig = Map.of(
                            Util.brokerConfigResource(podId), configDiff.alterConfigOps(),
                            Util.brokerLoggersConfigResource(podId), loggersDiff.alterConfigOps());

                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.traceCr(reconciliation, "Altering broker configuration {} with {}", podId, updatedConfig);
                    } else {
                        LOGGER.debugCr(reconciliation, "Altering broker configuration {}", podId);
                    }
                    this.alterConfigResult = kafkaAvailability.alterConfigs(updatedConfig);
                }

                if (alterConfigResult.isComplete()) {
                    // Call get to find out any exception
                    if (alterConfigResult.result().get(Util.brokerConfigResource(podId)) != null ||
                            alterConfigResult.result().get(Util.brokerLoggersConfigResource(podId)) != null) {
                        this.alterConfigResult = null;
                        transition = new Transition(NEEDS_RESTART, 30_000);
                    } else {
                        LOGGER.infoCr(reconciliation, "Dynamic reconfiguration for broker {} was successful.", podId);
                        transition = new Transition(AWAITING_READY, 0);
                    }
                } else {
                    transition = new Transition(NEEDS_RESTART, 1_000);
                }
            } catch (Exception e) {
                transition = new Transition(NEEDS_RECONFIG, 1_000);
            }
            p.complete(transition);
        }).map(this::makeTransition).map((Void) null);
    }

    Future<Void> maybeRestart() {
        assertEventLoop(vertx);
        // TODO should the restart path revert to UNKNOWN, so we have to prove non-controllership again
        state.assertIsOneOf(NEEDS_RESTART);
        if (restartingBrokers.size() > parallelism) {
            makeTransition(NEEDS_RESTART, 10_000); // TODO have a future that is completed when restartingBrokers changes?
            return Future.succeededFuture();
        } else {
            Future<Transition> otherwise = kafkaAvailability.canRoll(podId, Collections.unmodifiableSet(restartingBrokers)).compose(canRoll -> {
                if (canRoll) {
                    assertEventLoop(vertx);
                    restartingBrokers.add(podId);
                    // No point in checking for readiness immediately
                    return podOperations.restart(reconciliation, pod, operationTimeoutMs).map(i -> new Transition(AWAITING_READY, 2_000));
                } else {
                    return Future.succeededFuture(new Transition(NEEDS_RESTART, 10_000));
                }
            }).otherwise(new Transition(NEEDS_RESTART, 10_000)
            );
            return otherwise.map(this::makeTransition).map((Void) null);
        }
    }

    Future<Void> checkForReadiness() {
        assertEventLoop(vertx);
        state.assertIsOneOf(AWAITING_READY);
        // TODO IS this how we want to handle readiness, where it doesn't happen via the state machine, but lower down?
        // It relinquishes control so makes checking for things like continued existence of the Kafka CR more difficult.
        return podOperations.readiness(reconciliation, reconciliation.namespace(), podName, 1_000, 120_000)
                .map(new Transition(AWAITING_LEADERSHIP, 0))
                .otherwise(new Transition(AWAITING_READY, 0))
                .map(this::makeTransition)
                .map((Void) null);
    }

    Future<Void> checkForLeadership() {
        assertEventLoop(vertx);
        state.assertIsOneOf(AWAITING_LEADERSHIP);

        return kafkaAvailability.partitionsWithPreferredButNotCurrentLeader(podId)
            .compose(partitions -> {
                assertEventLoop(vertx);
                if (partitions.isEmpty()) {
                    restartingBrokers.remove(podId);
                    return Future.succeededFuture(new Transition(DONE, 0));
                } else {
                    return kafkaAvailability.electPreferred(partitions)
                            .map(new Transition(AWAITING_READY, 0))
                            .otherwise(new Transition(AWAITING_READY, 10_000));
                }
            }).map(this::makeTransition)
            .map((Void) null);
    }

    private static boolean isPending(Pod pod) {
        return pod != null && pod.getStatus() != null && "Pending".equals(pod.getStatus().getPhase());
    }

    private static boolean isUnschedulable(Pod pod) {
        return isPending(pod)
                && pod.getStatus().getConditions().stream().anyMatch(ps ->
                "PodScheduled".equals(ps.getType())
                        && "Unschedulable".equals(ps.getReason())
                        && "False".equals(ps.getStatus()));
    }

    private static boolean isPodStuck(Pod pod) {
        Set<String> set = new HashSet<>();
        set.add("CrashLoopBackOff");
        set.add("ImagePullBackOff");
        set.add("ContainerCreating");
        return isPending(pod) || podWaitingBecauseOfAnyReasons(pod, set);
    }

    private static boolean podWaitingBecauseOfAnyReasons(Pod pod, Set<String> reasons) {
        if (pod != null && pod.getStatus() != null) {
            Optional<ContainerStatus> kafkaContainerStatus = pod.getStatus().getContainerStatuses().stream()
                    .filter(containerStatus -> containerStatus.getName().equals("kafka")).findFirst();
            if (kafkaContainerStatus.isPresent()) {
                ContainerStateWaiting waiting = kafkaContainerStatus.get().getState().getWaiting();
                if (waiting != null) {
                    return reasons.contains(waiting.getReason());
                }
            }
        }
        return false;
    }
}
