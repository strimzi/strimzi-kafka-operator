/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.internal.readiness.Readiness;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

import static io.strimzi.operator.cluster.operator.resource.KafkaRoller.RestartState.AWAITING_LEADERSHIP;
import static io.strimzi.operator.cluster.operator.resource.KafkaRoller.RestartState.AWAITING_READY;
import static io.strimzi.operator.cluster.operator.resource.KafkaRoller.RestartState.CONTROLLER;
import static io.strimzi.operator.cluster.operator.resource.KafkaRoller.RestartState.DONE;
import static io.strimzi.operator.cluster.operator.resource.KafkaRoller.RestartState.NEEDS_CLASSIFY;
import static io.strimzi.operator.cluster.operator.resource.KafkaRoller.RestartState.NEEDS_RECONFIG;
import static io.strimzi.operator.cluster.operator.resource.KafkaRoller.RestartState.NEEDS_RESTART;
import static io.strimzi.operator.cluster.operator.resource.KafkaRoller.RestartState.READY;
import static io.strimzi.operator.cluster.operator.resource.KafkaRoller.RestartState.START;
import static io.strimzi.operator.cluster.operator.resource.KafkaRoller.RestartState.UNREADY;

/**
 * <p>Manages the rolling restart or reconfiguration of a Kafka cluster.</p>
 *
 * <p>All the brokers in the cluster are considered for an action (restart or reconfigure).
 * (The reason we always consider <em>all</em> brokers is so that the regular reconciliation of a cluster done
 * by the CO means we maybe be able to resolve spontaneous problems in the cluster by restarting.)
 * <p>The brokers are processed in three batches:</p>
 * <ul>
 *     <li>Brokers that are either not {@code Ready} or do not appear to be up and are not in log recovery
 *     will be handled first.
 *     This is to avoid repeated reconciliations each taking out one broker until the whole cluster is
 *     borked.</li>
 *
 *     <li>Brokers that are not the controller are handled next.</li>
 *
 *     <li>Finally the controller is handled. This happens last to avoid the need for multiple controller
 *     elections.</li>
 * </ul>
 * <p>Since all these states are transient (and rolling actions can take a long time) the algorithm cannot
 * partition the brokers initially and then work on each batch sequentially.
 * Rather it considers all brokers that haven't yet been actioned and based on their
 * <em>current</em> state either actions them now, or defers them for reconsideration in the future.</p>
 *
 * <h3>Reconfigurations</h3>
 * <p>Where possible reconfiguration of a broker is favoured over restart, because it affects cluster and client state
 * much less. If the only reason to for an action is to reconfigure a broker we inspect the current and desired broker
 * and logger configs to determine whether they can be performed dynamically.</p>
 *
 * <p>Reconfiguration is done via the Admin client.</p>
 *
 * <h4>Reconfiguration post-conditions</h4>
 * // TODO What are they? How long should we wait for deleterious effects to be observable and how would they be observed
 * // canary?
 *
 * <h3>Restarts</h3>
 * <p>Restart is done by deleting the pod on kubernetes. As such the broker process is sent an initial SIGTERM
 * which begins graceful shutdown. If it has not shutdown within the terminationGracePeriod it is sent a SIGKILL,
 * likely resulting in log files not being closed cleaning and thus incurring log recovery on start up.</p>
 *
 * <h4>Restart pre-conditions</h4>
 * <ul>
 *     <li>the restart must not result in any topics replicated on that broker from being under-replicated.</li>
 *     <li>the broker must not be in log recovery</li>
 * </ul>
 * <p>If any of these preconditions are violated restart is deferred.</p>
 *
 * <h4>Restart post-conditions</h4>
 * <ul>
 *     <li>the pod must become {@code Ready}</li>
 *     <li>the pod must become leader for all the partitions that is was leading before the restart (or, if that wasn't know, all the partitions</li>
 *     TODO figure out exactly what we want here.
 * </ul>
 *
 * // TODO parallelism?? Also vertx?
 * The restart or reconfiguration of a individual broker is managed by a state machine whose states
 * are given by {@link RestartState}.
 * {@link RestartContext#makeTransition(RestartState, long)} this guarantees that each broker can only be restarted once.
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ParameterNumber"})
public class KafkaRoller {

    static class UnforceableProblem extends RuntimeException {
        // TODO delete me
    }

    static class FatalProblem extends RuntimeException {
        // TODO delete me
    }

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaRoller.class);

    private final PodOperator podOperations;
    protected final long operationTimeoutMs;
    protected final Vertx vertx;
    private final String cluster;
    private final Secret clusterCaCertSecret;
    private final Secret coKeySecret;
    private final Integer numPods;
    protected String namespace;
    private final AdminClientProvider adminClientProvider;
    private final String kafkaConfig;
    private final String kafkaLogging;
    private final KafkaVersion kafkaVersion;
    private final Reconciliation reconciliation;
    private final boolean allowReconfiguration;
    private Admin allClient;

    public KafkaRoller(Reconciliation reconciliation, Vertx vertx, PodOperator podOperations,
                       long pollingIntervalMs, long operationTimeoutMs, Supplier<BackOff> backOffSupplier,
                       StatefulSet sts, Secret clusterCaCertSecret, Secret coKeySecret,
                       String kafkaConfig, String kafkaLogging, KafkaVersion kafkaVersion, boolean allowReconfiguration) {
        this(reconciliation, vertx, podOperations, pollingIntervalMs, operationTimeoutMs, backOffSupplier,
                sts, clusterCaCertSecret, coKeySecret, new DefaultAdminClientProvider(), kafkaConfig, kafkaLogging, kafkaVersion, allowReconfiguration);
    }

    public KafkaRoller(Reconciliation reconciliation, Vertx vertx, PodOperator podOperations,
                       long pollingIntervalMs, long operationTimeoutMs, Supplier<BackOff> backOffSupplier,
                       StatefulSet sts, Secret clusterCaCertSecret, Secret coKeySecret,
                       AdminClientProvider adminClientProvider,
                       String kafkaConfig, String kafkaLogging, KafkaVersion kafkaVersion, boolean allowReconfiguration) {
        this.namespace = sts.getMetadata().getNamespace();
        this.cluster = Labels.cluster(sts);
        this.numPods = sts.getSpec().getReplicas();
        this.clusterCaCertSecret = clusterCaCertSecret;
        this.coKeySecret = coKeySecret;
        this.vertx = vertx;
        this.operationTimeoutMs = operationTimeoutMs;
        this.podOperations = podOperations;
        this.adminClientProvider = adminClientProvider;
        this.kafkaConfig = kafkaConfig;
        this.kafkaLogging = kafkaLogging;
        this.kafkaVersion = kafkaVersion;
        this.reconciliation = reconciliation;
        this.allowReconfiguration = allowReconfiguration;
    }

    /**
     * Returns a Future which completed with the actual pod corresponding to the abstract representation
     * of the given {@code pod}.
     */
    protected Future<Pod> pod(Integer podId) {
        return podOperations.getAsync(namespace, KafkaCluster.kafkaPodName(cluster, podId));
    }

    private ConcurrentHashMap<Integer, RestartContext> podToContext = new ConcurrentHashMap<>();
    private Function<Pod, List<String>> podNeedsRestart;

//    /**
//     * If allClient has not been initialized yet, does exactly that
//     * @return true if the creation of AC succeeded, false otherwise
//     */
//    private boolean initAdminClient() {
//        if (this.allClient == null) {
//            try {
//                this.allClient = adminClient(IntStream.range(0, numPods).boxed().collect(Collectors.toList()), false);
//            } catch (ForceableProblem | FatalProblem e) {
//                return false;
//            }
//        }
//        return true;
//    }

     /**
     * Asynchronously perform a rolling restart of some subset of the pods,
     * completing the returned Future when rolling is complete.
     * Which pods get rolled is determined by {@code podNeedsRestart}.
     * The pods may not be rolled in id order, due to the {@linkplain KafkaRoller rolling algorithm}.
     * @param podNeedsRestart Predicate for determining whether a pod should be rolled.
     * @return A Future completed when rolling is complete.
     */
    public Future<Void> rollingRestart(Function<Pod, List<String>> podNeedsRestart) {
        this.podNeedsRestart = podNeedsRestart;
        Promise<Void> result = Promise.promise();
        if (!vertx.getOrCreateContext().isEventLoopContext()) {
            return Future.failedFuture("Not event loop context!");
        }
        // There's nowhere for the group state to reside
        buildQueue().compose(queue -> {
            // create a queue
            if (queue.isEmpty()) {
                // handle empty queue (in theory impossible)
                result.complete();
            }
            Handler<Long> longHandler = new Handler<>() {
                @Override
                public void handle(Long timerId) {
                    assertEventLoop();
                    var context = queue.remove();
                    if (context.state() == DONE) {
                        // The head of the queue is done, due to the ordering that means all the contexts are done done.
                        LOGGER.debugCr(reconciliation, "Rolling actions complete");
                        result.complete();
                    }
                    LOGGER.debugCr(reconciliation, "Progressing {}", context);

                    context.progressOne().map(i -> {
                        assertEventLoop();
                        requeue(context);
                        return null;
                    });
                }

                private void requeue(RestartContext context) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debugCr(reconciliation, "Requeuing {}", context);
                    }
                    queue.add(context);
                    vertx.setTimer(context.actualDelayMs, this);
                }
            };
            vertx.setTimer(0, longHandler);
            return Future.succeededFuture();
        });
        return result.future().eventually(i -> vertx.executeBlocking(p -> {
            try {
                allClient.close(Duration.ofSeconds(30));
                p.complete();
            } catch (Exception e) {
                p.fail(e);
            }
        }));
    }

    /**
     * Build an initial list of contexts.
     */
    private Future<PriorityQueue<RestartContext>> buildQueue() {
        return vertx.executeBlocking(p -> {
            var queue = new PriorityQueue<>(numPods,
                    Comparator.comparing(RestartContext::state)
                            .thenComparing(RestartContext::notBefore)
                            .thenComparing(RestartContext::podId));
            for (int podId = 0; podId < numPods; podId++) {
                // Order the podIds unready first otherwise repeated reconciliations might each restart a pod
                // only for it not to become ready and thus drive the cluster to a worse state.
                // TODO a check for log recovery
                RestartContext context = new RestartContext(podId);
                queue.add(context);
            }
            p.complete(queue);
        });
    }

    /**
     * State machine:
     * <code><pre>
     *                 START
     *                   ↓
     *                UNKNOWN
     *                   ↕
     * any of (UNREADY, READY, CONTROLLER) which can all inter-transition
     *               ↙       ↘
     * NEEDS_RECONFIG    →    NEEDS_RESTART
     *               ↘       ↙
     *            AWAITING_READY
     *                   ↓
     *          AWAITING_LEADERSHIP
     *                   ↓
     *                 DONE
     * </pre></code>
     * In addition self transitions are allowed in all states except START, DONE
     * State transitions are managed by {@link RestartContext#makeTransition(RestartState, long)}.
     */
    enum RestartState {
        // NOTE: The order of these declarations matters, because it's used for the ordering of the priority queue
        /** The start state */
        START,
        /** A pod is unready (and should be rolled in the first batch) */
        UNREADY,
        /** An unknown state (due to an error during {@link RestartContext#classify()}). */
        NEEDS_CLASSIFY,
        /** A pod is ready (and should be rolled in the 2nd batch) */
        READY,
        /** A pod is the controller (and should be rolled in the final singleton batch) */
        CONTROLLER,
        /** A pod needs reconfig (may transition to NEEDS_RESTART if the reconfig fails) */
        NEEDS_RECONFIG,
        /** A pod needs restart */
        NEEDS_RESTART,
        /** Waiting to observe readiness following an action */
        AWAITING_READY,
        /** Waiting to observe leadership following an action */
        AWAITING_LEADERSHIP,
        /** The final state */
        DONE;
        RestartState assertIsOneOf(RestartState... states) {
            List<RestartState> list = Arrays.asList(states);
            if (!list.contains(this)) {
                throw new IllegalStateException("Should be in one of states " + list + " but in state " + this);
            }
            return this;
        }
    }

    /** Immutable class for passing between threads */
    static class Transition {
        private final RestartState state;
        private final long delayMs;

        public Transition(RestartState state, long delayMs) {
            this.state = state;
            this.delayMs = delayMs;
        }
    }


    /* Since restarting broker B won't result in its instantaneous removal from observed ISR
     * we need keep track of brokers which we're restarting, so we can correctly access
     * whether the restart of broker C will impact availability.
     * This allows for some parallelism in restart.
     */
    Set<Integer> restartingBrokers = new HashSet<>();
    int parallelism = 2;

    private void assertEventLoop() {
        if (!vertx.getOrCreateContext().isEventLoopContext()) {
            throw new IllegalStateException("Not executing on event loop thread");
        }
    }

    class RestartContext {
        private final static long BASE_DELAY_MS = 2048;
        private final int podId;
        private final String podName;
        // The current state
        private RestartState state;
        // The number of self transitions while in this state
        private int numSelfTransitions;
        // #ms since epoch that we've been in the current state
        private long stateEpoch;
        // The time wrt epoch after which we can consider an action
        private long notBefore;
        private KafkaBrokerConfigurationDiff configDiff;
        private KafkaBrokerLoggingConfigurationDiff loggersDiff;
        private int numReconfigsLeft = 3;
        private int numRetriesLeft = 3;
        private Future<Boolean> canRoll;

        private volatile Pod pod;
        private long actualDelayMs;

        public RestartContext(int podId) {
            this.podId = podId;
            this.podName = podName(podId);
            this.state = NEEDS_CLASSIFY;
            this.actualDelayMs = 0;
            this.stateEpoch = this.notBefore = System.currentTimeMillis();
        }

        public RestartState state() {
            return state;
        }

        public long notBefore() {
            return notBefore;
        }

        public int podId() {
            return podId;
        }

        @Override
        public String toString() {
            return "RestartContext(" +
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
         * @return the delay in ms after which a new transition should be attempted
         */
        Future<Void> progressOne() {
            assertEventLoop();
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

        private Future<Void> restart(Pod pod) {
            return podOperations.restart(reconciliation, pod, operationTimeoutMs);
        }

        private volatile KafkaFuture<Map<TopicPartition, Optional<Throwable>>> electLeaders = null;

        private Transition electPreferredLeader(Set<TopicPartition> partitions) {
            if (electLeaders == null) {
                this.electLeaders = allClient.electLeaders(ElectionType.PREFERRED, partitions).partitions();
            }
            if (electLeaders.isDone()) {
                try {
                    electLeaders.getNow(null).forEach((key, errOption) -> {
                        if (errOption.isPresent()) {
                            Throwable cause = errOption.get();
                            LOGGER.traceCr(reconciliation, "{}: Failed to elect preferred leader {} for partition {} due to {}", reconciliation, podId, key, cause);
                        }
                    });
                    electLeaders = null;
                    return new Transition(AWAITING_LEADERSHIP, 20_000);
                } catch (ExecutionException | InterruptedException e) {
                    // note: getNow() does not actually throw InterruptedException, it's a mistake in the Kafka API.
                    electLeaders = null;
                    return new Transition(AWAITING_LEADERSHIP, 20_000);
                }
            } else {
                return new Transition(AWAITING_LEADERSHIP, 20_000);
            }

        }

        private RestartState makeTransition(Transition transition) {
            return makeTransition(transition.state, transition.delayMs);
        }

        /**
         * Transition to a new state.
         * Transitions which would cause multiple restarts are not allowed.
         */
        private RestartState makeTransition(RestartState newState, long delay) {
            assertEventLoop();
            // Prevent restarting any pod more than once
            if (this.state.ordinal() >= NEEDS_RESTART.ordinal()
                    && newState.ordinal() <= NEEDS_RESTART.ordinal()) {
                throw new IllegalStateException("Illegal state transition " + this.state + " -> " + newState);
            }
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

        /** Determine whether this broker is READY, UNREADY or CONTROLLER */
        Future<Void> classify() {
            assertEventLoop();
            state.assertIsOneOf(START, NEEDS_CLASSIFY, READY, UNREADY, CONTROLLER);
            return vertx.<Transition>executeBlocking(p -> {
                Transition transition = null;
                try {
                    this.pod = podOperations.get(namespace, podName);
                    if (Readiness.isPodReady(pod)) {
                        if (isInLogRecovery()) {
                            transition = new Transition(NEEDS_CLASSIFY, 10_000); // retry in a while
                        } else {
                            transition = new Transition(READY, 0);
                        }
                    }
                    p.complete(transition);
                } catch (Exception e) {
                    p.tryFail(e);
                }
            }).compose(transition -> isController().map(isController -> {
                if (isController) {
                    return new Transition(CONTROLLER, 0);
                }
                if (transition == null) {
                    return new Transition(UNREADY, 0);
                } else {
                    return transition;
                }
                // TODO would need the map(this::makeTransition) to handle the failure case.
            })).otherwise(error -> {
                return new Transition(NEEDS_CLASSIFY, 10_000); // retry in a while
            }).map(this::makeTransition).map(null);
        }

        boolean isInLogRecovery() {
            return false; // TODO implement this
        }

        Future<Boolean> isController() {
            return Util.kafkaFutureToVertxFuture(reconciliation, vertx, allClient.describeCluster(new DescribeClusterOptions().timeoutMs(5_000))
                    .controller().thenApply(controller -> controller.id() == podId));
        }

        Future<Void> buildPlan() {
            assertEventLoop();
            state.assertIsOneOf(READY, CONTROLLER);
            Future<Transition> map = podOperations.getAsync(namespace, podName).compose((Function<Pod, Future<Transition>>) pod -> {
                List<String> reasonToRestartPod = Objects.requireNonNull(podNeedsRestart.apply(pod));
                boolean podStuck = pod != null && isUnschedulable(pod);
                if (podStuck && !reasonToRestartPod.contains("Pod has old generation")) {
                    // If the pod is unschedulable then deleting it, or trying to open an Admin client to it will make no difference
                    // Treat this as fatal because if it's not possible to schedule one pod then it's likely that proceeding
                    // and deleting a different pod in the meantime will likely result in another unschedulable pod.
                    return Future.failedFuture(new /*FatalProblem*/ RuntimeException("Pod is unschedulable"));
                }
                // Unless the annotation is present, check the pod is at least ready.
                boolean needsRestart = !reasonToRestartPod.isEmpty();

                if (needsRestart) {
                    LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted. Reason: {}", podId, reasonToRestartPod);
                    return Future.succeededFuture(new Transition(NEEDS_RESTART, 0));
                } else if (allowReconfiguration) {
                    // Always get the broker config. This request gets sent to that specific broker, so it's a proof that we can
                    // connect to the broker and that it's capable of responding.
                    return existingBrokerConfig().map(m2 -> {
                        KafkaBrokerConfigurationDiff configDiff = new KafkaBrokerConfigurationDiff(reconciliation, m2.get(ConfigResource.Type.BROKER), kafkaConfig, kafkaVersion, podId);
                        KafkaBrokerLoggingConfigurationDiff loggersDiff = new KafkaBrokerLoggingConfigurationDiff(reconciliation, m2.get(ConfigResource.Type.BROKER_LOGGER), kafkaLogging, podId);
                        if (configDiff.getDiffSize() > 0) {
                            if (configDiff.canBeUpdatedDynamically()) {
                                LOGGER.debugCr(reconciliation, "Pod {} needs to be reconfigured.", podId);
                                return new Transition(NEEDS_RECONFIG, 0);
                            } else {
                                LOGGER.debugCr(reconciliation, "Pod {} needs to be restarted, because reconfiguration cannot be done dynamically", podId);
                                return new Transition(NEEDS_RESTART, 0);
                            }
                        } else {
                            if (loggersDiff.getDiffSize() > 0) {
                                LOGGER.debugCr(reconciliation, "Pod {} logging needs to be reconfigured.", podId);
                                return new Transition(NEEDS_RECONFIG, 0);
                            } else {
                                return new Transition(AWAITING_READY, 0);
                            }
                        }
                    });
                } else {
                    return Future.succeededFuture(new Transition(NEEDS_RESTART, 0));
                }
            });
            return map.map(this::makeTransition).map(null);
        }

        /**
         * Returns a config of the given broker.
         * @return a Future which completes with the config of the given broker.
         */
        protected Future<Map<ConfigResource.Type, Config>> existingBrokerConfig() {
            DescribeConfigsResult describeConfigsResult = allClient.describeConfigs(
                    List.of(Util.brokerConfigResource(podId), Util.brokerLoggersConfigResource(podId)));
            return Util.kafkaFutureToVertxFuture(reconciliation, vertx, describeConfigsResult.all())
                    .map(mapOfConfigs -> mapOfConfigs.entrySet().stream()
                            .collect(Collectors.toMap(entry -> entry.getKey().type(), Map.Entry::getValue)));
        }


        private AlterConfigsResult alterConfigResult;

        Future<Void> dynamicUpdateBrokerConfig() {
            assertEventLoop();
            state.assertIsOneOf(NEEDS_RECONFIG);
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
                        this.alterConfigResult = allClient.incrementalAlterConfigs(updatedConfig, new AlterConfigsOptions().timeoutMs(30_000));
                    }

                    if (alterConfigResult.all().isDone()) {
                        // Call get to find out any exception
                        if (alterConfigResult.values().get(Util.brokerConfigResource(podId)).isCompletedExceptionally() ||
                                alterConfigResult.values().get(Util.brokerLoggersConfigResource(podId)).isCompletedExceptionally()) {
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
            }).map(this::makeTransition).map(null);
        }

        Future<Void> maybeRestart() {
            assertEventLoop();
            // TODO should the restart path revert to UNKNOWN, so we have to prove non-controllership again
            state.assertIsOneOf(NEEDS_RESTART);
            if (restartingBrokers.size() > parallelism) {
                makeTransition(NEEDS_RESTART, 10_000);// TODO have a future that is completed when restartingBrokers changes?
                return Future.succeededFuture();
            } else {
                return availability(allClient).canRoll(podId, restartingBrokers).map(canRoll -> {
                    if (canRoll) {
                        assertEventLoop();
                        restartingBrokers.add(podId);
                        restart(pod);
                        return new Transition(AWAITING_READY, 2_000); // No point in checking for readiness immediately
                    } else {
                        return new Transition(NEEDS_RESTART, 10_000);
                    }
                }).otherwise(new Transition(NEEDS_RESTART, 10_000))
                        .map(this::makeTransition).map(null);
            }
        }

        Future<Void> checkForReadiness() {
            assertEventLoop();
            state.assertIsOneOf(AWAITING_READY);
            return podOperations.readiness(reconciliation, namespace, podName, 1_000, 120_000)
                    .map(new Transition(AWAITING_LEADERSHIP, 0))
                    .otherwise(new Transition(AWAITING_READY, 0))
                    .map(this::makeTransition)
                    .map(null);
        }

        Future<Void> checkForLeadership() {
            assertEventLoop();
            state.assertIsOneOf(AWAITING_READY);

            return partitionsToElect(podId).compose(partitions -> {
                assertEventLoop();
                if (partitions.isEmpty()) {
                    restartingBrokers.remove(podId);
                    return Future.succeededFuture(new Transition(DONE, 0));
                } else {
                    return Util.kafkaFutureToVertxFuture(reconciliation, vertx, allClient.electLeaders(ElectionType.PREFERRED, partitions).partitions())
                            .map(new Transition(AWAITING_READY, 0))
                            .otherwise(new Transition(AWAITING_READY, 10_000));
                }
            }).map(this::makeTransition)
                .map(null);
        }

        private Future<Set<TopicPartition>> partitionsToElect(int podId) {
            return availability(allClient).partitionsWithPreferredButNotCurrentLeader(podId);
        }
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



//    /**
//     * Determine whether the pod should be restarted, or the broker reconfigured.
//     */
//    private RestartPlan restartPlan(int podId, Pod pod, RestartContext restartContext) throws ForceableProblem, InterruptedException, FatalProblem {
//
//        List<String> reasonToRestartPod = Objects.requireNonNull(podNeedsRestart.apply(pod));
//        boolean podStuck = pod != null && isUnschedulable(pod);
//        if (podStuck && !reasonToRestartPod.contains("Pod has old generation")) {
//            // If the pod is unschedulable then deleting it, or trying to open an Admin client to it will make no difference
//            // Treat this as fatal because if it's not possible to schedule one pod then it's likely that proceeding
//            // and deleting a different pod in the meantime will likely result in another unschedulable pod.
//            throw new FatalProblem("Pod is unschedulable");
//        }
//        // Unless the annotation is present, check the pod is at least ready.
//        boolean needsRestart = !reasonToRestartPod.isEmpty();
//        KafkaBrokerConfigurationDiff diff = null;
//        KafkaBrokerLoggingConfigurationDiff loggingDiff = null;
//        boolean needsReconfig = false;
//        // Always get the broker config. This request gets sent to that specific broker, so it's a proof that we can
//        // connect to the broker and that it's capable of responding.
//        if (!initAdminClient()) {
//            return new RestartPlan(true);
//        }
//        Config brokerConfig;
//        try {
//            brokerConfig = existingBrokerConfig(podId);
//        } catch (ForceableProblem e) {
//            if (restartContext.backOff.done()) {
//                needsRestart = true;
//                brokerConfig = null;
//            } else {
//                throw e;
//            }
//        }
//
//        if (!needsRestart && allowReconfiguration) {
//            LOGGER.traceCr(reconciliation, "Broker {}: description {}", podId, brokerConfig);
//            diff = new KafkaBrokerConfigurationDiff(reconciliation, brokerConfig, kafkaConfig, kafkaVersion, podId);
//            loggingDiff = loggingDiff(podId);
//            if (diff.getDiffSize() > 0) {
//                if (diff.canBeUpdatedDynamically()) {
//                    LOGGER.debugCr(reconciliation, "Pod {} needs to be reconfigured.", podId);
//                    needsReconfig = true;
//                } else {
//                    LOGGER.debugCr(reconciliation, "Pod {} needs to be restarted, because reconfiguration cannot be done dynamically", podId);
//                    needsRestart = true;
//                }
//            }
//
//            if (loggingDiff.getDiffSize() > 0) {
//                LOGGER.debugCr(reconciliation, "Pod {} logging needs to be reconfigured.", podId);
//                needsReconfig = true;
//            }
//        } else if (needsRestart) {
//            LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted. Reason: {}", podId, reasonToRestartPod);
//        }
//        return new RestartPlan(needsRestart, needsReconfig, podStuck, diff, loggingDiff);
//    }

    /** Exceptions which we're prepared to ignore (thus forcing a restart) in some circumstances. */
    static final class ForceableProblem extends Exception {
        final boolean forceNow;
        ForceableProblem(String msg) {
            this(msg, null);
        }

        ForceableProblem(String msg, Throwable cause) {
            this(msg, cause, false);
        }

        ForceableProblem(String msg, boolean forceNow) {
            this(msg, null, forceNow);
        }

        ForceableProblem(String msg, Throwable cause, boolean forceNow) {
            super(msg, cause);
            this.forceNow = forceNow;
        }
    }


    /**
     * Returns an AdminClient instance bootstrapped from the given pod.
     */
    protected Admin adminClient(List<Integer> bootstrapPods) throws ForceableProblem/*, FatalProblem*/ {
        List<String> podNames = bootstrapPods.stream().map(this::podName).collect(Collectors.toList());
        try {
            String bootstrapHostnames = podNames.stream().map(podName -> KafkaCluster.podDnsName(this.namespace, this.cluster, podName) + ":" + KafkaCluster.REPLICATION_PORT).collect(Collectors.joining(","));
            LOGGER.debugCr(reconciliation, "Creating AdminClient for {}", bootstrapHostnames);
            return adminClientProvider.createAdminClient(bootstrapHostnames, this.clusterCaCertSecret, this.coKeySecret, "cluster-operator");
        } catch (KafkaException e) {
//            if (ceShouldBeFatal && (e instanceof ConfigException
//                    || e.getCause() instanceof ConfigException)) {
//                throw new FatalProblem("An error while try to create an admin client with bootstrap brokers " + podNames, e);
//            } else {
                throw new ForceableProblem("An error while try to create an admin client with bootstrap brokers " + podNames, e);
//            }
        } catch (RuntimeException e) {
            throw new ForceableProblem("An error while try to create an admin client with bootstrap brokers " + podNames, e);
        }
    }

    /* test */ protected KafkaAvailability availability(Admin ac) {
        return new KafkaAvailability(reconciliation, vertx, ac);
    }

    String podName(int podId) {
        return KafkaCluster.kafkaPodName(this.cluster, podId);
    }

//    /**
//     * If we've already had trouble connecting to this broker try to probe whether the connection is
//     * open on the broker; if it's not then maybe throw a ForceableProblem to immediately force a restart.
//     * This is an optimization for brokers which don't seem to be running.
//     */
//    private void maybeTcpProbe(int podId, Exception executionException, RestartContext restartContext) throws Exception {
//        if (restartContext.connectionError() + numPods * 120_000L >= System.currentTimeMillis()) {
//            try {
//                LOGGER.debugCr(reconciliation, "Probing TCP port due to previous problems connecting to pod {}", podId);
//                // do a tcp connect and close (with a short connect timeout)
//                tcpProbe(podName(podId), KafkaCluster.REPLICATION_PORT);
//            } catch (IOException connectionException) {
//                throw new ForceableProblem("Unable to connect to " + podName(podId) + ":" + KafkaCluster.REPLICATION_PORT, executionException.getCause(), true);
//            }
//            throw executionException;
//        } else {
//            restartContext.noteConnectionError();
//            throw new ForceableProblem("Error while trying to determine the cluster controller from pod " + podName(podId), executionException.getCause());
//        }
//    }
//
//    /**
//     * Tries to open and close a TCP connection to the given host and port.
//     * @param hostname The host
//     * @param port The port
//     * @throws IOException if anything went wrong.
//     */
//    void tcpProbe(String hostname, int port) throws IOException {
//        Socket socket = new Socket();
//        try {
//            socket.connect(new InetSocketAddress(hostname, port), 5_000);
//        } finally {
//            socket.close();
//        }
//    }

    @Override
    public String toString() {
        return podToContext.toString();
    }

}
