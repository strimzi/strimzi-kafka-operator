/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.SslAuthenticationException;

import static java.util.Collections.singletonList;

/**
 * <p>Manages the rolling restart of a Kafka cluster.</p>
 *
 * <p>The following algorithm is used:</p>
 *
 * <pre>
 *   0. Start with a list of all the pods
 *   1. While the list is non-empty:
 *     2. Take the next pod from the list.
 *     3. Test whether the pod needs to be restarted.
 *         If not then:
 *           i.  Wait for it to be ready.
 *           ii. Continue from 1.
 *     4. Otherwise, check whether the pod is the controller
 *         If so, and there are still pods to be maybe-restarted then:
 *           i.  Reschedule the restart of this pod by appending it the list
 *           ii. Continue from 1.
 *     5. Otherwise, check whether the pod can be restarted without "impacting availability"
 *         If not then:
 *           i.  Reschedule the restart of this pod by appending it the list
 *           ii. Continue from 1.
 *     6. Otherwise:
 *         i.   Restart the pod
 *         ii.  Wait for it to become ready (in the kube sense)
 *         iii. Continue from 1.
 * </pre>
 *
 * <p>Where "impacting availability" is defined by {@link KafkaAvailability}.</p>
 *
 * <p>Note the following important properties of this algorithm:</p>
 * <ul>
 *     <li>if there is a spontaneous change in controller while the rolling restart is happening, any new
 *     controller is still the last pod to be rolled, thus avoid unnecessary controller elections.</li>
 *     <li>rolling should happen without impacting any topic's min.isr.</li>
 *     <li>even pods which aren't candidates for rolling are checked for readiness which partly avoids
 *     successive reconciliations each restarting a pod which never becomes ready</li>
 * </ul>
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ParameterNumber"})
public class KafkaRoller {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaRoller.class);

    private final PodOperator podOperations;
    private final long pollingIntervalMs;
    protected final long operationTimeoutMs;
    protected final Vertx vertx;
    private final String cluster;
    private final Secret clusterCaCertSecret;
    private final Secret coKeySecret;
    private final List<String> podList;
    private final KubernetesRestartEventPublisher eventsPublisher;
    private final Supplier<BackOff> backoffSupplier;
    protected String namespace;
    private final AdminClientProvider adminClientProvider;
    private final Function<Integer, String> kafkaConfigProvider;
    private final String kafkaLogging;
    private final KafkaVersion kafkaVersion;
    private final Reconciliation reconciliation;
    private final boolean allowReconfiguration;
    private Admin allClient;

    /**
     * Constructor
     *
     * @param reconciliation        Reconciliation marker
     * @param vertx                 Vert.x instance
     * @param podOperations         Pod operator for managing pods
     * @param pollingIntervalMs     Polling interval in milliseconds
     * @param operationTimeoutMs    Operation timeout in milliseconds
     * @param backOffSupplier       Backoff supplier
     * @param podList               List of Kafka pods
     * @param clusterCaCertSecret   Secret with the Cluster CA public key
     * @param coKeySecret           Secret with the Cluster CA private key
     * @param adminClientProvider   Kafka Admin client provider
     * @param kafkaConfigProvider   Kafka configuration provider
     * @param kafkaLogging          Kafka logging configuration
     * @param kafkaVersion          Kafka version
     * @param allowReconfiguration  Flag indicting whether reconfiguration is allowed or not
     * @param eventsPublisher       Kubernetes Events publisher for publishing events about pod restarts
     */
    public KafkaRoller(Reconciliation reconciliation, Vertx vertx, PodOperator podOperations,
                       long pollingIntervalMs, long operationTimeoutMs, Supplier<BackOff> backOffSupplier, List<String> podList,
                       Secret clusterCaCertSecret, Secret coKeySecret,
                       AdminClientProvider adminClientProvider,
                       Function<Integer, String> kafkaConfigProvider, String kafkaLogging, KafkaVersion kafkaVersion, boolean allowReconfiguration, KubernetesRestartEventPublisher eventsPublisher) {
        this.namespace = reconciliation.namespace();
        this.cluster = reconciliation.name();
        this.podList = podList;
        this.eventsPublisher = eventsPublisher;
        if (podList.size() != podList.stream().distinct().count()) {
            throw new IllegalArgumentException();
        }
        this.backoffSupplier = backOffSupplier;
        this.clusterCaCertSecret = clusterCaCertSecret;
        this.coKeySecret = coKeySecret;
        this.vertx = vertx;
        this.operationTimeoutMs = operationTimeoutMs;
        this.podOperations = podOperations;
        this.pollingIntervalMs = pollingIntervalMs;
        this.adminClientProvider = adminClientProvider;
        this.kafkaConfigProvider = kafkaConfigProvider;
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
        return podOperations.getAsync(namespace, KafkaResources.kafkaPodName(cluster, podId));
    }

    private final ScheduledExecutorService singleExecutor = Executors.newSingleThreadScheduledExecutor(
        runnable -> new Thread(runnable, "kafka-roller"));

    private ConcurrentHashMap<String, RestartContext> podToContext = new ConcurrentHashMap<>();
    private Function<Pod, RestartReasons> podNeedsRestart;

    /**
     * If allClient has not been initialized yet, does exactly that
     * @return true if the creation of AC succeeded, false otherwise
     */
    private boolean initAdminClient() {
        if (this.allClient == null) {
            try {
                this.allClient = adminClient(IntStream.range(0, podList.size()).boxed().collect(Collectors.toList()), false);
            } catch (ForceableProblem | FatalProblem e) {
                LOGGER.warnCr(reconciliation, "Failed to create adminClient.", e);
                return false;
            }
        }
        return true;
    }

    /**
     * Asynchronously perform a rolling restart of some subset of the pods,
     * completing the returned Future when rolling is complete.
     * Which pods get rolled is determined by {@code podNeedsRestart}.
     * The pods may not be rolled in id order, due to the {@linkplain KafkaRoller rolling algorithm}.
     * @param podNeedsRestart Predicate for determining whether a pod should be rolled.
     * @return A Future completed when rolling is complete.
     */
    public Future<Void> rollingRestart(Function<Pod, RestartReasons> podNeedsRestart) {
        this.podNeedsRestart = podNeedsRestart;
        Promise<Void> result = Promise.promise();
        singleExecutor.submit(() -> {
            LOGGER.debugCr(reconciliation, "Verifying cluster pods are up-to-date.");
            List<PodRef> pods = new ArrayList<>(podList.size());

            for (int podIndex = 0; podIndex < podList.size(); podIndex++) {
                // Order the podNames unready first otherwise repeated reconciliations might each restart a pod
                // only for it not to become ready and thus drive the cluster to a worse state.
                pods.add(podOperations.isReady(namespace, podList.get(podIndex)) ? pods.size() : 0, new PodRef(podList.get(podIndex), ModelUtils.idOfPod(podList.get(podIndex))));
            }
            LOGGER.debugCr(reconciliation, "Initial order for updating pods (rolling restart or dynamic update) is {}", pods);
            List<Future> futures = new ArrayList<>(podList.size());
            for (PodRef podRef: pods) {
                futures.add(schedule(podRef, 0, TimeUnit.MILLISECONDS));
            }
            CompositeFuture.join(futures).onComplete(ar -> {
                singleExecutor.shutdown();
                try {
                    if (allClient != null) {
                        allClient.close(Duration.ofSeconds(30));
                    }
                } catch (RuntimeException e) {
                    LOGGER.debugCr(reconciliation, "Exception closing admin client", e);
                }
                vertx.runOnContext(ignored -> result.handle(ar.map((Void) null)));
            });
        });
        return result.future();
    }

    protected static class RestartContext {
        final Promise<Void> promise;
        final BackOff backOff;
        RestartReasons restartReasons;
        private long connectionErrorStart = 0L;

        boolean needsRestart;
        boolean needsReconfig;
        boolean forceRestart;
        KafkaBrokerConfigurationDiff diff;
        KafkaBrokerLoggingConfigurationDiff logDiff;

        RestartContext(Supplier<BackOff> backOffSupplier) {
            promise = Promise.promise();
            backOff = backOffSupplier.get();
            backOff.delayMs();
        }

        public void clearConnectionError() {
            connectionErrorStart = 0L;
        }

        long connectionError() {
            return connectionErrorStart;
        }

        void noteConnectionError() {
            if (connectionErrorStart == 0L) {
                connectionErrorStart = System.currentTimeMillis();
            }
        }

        @Override
        public String toString() {
            return "RestartContext{" +
                    "promise=" + promise +
                    ", backOff=" + backOff +
                    '}';
        }
    }

    /**
     * Schedule the rolling of the given pod at or after the given delay,
     * completed the returned Future when the pod is rolled.
     * When called multiple times with the same podId this method will return the same Future instance.
     * Pods will be rolled one-at-a-time so the delay may be overrun.
     * @param podRef  The reference to pod to roll.
     * @param delay The delay.
     * @param unit The unit of the delay.
     * @return A future which completes when the pod has been rolled.
     */
    private Future<Void> schedule(PodRef podRef, long delay, TimeUnit unit) {
        RestartContext ctx = podToContext.computeIfAbsent(podRef.getPodName(),
            k -> new RestartContext(backoffSupplier));
        singleExecutor.schedule(() -> {
            LOGGER.debugCr(reconciliation, "Considering updating pod {} after a delay of {} {}", podRef, delay, unit);
            try {
                restartIfNecessary(podRef, ctx);
                ctx.promise.complete();
            } catch (InterruptedException e) {
                // Let the executor deal with interruption.
                Thread.currentThread().interrupt();
            } catch (FatalProblem e) {
                LOGGER.infoCr(reconciliation, "Could not verify pod {} is up-to-date, giving up after {} attempts. Total delay between attempts {}ms",
                        podRef, ctx.backOff.maxAttempts(), ctx.backOff.totalDelayMs(), e);
                ctx.promise.fail(e);
                singleExecutor.shutdownNow();
                podToContext.forEachValue(Integer.MAX_VALUE, f -> {
                    f.promise.tryFail(e);
                });
            } catch (Exception e) {
                if (ctx.backOff.done()) {
                    LOGGER.infoCr(reconciliation, "Could not verify pod {} is up-to-date, giving up after {} attempts. Total delay between attempts {}ms",
                            podRef, ctx.backOff.maxAttempts(), ctx.backOff.totalDelayMs(), e);
                    ctx.promise.fail(e instanceof TimeoutException ?
                            new io.strimzi.operator.common.operator.resource.TimeoutException() :
                            e);
                } else {
                    long delay1 = ctx.backOff.delayMs();
                    LOGGER.infoCr(reconciliation, "Will temporarily skip verifying pod {} is up-to-date due to {}, retrying after at least {}ms",
                            podRef, e, delay1);
                    schedule(podRef, delay1, TimeUnit.MILLISECONDS);
                }
            }
        }, delay, unit);
        return ctx.promise.future();
    }

    /**
     * Restart the given pod now if necessary according to {@link #podNeedsRestart}.
     * This method blocks.
     * @param podRef Reference of pod to roll.
     * @param restartContext
     * @throws InterruptedException Interrupted while waiting.
     * @throws ForceableProblem Some error. Not thrown when finalAttempt==true.
     * @throws UnforceableProblem Some error, still thrown when finalAttempt==true.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @SuppressWarnings({"checkstyle:CyclomaticComplexity"})
    private void restartIfNecessary(PodRef podRef, RestartContext restartContext)
            throws Exception {
        Pod pod;
        try {
            pod = podOperations.get(namespace, podRef.getPodName());
            if (pod == null) {
                LOGGER.debugCr(reconciliation, "Pod {} doesn't exist. There seems to be some problem with the creation of pod by StatefulSets/StrimziPodSets controller", podRef.getPodName());
                return;
            }
        } catch (KubernetesClientException e) {
            throw new UnforceableProblem("Error getting pod " + podRef.getPodName(), e);
        }

        restartContext.restartReasons = podNeedsRestart.apply(pod);

        try {
            checkReconfigurability(podRef, pod, restartContext);
            if (restartContext.forceRestart || restartContext.needsRestart || restartContext.needsReconfig) {
                if (!restartContext.forceRestart && deferController(podRef, restartContext)) {
                    LOGGER.debugCr(reconciliation, "Pod {} is controller and there are other pods to verify. Non-controller pods will be verified first.", podRef);
                    throw new ForceableProblem("Pod " + podRef.getPodName() + " is controller and there are other pods to verify. Non-controller pods will be verified first");
                } else {
                    if (restartContext.forceRestart || canRoll(podRef, 60_000, TimeUnit.MILLISECONDS, false, restartContext)) {
                        // Check for rollability before trying a dynamic update so that if the dynamic update fails we can go to a full restart
                        if (restartContext.forceRestart || !maybeDynamicUpdateBrokerConfig(podRef, restartContext)) {
                            LOGGER.debugCr(reconciliation, "Pod {} can be rolled now", podRef);
                            restartAndAwaitReadiness(pod, operationTimeoutMs, TimeUnit.MILLISECONDS, restartContext);
                        } else {
                            awaitReadiness(pod, operationTimeoutMs, TimeUnit.MILLISECONDS);
                        }
                    } else {
                        LOGGER.debugCr(reconciliation, "Pod {} cannot be updated right now", podRef);
                        throw new UnforceableProblem("Pod " + podRef.getPodName() + " cannot be updated right now.");
                    }
                }
            } else {
                // By testing even pods which don't need needsRestart for readiness we prevent successive reconciliations
                // from taking out a pod each time (due, e.g. to a configuration error).
                // We rely on Kube to try restarting such pods.
                LOGGER.debugCr(reconciliation, "Pod {} does not need to be restarted", podRef);
                LOGGER.debugCr(reconciliation, "Waiting for non-restarted pod {} to become ready", podRef);
                await(isReady(namespace, podRef.getPodName()), operationTimeoutMs, TimeUnit.MILLISECONDS, e -> new FatalProblem("Error while waiting for non-restarted pod " + podRef.getPodName() + " to become ready", e));
                LOGGER.debugCr(reconciliation, "Pod {} is now ready", podRef);
            }
        } catch (ForceableProblem e) {
            if (isPodStuck(pod) || restartContext.backOff.done() || e.forceNow) {
                if (canRoll(podRef, 60_000, TimeUnit.MILLISECONDS, true, restartContext)) {
                    LOGGER.warnCr(reconciliation, "Pod {} will be force-rolled, due to error: {}", podRef, e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
                    restartContext.restartReasons.add(RestartReason.POD_FORCE_RESTART_ON_ERROR);
                    restartAndAwaitReadiness(pod, operationTimeoutMs, TimeUnit.MILLISECONDS, restartContext);
                } else {
                    LOGGER.warnCr(reconciliation, "Pod {} can't be safely force-rolled; original error: ", podRef, e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
                    throw e;
                }
            } else {
                throw e;
            }
        }
    }

    private boolean podWaitingBecauseOfAnyReasons(Pod pod, Set<String> reasons) {
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

    private boolean isPending(Pod pod) {
        if (pod != null && pod.getStatus() != null && "Pending".equals(pod.getStatus().getPhase())) {
            return true;
        }
        return false;
    }

    private boolean isPodStuck(Pod pod) {
        Set<String> set = new HashSet<>();
        set.add("CrashLoopBackOff");
        set.add("ImagePullBackOff");
        set.add("ContainerCreating");
        return isPending(pod) || podWaitingBecauseOfAnyReasons(pod, set);
    }

    /**
     * Dynamically update the broker config if the plan says we can.
     * Return true if the broker was successfully updated dynamically.
     */
    private boolean maybeDynamicUpdateBrokerConfig(PodRef podRef, RestartContext restartContext) throws InterruptedException {
        boolean updatedDynamically;

        if (restartContext.needsReconfig) {
            try {
                dynamicUpdateBrokerConfig(podRef, allClient, restartContext.diff, restartContext.logDiff);
                updatedDynamically = true;
            } catch (ForceableProblem e) {
                LOGGER.debugCr(reconciliation, "Pod {} could not be updated dynamically ({}), will restart", podRef, e);
                updatedDynamically = false;
            }
        } else {
            updatedDynamically = false;
        }
        return updatedDynamically;
    }

    /**
     * Determine whether the pod should be restarted, or the broker reconfigured.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private void checkReconfigurability(PodRef podRef, Pod pod, RestartContext restartContext) throws ForceableProblem, InterruptedException, FatalProblem {

        RestartReasons reasonToRestartPod = restartContext.restartReasons;
        boolean podStuck = pod != null
                && pod.getStatus() != null
                && "Pending".equals(pod.getStatus().getPhase())
                && pod.getStatus().getConditions().stream().anyMatch(ps ->
                "PodScheduled".equals(ps.getType())
                        && "Unschedulable".equals(ps.getReason())
                        && "False".equals(ps.getStatus()));
        if (podStuck) {
            LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted, because it seems to be stuck and restart might help", podRef);
            restartContext.restartReasons.add(RestartReason.POD_STUCK);
        }

        if (podStuck
                && !reasonToRestartPod.contains(RestartReason.POD_HAS_OLD_GENERATION)   // "Pod has old generation" is used with StatefulSets
                && !reasonToRestartPod.contains(RestartReason.POD_HAS_OLD_REVISION)) {  // "Pod has old revision" is used with PodSets
            // If the pod is unschedulable then deleting it, or trying to open an Admin client to it will make no difference
            // Treat this as fatal because if it's not possible to schedule one pod then it's likely that proceeding
            // and deleting a different pod in the meantime will likely result in another unschedulable pod.
            throw new FatalProblem("Pod is unschedulable");
        }
        // Unless the annotation is present, check the pod is at least ready.
        boolean needsRestart = reasonToRestartPod.shouldRestart();
        KafkaBrokerConfigurationDiff diff = null;
        KafkaBrokerLoggingConfigurationDiff loggingDiff = null;
        boolean needsReconfig = false;
        // Always get the broker config. This request gets sent to that specific broker, so it's a proof that we can
        // connect to the broker and that it's capable of responding.
        if (!initAdminClient()) {
            LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted, because it does not seem to responding to connection attempts", podRef);
            reasonToRestartPod.add(RestartReason.POD_UNRESPONSIVE);
            restartContext.needsRestart = false;
            restartContext.needsReconfig = false;
            restartContext.forceRestart = true;
            restartContext.diff = null;
            restartContext.logDiff = null;
            return;
        }
        Config brokerConfig;
        try {
            brokerConfig = brokerConfig(podRef);
        } catch (ForceableProblem e) {
            if (restartContext.backOff.done()) {
                needsRestart = true;
                brokerConfig = null;
            } else {
                throw e;
            }
        }

        if (!needsRestart && allowReconfiguration) {
            LOGGER.traceCr(reconciliation, "Pod {}: description {}", podRef, brokerConfig);
            diff = new KafkaBrokerConfigurationDiff(reconciliation, brokerConfig, kafkaConfigProvider.apply(podRef.getPodId()), kafkaVersion, podRef.getPodId());
            loggingDiff = logging(podRef);

            if (diff.getDiffSize() > 0) {
                if (diff.canBeUpdatedDynamically()) {
                    LOGGER.debugCr(reconciliation, "Pod {} needs to be reconfigured.", podRef);
                    needsReconfig = true;
                } else {
                    LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted, dynamic update cannot be done.", podRef);
                    restartContext.restartReasons.add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                    needsRestart = true;
                }
            }

            // needsRestart value might have changed from the check in the parent if. So we need to check it again.
            if (!needsRestart && loggingDiff.getDiffSize() > 0) {
                LOGGER.debugCr(reconciliation, "Pod {} logging needs to be reconfigured.", podRef);
                needsReconfig = true;
            }
        } else if (needsRestart) {
            LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted. Reason: {}", podRef, reasonToRestartPod.getAllReasonNotes());
        }

        restartContext.needsRestart = needsRestart;
        restartContext.needsReconfig = needsReconfig;
        restartContext.forceRestart = podStuck;
        restartContext.diff = diff;
        restartContext.logDiff = loggingDiff;
    }

    /**
     * Returns a config of the given broker.
     * @param podRef The reference of the broker.
     * @return a Future which completes with the config of the given broker.
     */
    protected Config brokerConfig(PodRef podRef) throws ForceableProblem, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(podRef.getPodId()));
        return await(Util.kafkaFutureToVertxFuture(reconciliation, vertx, allClient.describeConfigs(singletonList(resource)).values().get(resource)),
            30, TimeUnit.SECONDS,
            error -> new ForceableProblem("Error getting broker config", error)
        );
    }

    /**
     * Returns logging of the given broker.
     * @param brokerId The id of the broker.
     * @return a Future which completes with the logging of the given broker.
     */
    protected Config brokerLogging(int brokerId) throws ForceableProblem, InterruptedException {
        ConfigResource resource = Util.getBrokersLogging(brokerId);
        return await(Util.kafkaFutureToVertxFuture(reconciliation, vertx, allClient.describeConfigs(singletonList(resource)).values().get(resource)),
                30, TimeUnit.SECONDS,
            error -> new ForceableProblem("Error getting broker logging", error)
        );
    }

    protected void dynamicUpdateBrokerConfig(PodRef podRef, Admin ac, KafkaBrokerConfigurationDiff configurationDiff, KafkaBrokerLoggingConfigurationDiff logDiff)
            throws ForceableProblem, InterruptedException {
        Map<ConfigResource, Collection<AlterConfigOp>> updatedConfig = new HashMap<>(2);
        var podId = podRef.podId;
        updatedConfig.put(Util.getBrokersConfig(podId), configurationDiff.getConfigDiff());
        updatedConfig.put(Util.getBrokersLogging(podId), logDiff.getLoggingDiff());

        LOGGER.debugCr(reconciliation, "Updating broker configuration {}", podRef);
        LOGGER.traceCr(reconciliation, "Updating broker configuration {} with {}", podRef, updatedConfig);

        AlterConfigsResult alterConfigResult = ac.incrementalAlterConfigs(updatedConfig);
        KafkaFuture<Void> brokerConfigFuture = alterConfigResult.values().get(Util.getBrokersConfig(podId));
        KafkaFuture<Void> brokerLoggingConfigFuture = alterConfigResult.values().get(Util.getBrokersLogging(podId));
        await(Util.kafkaFutureToVertxFuture(reconciliation, vertx, brokerConfigFuture), 30, TimeUnit.SECONDS,
            error -> {
                LOGGER.errorCr(reconciliation, "Error updating broker configuration for pod {}", podRef, error);
                return new ForceableProblem("Error updating broker configuration for pod " + podRef, error);
            });
        await(Util.kafkaFutureToVertxFuture(reconciliation, vertx, brokerLoggingConfigFuture), 30, TimeUnit.SECONDS,
            error -> {
                LOGGER.errorCr(reconciliation, "Error updating broker logging configuration pod {}", podRef, error);
                return new ForceableProblem("Error updating broker logging configuration pod " + podRef, error);
            });

        LOGGER.infoCr(reconciliation, "Dynamic update of pod {} was successful.", podRef);
    }

    private KafkaBrokerLoggingConfigurationDiff logging(PodRef podRef)
            throws ForceableProblem, InterruptedException {
        Config brokerLogging = brokerLogging(podRef.getPodId());
        LOGGER.traceCr(reconciliation, "Pod {}: logging description {}", podRef, brokerLogging);
        return new KafkaBrokerLoggingConfigurationDiff(reconciliation, brokerLogging, kafkaLogging);
    }

    /** Exceptions which we're prepared to ignore (thus forcing a restart) in some circumstances. */
    static final class ForceableProblem extends Exception {
        final boolean forceNow;
        ForceableProblem(String msg) {
            this(msg, null);
        }

        ForceableProblem(String msg, Throwable cause) {
            this(msg, cause, false);
        }

        ForceableProblem(String msg, Throwable cause, boolean forceNow) {
            super(msg, cause);
            this.forceNow = forceNow;
        }

        @Override
        public String toString() {
            /**
             * This is a static nested class, so we want to prevent the Outer$Nested
             * name returned by getSimpleName()
             */
            var name = "ForceableProblem";
            var message = getMessage();
            return (message != null) ? (name + ": " + message) : name;
        }
    }

    /** Exceptions which we're prepared to ignore in the final attempt */
    static final class UnforceableProblem extends Exception {
        UnforceableProblem(String msg) {
            this(msg, null);
        }
        UnforceableProblem(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    /** Immediately aborts rolling */
    static final class FatalProblem extends Exception {
        public FatalProblem(String message) {
            super(message);
        }

        FatalProblem(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    private boolean canRoll(PodRef podRef, long timeout, TimeUnit unit, boolean ignoreSslError, RestartContext restartContext)
            throws ForceableProblem, InterruptedException {
        try {
            return await(availability(allClient).canRoll(podRef.getPodId()), timeout, unit,
                t -> new ForceableProblem("An error while trying to determine the possibility of updating Kafka pods", t));
        } catch (ForceableProblem e) {
            // If we're not able to connect then roll
            if (ignoreSslError && e.getCause() instanceof SslAuthenticationException) {
                restartContext.restartReasons.add(RestartReason.POD_UNRESPONSIVE);
                return true;
            } else {
                throw e;
            }
        }
    }

    /**
     * Synchronously restart the given pod
     * by deleting it and letting it be recreated by K8s, then synchronously wait for it to be ready.
     *
     * @param pod            The Pod to restart.
     * @param timeout        The timeout.
     * @param unit           The timeout unit.
     * @param restartContext
     */
    private void restartAndAwaitReadiness(Pod pod, long timeout, TimeUnit unit, RestartContext restartContext)
            throws InterruptedException, UnforceableProblem, FatalProblem {
        String podName = pod.getMetadata().getName();
        LOGGER.debugCr(reconciliation, "Rolling pod {}", podName);
        await(restart(pod, restartContext), timeout, unit, e -> new UnforceableProblem("Error while trying to restart pod " + podName + " to become ready", e));
        awaitReadiness(pod, timeout, unit);
    }

    private void awaitReadiness(Pod pod, long timeout, TimeUnit unit) throws FatalProblem, InterruptedException {
        String podName = pod.getMetadata().getName();
        LOGGER.debugCr(reconciliation, "Waiting for restarted pod {} to become ready", podName);
        await(isReady(pod), timeout, unit, e -> new FatalProblem("Error while waiting for restarted pod " + podName + " to become ready", e));
        LOGGER.debugCr(reconciliation, "Pod {} is now ready", podName);
    }

    /**
     * Block waiting for up to the given timeout for the given Future to complete, returning its result.
     * @param future The future to wait for.
     * @param timeout The timeout
     * @param unit The timeout unit
     * @param exceptionMapper A function for rethrowing exceptions.
     * @param <T> The result type
     * @param <E> The exception type
     * @return The result of of the future
     * @throws E The exception type returned from {@code exceptionMapper}.
     * @throws TimeoutException If the given future is not completed before the timeout.
     * @throws InterruptedException If the waiting was interrupted.
     */
    private static <T, E extends Exception> T await(Future<T> future, long timeout, TimeUnit unit,
                                            Function<Throwable, E> exceptionMapper)
            throws E, InterruptedException {
        CompletableFuture<T> cf = new CompletableFuture<>();
        future.onComplete(ar -> {
            if (ar.succeeded()) {
                cf.complete(ar.result());
            } else {
                cf.completeExceptionally(ar.cause());
            }
        });
        try {
            return cf.get(timeout, unit);
        } catch (ExecutionException e) {
            throw exceptionMapper.apply(e.getCause());
        } catch (TimeoutException e) {
            throw exceptionMapper.apply(e);
        }
    }

    /**
     * Asynchronously delete the given pod, return a Future which completes when the Pod has been recreated.
     * Note: The pod might not be "ready" when the returned Future completes.
     *
     * @param pod            The pod to be restarted
     * @param restartContext
     * @return a Future which completes when the Pod has been recreated
     */
    protected Future<Void> restart(Pod pod, RestartContext restartContext) {
        return  podOperations.restart(reconciliation, pod, operationTimeoutMs)
                             .onComplete(i -> vertx.executeBlocking(ignored -> eventsPublisher.publishRestartEvents(pod, restartContext.restartReasons)));
    }

    /**
     * Returns an AdminClient instance bootstrapped from the given pod.
     */
    protected Admin adminClient(List<Integer> bootstrapPods, boolean ceShouldBeFatal) throws ForceableProblem, FatalProblem {
        List<String> podNames = bootstrapPods.stream().map(this::podName).collect(Collectors.toList());
        try {
            String bootstrapHostnames = podNames.stream().map(podName -> DnsNameGenerator.podDnsName(namespace, KafkaResources.brokersServiceName(cluster), podName) + ":" + KafkaCluster.REPLICATION_PORT).collect(Collectors.joining(","));
            LOGGER.debugCr(reconciliation, "Creating AdminClient for {}", bootstrapHostnames);
            return adminClientProvider.createAdminClient(bootstrapHostnames, this.clusterCaCertSecret, this.coKeySecret, "cluster-operator");
        } catch (KafkaException e) {
            if (ceShouldBeFatal && (e instanceof ConfigException
                    || e.getCause() instanceof ConfigException)) {
                throw new FatalProblem("An error while try to create an admin client with bootstrap brokers " + podNames, e);
            } else {
                throw new ForceableProblem("An error while try to create an admin client with bootstrap brokers " + podNames, e);
            }
        } catch (RuntimeException e) {
            throw new ForceableProblem("An error while try to create an admin client with bootstrap brokers " + podNames, e);
        }
    }

    protected KafkaAvailability availability(Admin ac) {
        return new KafkaAvailability(reconciliation, ac);
    }

    String podName(int podId) {
        return KafkaResources.kafkaPodName(this.cluster, podId);
    }
    
    /**
     * Return true if the given {@code podId} is the controller and there are other brokers we might yet have to consider.
     * This ensures that the controller is restarted/reconfigured last.
     */
    private boolean deferController(PodRef podRef, RestartContext restartContext) throws Exception {
        Integer controller = controller(podRef, operationTimeoutMs, TimeUnit.MILLISECONDS, restartContext);
        int stillRunning = podToContext.reduceValuesToInt(100, v -> v.promise.future().isComplete() ? 0 : 1,
                0, Integer::sum);
        return controller == podRef.getPodId() && stillRunning > 1;
    }

    /**
     * Completes the returned future <strong>on the context thread</strong> with the id of the controller of the cluster.
     * This will be -1 if there is not currently a controller.
     * @return A future which completes the the node id of the controller of the cluster,
     * or -1 if there is not currently a controller.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE") // seems to be completely spurious
    int controller(PodRef podRef, long timeout, TimeUnit unit, RestartContext restartContext) throws Exception {
        // Don't use all allClient here, because it will have cache metadata about which is the controller.
        try (Admin ac = adminClient(singletonList(podRef.getPodId()), false)) {
            Node controllerNode = null;
            try {
                DescribeClusterResult describeClusterResult = ac.describeCluster();
                KafkaFuture<Node> controller = describeClusterResult.controller();
                controllerNode = controller.get(timeout, unit);
                restartContext.clearConnectionError();
            } catch (ExecutionException | TimeoutException e) {
                maybeTcpProbe(podRef, e, restartContext);
            }
            int id = controllerNode == null || Node.noNode().equals(controllerNode) ? -1 : controllerNode.id();
            LOGGER.debugCr(reconciliation, "Controller is {}", id);
            return id;
        }
    }

    /**
     * If we've already had trouble connecting to this broker try to probe whether the connection is
     * open on the broker; if it's not then maybe throw a ForceableProblem to immediately force a restart.
     * This is an optimization for brokers which don't seem to be running.
     */
    private void maybeTcpProbe(PodRef podRef, Exception executionException, RestartContext restartContext) throws Exception {
        if (restartContext.connectionError() + podList.size() * 120_000L >= System.currentTimeMillis()) {
            try {
                LOGGER.debugCr(reconciliation, "Probing TCP port due to previous problems connecting to pod {}", podRef);
                // do a tcp connect and close (with a short connect timeout)
                tcpProbe(DnsNameGenerator.podDnsName(namespace, KafkaResources.brokersServiceName(cluster), podRef.getPodName()), KafkaCluster.REPLICATION_PORT);
            } catch (IOException connectionException) {
                throw new ForceableProblem("Unable to connect to " + podRef.getPodName() + ":" + KafkaCluster.REPLICATION_PORT, executionException.getCause(), true);
            }
            throw executionException;
        } else {
            restartContext.noteConnectionError();
            throw new ForceableProblem("Error while trying to determine the cluster controller from pod " + podRef.getPodName(), executionException.getCause());
        }
    }

    /**
     * Tries to open and close a TCP connection to the given host and port.
     * @param hostname The host
     * @param port The port
     * @throws IOException if anything went wrong.
     */
    /*test*/ void tcpProbe(String hostname, int port) throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(hostname, port), 5_000);
        } finally {
            socket.close();
        }
    }

    @Override
    public String toString() {
        return podToContext.toString();
    }

    protected Future<Void> isReady(Pod pod) {
        return isReady(pod.getMetadata().getNamespace(), pod.getMetadata().getName());
    }

    protected Future<Void> isReady(String namespace, String podName) {
        return podOperations.readiness(reconciliation, namespace, podName, pollingIntervalMs, operationTimeoutMs)
            .recover(error -> {
                LOGGER.warnCr(reconciliation, "Error waiting for pod {}/{} to become ready: {}", namespace, podName, error);
                return Future.failedFuture(error);
            });
    }

    protected static class PodRef {

        private final String podName;
        private final int podId;

        public PodRef(String podName, int podId) {
            this.podName = podName;
            this.podId = podId;
        }

        public String getPodName() {
            return podName;
        }

        public int getPodId() {
            return podId;
        }

        @Override
        public String toString() {
            return podName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PodRef podRef = (PodRef) o;
            return Objects.equals(podName, podRef.podName);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(podName);
        }
    }
}
