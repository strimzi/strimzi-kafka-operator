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
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.PodOperator;
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
    private final List<String> podNames;
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

    public KafkaRoller(Reconciliation reconciliation,
                       Vertx vertx,
                       PodOperator podOperations,
                       long pollingIntervalMs,
                       long operationTimeoutMs,
                       Supplier<BackOff> backOffSupplier,
                       List<String> podNames,
                       Secret clusterCaCertSecret,
                       Secret coKeySecret,
                       AdminClientProvider adminClientProvider,
                       Function<Integer, String> kafkaConfigProvider,
                       String kafkaLogging,
                       KafkaVersion kafkaVersion,
                       boolean allowReconfiguration,
                       KubernetesRestartEventPublisher eventsPublisher) {
        this.namespace = reconciliation.namespace();
        this.cluster = reconciliation.name();
        this.podNames = podNames;
        this.eventsPublisher = eventsPublisher;
        if (podNames.size() != podNames.stream().distinct().count()) {
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

    private final ConcurrentHashMap<String, RestartContext> podToContext = new ConcurrentHashMap<>();

    /**
     * If allClient has not been initialized yet, does exactly that
     * @return true if the creation of AC succeeded, false otherwise
     */
    private boolean initAdminClient() {
        if (this.allClient == null) {
            try {
                this.allClient = adminClient(IntStream.range(0, podNames.size()).boxed().collect(Collectors.toList()), false);
            } catch (ForceableProblem | FatalProblem e) {
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
        Promise<Void> result = Promise.promise();
        singleExecutor.submit(() -> {
            List<PodRef> pods = new ArrayList<>(podNames.size());

            for (String podName : podNames) {
                // Order the podNames unready first otherwise repeated reconciliations might each restart a pod
                // only for it not to become ready and thus drive the cluster to a worse state.
                pods.add(podOperations.isReady(namespace, podName) ? pods.size() : 0, new PodRef(podName, idOfPod(podName)));
            }
            LOGGER.debugCr(reconciliation, "Initial order for rolling restart {}", pods);
            List<Future<Void>> futures = new ArrayList<>(podNames.size());
            for (PodRef podRef: pods) {
                futures.add(schedule(podRef, 0, TimeUnit.MILLISECONDS, podNeedsRestart));
            }
            Util.compositeFuture(futures).onComplete(ar -> {
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
    private Future<Void> schedule(PodRef podRef, long delay, TimeUnit unit, Function<Pod, RestartReasons> podNeedsRestart) {
        RestartContext context = podToContext.computeIfAbsent(podRef.getPodName(), k -> new RestartContext(backoffSupplier));
        return schedule(context, podRef, delay, unit, podNeedsRestart);
    }

    // Break out the above override to pass context explicitly in recursive scheduling call
    private Future<Void> schedule(RestartContext context, PodRef podRef, long delay, TimeUnit unit, Function<Pod, RestartReasons> podNeedsRestart) {
        singleExecutor.schedule(() -> {
            RestartContext ctx = context;
            LOGGER.debugCr(reconciliation, "Considering restart of pod {} after delay of {} {}", podRef, delay, unit);
            try {
                Pod pod = loadPod(podRef);
                RestartReasons reasons = podNeedsRestart.apply(pod);

                //this method call will modify context by side effect
                //noinspection ConstantConditions - I'm reassigning on purpose to indicate modification of context
                ctx = restartOrDynamicallyReconfigure(podRef, pod, ctx, reasons);
                restartIfNecessary(podRef, pod, ctx);

                ctx.getPromise().complete();
            } catch (InterruptedException e) {
                // Let the executor deal with interruption.
                Thread.currentThread().interrupt();
            } catch (FatalProblem e) {
                LOGGER.infoCr(reconciliation, "Could not restart pod {}, giving up after {} attempts. Total delay between attempts {}ms",
                        podRef, ctx.getBackOff().maxAttempts(), ctx.getBackOff().totalDelayMs(), e);
                ctx.getPromise().fail(e);
                singleExecutor.shutdownNow();
                podToContext.forEachValue(Integer.MAX_VALUE, f -> f.getPromise().tryFail(e));
            } catch (Exception e) {
                if (ctx.getBackOff().done()) {
                    LOGGER.infoCr(reconciliation, "Could not roll pod {}, giving up after {} attempts. Total delay between attempts {}ms",
                            podRef, ctx.getBackOff().maxAttempts(), ctx.getBackOff().totalDelayMs(), e);
                    if (e instanceof TimeoutException) {
                        ctx.getPromise().fail(new io.strimzi.operator.common.operator.resource.TimeoutException());
                    } else {
                        ctx.getPromise().fail(e);
                    }
                } else {
                    long delay1 = ctx.getBackOff().delayMs();
                    LOGGER.infoCr(reconciliation, "Could not roll pod {} due to {}, retrying after at least {}ms",
                            podRef, e, delay1);
                    schedule(ctx, podRef, delay1, TimeUnit.MILLISECONDS, podNeedsRestart);
                }
            }
        }, delay, unit);
        return context.getPromise().future();
    }

    /**
     * Restart the given pod now if necessary
     * This method blocks.
     * @param podRef Reference of pod to roll.
     * @param pod Pod being rolled
     * @param context context with information about why we're rolling, if at all, and how
     * @throws InterruptedException Interrupted while waiting.
     * @throws ForceableProblem Some error. Not thrown when finalAttempt==true.
     * @throws UnforceableProblem Some error, still thrown when finalAttempt==true.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @SuppressWarnings({"checkstyle:CyclomaticComplexity"})
    private void restartIfNecessary(PodRef podRef, Pod pod, RestartContext context) throws Exception {

        try {
            if (context.needsForceRestart()) {
                LOGGER.debugCr(reconciliation, "Forcing restart for Pod {}", podRef);
                restartAndAwaitReadiness(pod, operationTimeoutMs, TimeUnit.MILLISECONDS, context.getRestartReasons());
                return;
            }

            if (context.needsRestart() || context.needsReconfig()) {
                if (isController(podRef, context)) {
                    LOGGER.debugCr(reconciliation, "Pod {} is controller and there are other pods to roll", podRef);
                    throw new ForceableProblem("Pod " + podRef.getPodName() + " is currently the controller and there are other pods still to roll");
                }

                // Check for rollability before trying a dynamic update so that if the dynamic update fails we can go to a full restart
                if (!canRoll(podRef, 60_000, TimeUnit.MILLISECONDS, false)) {
                    LOGGER.debugCr(reconciliation, "Pod {} cannot be rolled right now", podRef);
                    throw new UnforceableProblem("Pod " + podRef.getPodName() + " is currently not rollable");
                }

                // Passing fields as explicit parameters is deliberate, to help code reading
                // Try to update the broker config, and if it fails, proceed to restart
                if (context.needsReconfig() && maybeDynamicUpdateBrokerConfig(podRef.getPodId(), context.getDiff(), context.getLogDiff())) {
                    awaitReadiness(pod, operationTimeoutMs, TimeUnit.MILLISECONDS);
                } else {
                    LOGGER.debugCr(reconciliation, "Pod {} can be rolled now", podRef);
                    restartAndAwaitReadiness(pod, operationTimeoutMs, TimeUnit.MILLISECONDS, context.getRestartReasons());
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
            if (isPodStuck(pod) || context.getBackOff().done() || e.forceNow) {
                String errorMessage = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                if (canRoll(podRef, 60_000, TimeUnit.MILLISECONDS, true)) {
                    LOGGER.warnCr(reconciliation, "Pod {} will be force-rolled, due to error: {}", podRef, errorMessage);
                    RestartReasons withError = context.getRestartReasons().add(RestartReason.POD_FORCE_RESTART_ON_ERROR, errorMessage);
                    restartAndAwaitReadiness(pod, operationTimeoutMs, TimeUnit.MILLISECONDS, withError);
                } else {
                    LOGGER.warnCr(reconciliation, "Pod {} can't be safely force-rolled; original error: ", podRef, errorMessage);
                    throw e;
                }
            } else {
                throw e;
            }
        }
    }

    private Pod loadPod(PodRef podRef) throws UnforceableProblem {
        Pod pod;
        try {
            pod = podOperations.get(namespace, podRef.getPodName());
        } catch (KubernetesClientException e) {
            throw new UnforceableProblem("Error getting pod " + podRef.getPodName(), e);
        }
        return pod;
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

    private int idOfPod(String podName)  {
        return Integer.parseInt(podName.substring(podName.lastIndexOf("-") + 1));
    }

    private boolean isPending(Pod pod) {
        return pod != null && pod.getStatus() != null && "Pending".equals(pod.getStatus().getPhase());
    }

    private boolean isPodStuck(Pod pod) {
        Set<String> set = new HashSet<>();
        set.add("CrashLoopBackOff");
        set.add("ImagePullBackOff");
        set.add("ContainerCreating");
        return isPending(pod) || podWaitingBecauseOfAnyReasons(pod, set);
    }

    /**
     * Dynamically update the broker config
     * Return false if the broker was unsuccessfully updated dynamically.
     */
    private boolean maybeDynamicUpdateBrokerConfig(int brokerId, KafkaBrokerConfigurationDiff diff, KafkaBrokerLoggingConfigurationDiff logDiff) throws InterruptedException {
        try {
            dynamicUpdateBrokerConfig(brokerId, allClient, diff, logDiff);
            return true;
        } catch (ForceableProblem e) {
            LOGGER.debugCr(reconciliation, "Pod {} could not be updated dynamically ({}), will restart", brokerId, e);
            return false;
        }
    }

    /**
     * Determine whether the pod should be restarted, or the broker reconfigured.
     * Modifies the passed in context by side effect
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private RestartContext restartOrDynamicallyReconfigure(PodRef podRef, Pod pod, RestartContext restartContext, RestartReasons reasonsToRestartPod) throws ForceableProblem, InterruptedException, FatalProblem {

        //TODO should pod ever be null here?
        boolean podStuck = pod != null
                && pod.getStatus() != null
                && "Pending".equals(pod.getStatus().getPhase())
                && pod.getStatus().getConditions().stream().anyMatch(ps ->
                "PodScheduled".equals(ps.getType())
                        && "Unschedulable".equals(ps.getReason())
                        && "False".equals(ps.getStatus()));
        if (podStuck
                && !reasonsToRestartPod.contains(RestartReason.POD_HAS_OLD_GENERATION) // "Pod has old generation" is used with StatefulSets
                && !reasonsToRestartPod.contains(RestartReason.POD_HAS_OLD_REVISION)) {  // "Pod has old revision" is used with PodSets
            // If the pod is unschedulable then deleting it, or trying to open an Admin client to it will make no difference
            // Treat this as fatal because if it's not possible to schedule one pod then it's likely that proceeding
            // and deleting a different pod in the meantime will likely result in another unschedulable pod.
            throw new FatalProblem("Pod is unschedulable");
        }
        // Unless the annotation is present, check the pod is at least ready.
        boolean needsRestart = reasonsToRestartPod.shouldRoll();
        KafkaBrokerConfigurationDiff diff = null;
        KafkaBrokerLoggingConfigurationDiff loggingDiff = null;
        boolean needsReconfig = false;
        // Always get the broker config. This request gets sent to that specific broker, so it's a proof that we can
        // connect to the broker and that it's capable of responding.
        if (!initAdminClient()) {
            LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted, because it does not seem to responding to connection attempts", podRef);
            restartContext.setNeedsRestart(false);
            restartContext.setNeedsReconfig(false);

            //TODO - figure out a way to handle this in a nicer manner
            restartContext.setForceRestart(true);
            restartContext.setRestartReasons(reasonsToRestartPod.add(RestartReason.POD_UNRESPONSIVE));
            restartContext.setDiff(null);
            restartContext.setLogDiff(null);
            return restartContext;
        }
        Config brokerConfig;
        try {
            brokerConfig = brokerConfig(podRef);
        } catch (ForceableProblem e) {
            if (restartContext.getBackOff().done()) {
                // Will restart due to same reason as above (presumable broker connectivity issues), based on old comment above previous admin check
                restartContext.setRestartReasons(reasonsToRestartPod.add(RestartReason.POD_UNRESPONSIVE));
                needsRestart = true;
                brokerConfig = null;
            } else {
                throw e;
            }
        }

        if (!needsRestart && allowReconfiguration) {
            LOGGER.traceCr(reconciliation, "Broker {}: description {}", podRef, brokerConfig);
            diff = new KafkaBrokerConfigurationDiff(reconciliation, brokerConfig, kafkaConfigProvider.apply(podRef.getPodId()), kafkaVersion, podRef.getPodId());
            loggingDiff = logging(podRef);

            if (diff.getDiffSize() > 0) {
                if (diff.canBeUpdatedDynamically()) {
                    LOGGER.debugCr(reconciliation, "Pod {} needs to be reconfigured.", podRef);
                    needsReconfig = true;
                } else {
                    LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted, because reconfiguration cannot be done dynamically", podRef);
                    reasonsToRestartPod.add(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART);
                    needsRestart = true;
                }
            }

            // needsRestart value might have changed from the check in the parent if. So we need to check it again.
            if (!needsRestart && loggingDiff.getDiffSize() > 0) {
                LOGGER.debugCr(reconciliation, "Pod {} logging needs to be reconfigured.", podRef);
                needsReconfig = true;
            }
        } else if (needsRestart) {
            LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted. Reason: {}", podRef, reasonsToRestartPod.getAllReasonNotes());
        }

        if (podStuck)   {
            LOGGER.infoCr(reconciliation, "Pod {} needs to be restarted, because it seems to be stuck and restart might help", podRef);
            reasonsToRestartPod.add(RestartReason.POD_STUCK);
        }

        restartContext.setRestartReasons(reasonsToRestartPod);
        restartContext.setNeedsRestart(needsRestart);
        restartContext.setNeedsReconfig(needsReconfig);
        restartContext.setForceRestart(podStuck);
        restartContext.setDiff(diff);
        restartContext.setLogDiff(loggingDiff);
        return restartContext;
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

    protected void dynamicUpdateBrokerConfig(int podId, Admin ac, KafkaBrokerConfigurationDiff configurationDiff, KafkaBrokerLoggingConfigurationDiff logDiff)
            throws ForceableProblem, InterruptedException {
        Map<ConfigResource, Collection<AlterConfigOp>> updatedConfig = new HashMap<>(2);
        updatedConfig.put(Util.getBrokersConfig(podId), configurationDiff.getConfigDiff());
        updatedConfig.put(Util.getBrokersLogging(podId), logDiff.getLoggingDiff());

        LOGGER.debugCr(reconciliation, "Altering broker configuration {}", podId);
        LOGGER.traceCr(reconciliation, "Altering broker configuration {} with {}", podId, updatedConfig);

        AlterConfigsResult alterConfigResult = ac.incrementalAlterConfigs(updatedConfig);
        KafkaFuture<Void> brokerConfigFuture = alterConfigResult.values().get(Util.getBrokersConfig(podId));
        KafkaFuture<Void> brokerLoggingConfigFuture = alterConfigResult.values().get(Util.getBrokersLogging(podId));
        await(Util.kafkaFutureToVertxFuture(reconciliation, vertx, brokerConfigFuture), 30, TimeUnit.SECONDS,
            error -> {
                LOGGER.errorCr(reconciliation, "Error doing dynamic config update", error);
                return new ForceableProblem("Error doing dynamic update", error);
            });
        await(Util.kafkaFutureToVertxFuture(reconciliation, vertx, brokerLoggingConfigFuture), 30, TimeUnit.SECONDS,
            error -> {
                LOGGER.errorCr(reconciliation, "Error performing dynamic logging update for pod {}", podId, error);
                return new ForceableProblem("Error performing dynamic logging update for pod " + podId, error);
            });

        LOGGER.infoCr(reconciliation, "Dynamic reconfiguration for broker {} was successful.", podId);
    }

    private KafkaBrokerLoggingConfigurationDiff logging(PodRef podRef)
            throws ForceableProblem, InterruptedException {
        Config brokerLogging = brokerLogging(podRef.getPodId());
        LOGGER.traceCr(reconciliation, "Broker {}: logging description {}", podRef, brokerLogging);
        return new KafkaBrokerLoggingConfigurationDiff(reconciliation, brokerLogging, kafkaLogging, podRef.getPodId());
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

    private boolean canRoll(PodRef podRef, long timeout, TimeUnit unit, boolean ignoreSslError)
            throws ForceableProblem, InterruptedException {
        try {
            return await(availability(allClient).canRoll(podRef.getPodId()), timeout, unit,
                t -> new ForceableProblem("An error while trying to determine rollability", t));
        } catch (ForceableProblem e) {
            // If we're not able to connect then roll
            if (ignoreSslError && e.getCause() instanceof SslAuthenticationException) {
                return true;
            } else {
                throw e;
            }
        }
    }

    /**
     * Synchronously restart the given pod
     * by deleting it and letting it be recreated by K8s, then synchronously wait for it to be ready.
     * @param pod The Pod to restart.
     * @param timeout The timeout.
     * @param unit The timeout unit.
     * @param restartReasons why the pod is being restarted
     */
    private void restartAndAwaitReadiness(Pod pod, long timeout, TimeUnit unit, RestartReasons restartReasons)
            throws InterruptedException, UnforceableProblem, FatalProblem {
        String podName = pod.getMetadata().getName();
        LOGGER.debugCr(reconciliation, "Rolling pod {}", podName);
        await(restart(pod, restartReasons), timeout, unit, e -> new UnforceableProblem("Error while trying to restart pod " + podName + " to become ready", e));
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
     * @return The result of the future
     * @throws E The exception type returned from {@code exceptionMapper}.
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
     * @param pod The pod to be restarted
     * @param restartReasons why the pod is being restarted
     * @return a Future which completes when the Pod has been recreated
     */
    protected Future<Void> restart(Pod pod, RestartReasons restartReasons) {
        return podOperations.restart(reconciliation, pod, operationTimeoutMs).onSuccess(v -> vertx.executeBlocking(p -> {
            eventsPublisher.publishRestartEvents(pod, restartReasons);
            p.complete();
        }));
    }

    /**
     * Returns an AdminClient instance bootstrapped from the given pod.
     */
    protected Admin adminClient(List<Integer> bootstrapPods, boolean configExceptionShouldBeFatal) throws ForceableProblem, FatalProblem {
        List<String> podNames = bootstrapPods.stream().map(this::podName).collect(Collectors.toList());
        try {
            String bootstrapHostnames = podNames.stream().map(podName -> DnsNameGenerator.podDnsName(namespace, KafkaResources.brokersServiceName(cluster), podName) + ":" + KafkaCluster.REPLICATION_PORT).collect(Collectors.joining(","));
            LOGGER.debugCr(reconciliation, "Creating AdminClient for {}", bootstrapHostnames);
            return adminClientProvider.createAdminClient(bootstrapHostnames, this.clusterCaCertSecret, this.coKeySecret, "cluster-operator");
        } catch (KafkaException e) {
            if (configExceptionShouldBeFatal && (e instanceof ConfigException
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
    private boolean isController(PodRef podRef, RestartContext restartContext) throws Exception {
        int controller = controller(podRef, operationTimeoutMs, TimeUnit.MILLISECONDS, restartContext);
        int stillRunning = podToContext.reduceValuesToInt(100, v -> v.getPromise().future().isComplete() ? 0 : 1,
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
        if (restartContext.connectionError() + podNames.size() * 120_000L >= System.currentTimeMillis()) {
            try {
                LOGGER.debugCr(reconciliation, "Probing TCP port due to previous problems connecting to pod {}", podRef);
                // do a tcp connect and close (with a short connect timeout)
                tcpProbe(podRef.getPodName(), KafkaCluster.REPLICATION_PORT);
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
    void tcpProbe(String hostname, int port) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(hostname, port), 5_000);
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

    public static class PodRef {

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
            return "{" +
                    podName +
                    '}';
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
