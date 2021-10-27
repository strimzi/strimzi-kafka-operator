/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Function;
import java.util.function.Supplier;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

/**
 * <p>Manages the rolling restart or reconfiguration of a Kafka cluster.</p>
 *
 * <p>All the brokers in the cluster are considered for an action (restart or reconfigure).
 * (The reason we always consider <em>all</em> brokers is so that the regular reconciliation of a cluster done
 * by the CO means we may be able to resolve spontaneous problems in the cluster by restarting.)
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
 * Rather it considers all brokers that haven't yet been actioned and, based on their
 * <em>current</em> state, either actions them now, or defers them for reconsideration in the future.</p>
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
 * <p>Restart is done by deleting the pod on kubernetes. As such, the broker process is sent an initial SIGTERM
 * which begins graceful shutdown. If it has not shutdown within the {@code terminationGracePeriodSeconds} it is
 * sent a SIGKILL, likely resulting in log files not being closed cleanly and thus incurring
 * log recovery on start up.</p>
 *
 * <h4>Restart pre-conditions</h4>
 * <ul>
 *     <li>the restart must not result in any topics replicated on that broker from being under-replicated.</li>
 *     <li>TODO the broker must not be in log recovery</li>
 * </ul>
 * <p>If any of these preconditions are violated restart is deferred, as described above</p>
 *
 * <h4>Restart post-conditions</h4>
 * <ul>
 *     <li>the pod must become {@code Ready}</li>
 *     <li>the pod must become leader for all the partitions that is was leading before the restart (or, if that wasn't know, all the partitions)</li>
 *     <li>TODO figure out exactly what we want here.</li>
 * </ul>
 *
 * // TODO parallelism?? Also vertx?
 * The restart or reconfiguration of a individual broker is managed by a state machine whose states
 * are given by {@link BrokerActionContext.State}.
 * {@link BrokerActionContext#makeTransition(BrokerActionContext.State, long)} guarantees that each broker can only be restarted once.
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ParameterNumber"})
public class KafkaRoller {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaRoller.class);

    private final PodOperator podOperations;
    protected final long operationTimeoutMs;
    protected final Vertx vertx;
    private final String cluster;
    private final Secret clusterCaCertSecret;
    private final Secret coKeySecret;
    private final Integer numPods;
    protected final String namespace;
    private final KafkaAvailability kafkaAvailability;
    private final String kafkaConfig;
    private final String kafkaLogging;
    private final KafkaVersion kafkaVersion;
    private final Reconciliation reconciliation;
    private final boolean allowReconfiguration;
    private final Function<Pod, List<String>> podNeedsRestart;
    private final PriorityBlockingQueue<BrokerActionContext> queue;

    /* Since restarting a broker won't result in its instantaneous removal from observed ISR
     * (it will take a while for the leader to remove it from the ISR and for the metadata to propagate)
     * we need keep track of brokers which we're restarting, so we can correctly assess
     * whether the restart of broker C will impact availability.
     * This allows for some parallelism in restart.
     */
    Set<Integer> restartingBrokers = new HashSet<>();

    public KafkaRoller(Reconciliation reconciliation,
                       Vertx vertx,
                       PodOperator podOperations,
                       long pollingIntervalMs,
                       long operationTimeoutMs,
                       Supplier<BackOff> backOffSupplier,
                       StatefulSet sts,
                       Secret clusterCaCertSecret,
                       Secret coKeySecret,
                       AdminClientProvider adminClientProvider,
                       String kafkaConfig,
                       String kafkaLogging,
                       KafkaVersion kafkaVersion,
                       boolean allowReconfiguration,
                       Function<Pod, List<String>> podNeedsRestart) {
        this(reconciliation,
                vertx,
                podOperations,
                operationTimeoutMs,
                sts.getMetadata().getNamespace(),
                Labels.cluster(sts),
                sts.getSpec().getReplicas(),
                clusterCaCertSecret,
                coKeySecret,
                new Supplier<KafkaAvailability>() {
                    @Override
                    public KafkaAvailability get() {
                        var admin = adminClientProvider.createAdminClient(KafkaCluster.headlessServiceName(Labels.cluster(sts)),
                                clusterCaCertSecret, coKeySecret, "cluster-operator");
                        return new KafkaAvailability(reconciliation, vertx, admin);
                    }
                }.get(),
                kafkaConfig,
                kafkaLogging,
                kafkaVersion,
                allowReconfiguration,
                podNeedsRestart);
    }

    /* test */ KafkaRoller(Reconciliation reconciliation,
                       Vertx vertx,
                       PodOperator podOperations,
                       long operationTimeoutMs,
                       String namespace,
                       String cluster,
                       int replicas,
                       Secret clusterCaCertSecret,
                       Secret coKeySecret,
                       KafkaAvailability kafkaAvailability,
                       String kafkaConfig,
                       String kafkaLogging,
                       KafkaVersion kafkaVersion,
                       boolean allowReconfiguration,
                       Function<Pod, List<String>> podNeedsRestart) {
        this.namespace = namespace;
        this.cluster = cluster;
        this.numPods = replicas;
        this.clusterCaCertSecret = clusterCaCertSecret;
        this.coKeySecret = coKeySecret;
        this.vertx = vertx;
        this.operationTimeoutMs = operationTimeoutMs;
        this.podOperations = podOperations;
        this.kafkaAvailability = kafkaAvailability;
        this.kafkaConfig = kafkaConfig;
        this.kafkaLogging = kafkaLogging;
        this.kafkaVersion = kafkaVersion;
        this.reconciliation = reconciliation;
        this.allowReconfiguration = allowReconfiguration;
        this.queue = new PriorityBlockingQueue<>(numPods,
                Comparator.comparing(BrokerActionContext::state)
                        .thenComparing(BrokerActionContext::notBefore)
                        .thenComparing(BrokerActionContext::podId));
        this.podNeedsRestart = podNeedsRestart;
    }

    /**
     * Returns a Future which completed with the actual pod corresponding to the abstract representation
     * of the given {@code pod}.
     */
    protected Future<Pod> pod(Integer podId) {
        return podOperations.getAsync(namespace, KafkaCluster.kafkaPodName(cluster, podId));
    }

     /**
     * Asynchronously perform a rolling restart of some subset of the pods,
     * completing the returned Future when rolling is complete.
     * Which pods get rolled is determined by {@code podNeedsRestart}.
     * The pods may not be rolled in id order, due to the {@linkplain KafkaRoller rolling algorithm}.
     * @return A Future completed when rolling is complete.
     */
    public Future<Void> rollingRestart() {
        Promise<Void> rollResult = Promise.promise();
        BrokerActionContext.assertEventLoop(vertx);
        // There's nowhere for the group state to reside
        buildQueue().compose(i -> {
            vertx.setTimer(1, new Handler<>() {
                @Override
                public void handle(Long timerId) {
                    try {
                        progressOneContext().compose(
                            delay -> {
                                if (delay != null) {
                                    vertx.setTimer(delay, this);
                                } else {
                                    rollResult.complete();
                                }
                                return null;
                            },
                            error -> {
                                rollResult.fail(error);
                                return null;
                            });
                    } catch (Exception e) {
                        rollResult.tryFail(e);
                    }
                }
            });
            return Future.succeededFuture();
        }).recover(error -> {
            rollResult.fail(error);
            return Future.failedFuture(error);
        });
        return rollResult.future().eventually(i -> vertx.executeBlocking(promise -> {
            try {
                LOGGER.debugCr(reconciliation, "Closing KafkaAvailability");
                kafkaAvailability.close(Duration.ofSeconds(30));
                promise.complete();
            } catch (Exception e) {
                promise.fail(e);
            }
        }));
    }

    /* test */ PriorityBlockingQueue<BrokerActionContext> queue() {
        return queue;
    }

    /**
     * Build an initial list of contexts.
     */
    /* test */ Future<Void> buildQueue() {
        return vertx.executeBlocking(p -> {
            // TODO why is this executeBlocking?
            for (int podId = 0; podId < numPods; podId++) {
                // Order the podIds unready first otherwise repeated reconciliations might each restart a pod
                // only for it not to become ready and thus drive the cluster to a worse state.
                // TODO a check for log recovery
                BrokerActionContext context = new BrokerActionContext(vertx,
                        podOperations,
                        operationTimeoutMs,
                        allowReconfiguration,
                        reconciliation,
                        podId,
                        KafkaCluster.kafkaPodName(this.cluster, podId),
                        kafkaVersion,
                        kafkaConfig,
                        kafkaLogging,
                        podNeedsRestart,
                        kafkaAvailability,
                        restartingBrokers);
                queue.add(context);
            }
            p.complete(null);
        });
    }

    /**
     * Polls the queue for the next context to be run.
     * If there is a next context then calls {@link BrokerActionContext#progressOne()} on it, re-queues it and
     * the returned Future is completed with the delay before that context should next be progressed
     * If there is not a next context then the returned Future is completed with null and the rolling is complete.
     * If an error occurs such that the rolling should be stopped (e.g. the existence of unschedulable pods) then
     * the returned Future is failed.
     */
    /* test */ Future<Long> progressOneContext() {
        BrokerActionContext.assertEventLoop(vertx);
        var context = queue.poll();
        if (context == null) {
            LOGGER.debugCr(reconciliation, "All rolling actions complete");
            return Future.succeededFuture();
        } else {
            if (context.state().isEndState()) {
                if (context.state() == BrokerActionContext.State.UNSCHEDULABLE) {
                    return Future.failedFuture(new RuntimeException("Unschedulable pod " + context.podId()));
                } else {
                    LOGGER.debugCr(reconciliation, "Rolling action on {} complete", context.podId());
                    // Because of the order of the queue the only way to see DONE if is every context is done
                    return Future.succeededFuture();
                }
            } else {
                LOGGER.debugCr(reconciliation, "Progressing {}", context);
                return context.progressOne().compose(i -> {
                    BrokerActionContext.assertEventLoop(vertx);
                    LOGGER.debugCr(reconciliation, "Progressed {}", context);
                    return Future.succeededFuture(requeue(context));
                });
            }
        }
    }

    private long requeue(BrokerActionContext context) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debugCr(reconciliation, "Requeuing {}", context);
        }
        queue.add(context);
        return context.actualDelayMs();
    }
}
