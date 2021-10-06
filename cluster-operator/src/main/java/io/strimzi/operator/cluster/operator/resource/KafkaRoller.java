/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;

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
 *     <li>the pod must become leader for all the partitions that is was leading before the restart (or, if that wasn't know, all the partitions</li>
 *     TODO figure out exactly what we want here.
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
    protected String namespace;
    private final AdminClientProvider adminClientProvider;
    private final String kafkaConfig;
    private final String kafkaLogging;
    private final KafkaVersion kafkaVersion;
    private final Reconciliation reconciliation;
    private final boolean allowReconfiguration;
    private Admin _allClient;
    private Function<Pod, List<String>> podNeedsRestart;

    /* Since restarting a broker won't result in its instantaneous removal from observed ISR
     * (it will take a while for the leader to remove it from the ISR and for the metadata to propagate)
     * we need keep track of brokers which we're restarting, so we can correctly assess
     * whether the restart of broker C will impact availability.
     * This allows for some parallelism in restart.
     */
    Set<Integer> restartingBrokers = new HashSet<>();
    int parallelism = 2;

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

    private Admin adminClient() {
        if (this._allClient == null) {
            this._allClient = adminClientProvider.createAdminClient(KafkaCluster.headlessServiceName(cluster),
                    this.clusterCaCertSecret, this.coKeySecret, "cluster-operator");
        }
        return this._allClient;
    }

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
        BrokerActionContext.assertEventLoop(vertx);
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
                    BrokerActionContext.assertEventLoop(vertx);
                    var context = queue.remove();
                    if (context.state().isEndState()) {
                        // TODO handle unschedulable -> We should end up with some condition in the status.
                        // The head of the queue is done, due to the ordering that means all the contexts are done done.
                        LOGGER.debugCr(reconciliation, "Rolling actions complete");
                        result.complete();
                    }
                    LOGGER.debugCr(reconciliation, "Progressing {}", context);

                    context.progressOne().map(i -> {
                        BrokerActionContext.assertEventLoop(vertx);
                        requeue(context);
                        return null;
                    });
                }

                private void requeue(BrokerActionContext context) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debugCr(reconciliation, "Requeuing {}", context);
                    }
                    queue.add(context);
                    vertx.setTimer(context.actualDelayMs(), this);
                }
            };
            vertx.setTimer(0, longHandler);
            return Future.succeededFuture();
        });
        return result.future().eventually(i -> vertx.executeBlocking(promise -> {
            try {
                _allClient.close(Duration.ofSeconds(30));
                promise.complete();
            } catch (Exception e) {
                promise.fail(e);
            }
        }));
    }

    /**
     * Build an initial list of contexts.
     */
    private Future<PriorityQueue<BrokerActionContext>> buildQueue() {
        return vertx.executeBlocking(p -> {
            var queue = new PriorityQueue<>(numPods,
                    Comparator.comparing(BrokerActionContext::state)
                            .thenComparing(BrokerActionContext::notBefore)
                            .thenComparing(BrokerActionContext::podId));
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
                        new KafkaAvailability(reconciliation, vertx, adminClient()),
                        restartingBrokers);
                queue.add(context);
            }
            p.complete(queue);
        });
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

        ForceableProblem(String msg, boolean forceNow) {
            this(msg, null, forceNow);
        }

        ForceableProblem(String msg, Throwable cause, boolean forceNow) {
            super(msg, cause);
            this.forceNow = forceNow;
        }
    }

}
