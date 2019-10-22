/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

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
 *           i. Continue from 1.
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
 * <p>"impacting availability" is defined by {@link KafkaAvailability}.</p>
 *
 * <p>Note this algorithm still works if there is a spontaneous
 * change in controller while the rolling restart is happening, and it ensures the
 * controller is last pod to be rolled.</p>
 */
public class KafkaRoller2 {

    private static final Logger log = LogManager.getLogger(KafkaRoller2.class);
    private static final String NO_UID = "NULL";

    protected final PodOperator podOperations;
    protected final long pollingIntervalMs;
    protected final long operationTimeoutMs;
    protected final Vertx vertx;
    private final String cluster;
    private final Secret clusterCaCertSecret;
    private final Secret coKeySecret;
    private final Integer numPods;
    private final Supplier<BackOff> backoffSupplier;
    protected String namespace;
    private final AdminClientProvider adminClientProvider;

    public KafkaRoller2(Vertx vertx, PodOperator podOperations,
                        long pollingIntervalMs, long operationTimeoutMs, Supplier<BackOff> backOffSupplier,
                        StatefulSet ss, Secret clusterCaCertSecret, Secret coKeySecret) {
        this(vertx, podOperations, pollingIntervalMs, operationTimeoutMs, backOffSupplier,
                ss, clusterCaCertSecret, coKeySecret, new DefaultAdminClientProvider());
    }

    public KafkaRoller2(Vertx vertx, PodOperator podOperations,
                        long pollingIntervalMs, long operationTimeoutMs, Supplier<BackOff> backOffSupplier,
                        StatefulSet ss, Secret clusterCaCertSecret, Secret coKeySecret,
                        AdminClientProvider adminClientProvider) {
        this.namespace = ss.getMetadata().getNamespace();
        this.cluster = Labels.cluster(ss);
        this.numPods = ss.getSpec().getReplicas();
        this.backoffSupplier = backOffSupplier;
        this.clusterCaCertSecret = clusterCaCertSecret;
        this.coKeySecret = coKeySecret;
        this.vertx = vertx;
        this.operationTimeoutMs = operationTimeoutMs;
        this.podOperations = podOperations;
        this.pollingIntervalMs = pollingIntervalMs;
        this.adminClientProvider = adminClientProvider;
    }

    private static String getPodUid(Pod resource) {
        if (resource == null || resource.getMetadata() == null) {
            return NO_UID;
        }
        return resource.getMetadata().getUid();
    }

    /**
     * Returns a Future which completed with the actual pod corresponding to the abstract representation
     * of the given {@code pod}.
     */
    protected Future<Pod> pod(Integer podId) {
        return podOperations.getAsync(namespace, KafkaCluster.kafkaPodName(cluster, podId));
    }

    private final ScheduledExecutorService singleExecutor = Executors.newSingleThreadScheduledExecutor(
        runnable -> new Thread(runnable, "kafka-roller"));

    private ConcurrentHashMap<Integer, RestartContext> podToContext = new ConcurrentHashMap<>();
    private Predicate<Pod> podNeedsRestart;

    /**
     * Asynchronously perform a rolling restart of some subset of the pods,
     * completing the returned Future when rolling is complete.
     * Which pods get rolled is determined by {@code podNeedsRestart}.
     * The pods may not be rolled in id order, due to the {@linkplain KafkaRoller2 rolling algorithm}.
     * @param podNeedsRestart Predicate for determining whether a pod should be rolled.
     * @return A Future completed when rolling is complete.
     */
    public Future<Void> rollingRestart(Predicate<Pod> podNeedsRestart) {
        this.podNeedsRestart = podNeedsRestart;
        List<Future> futures = new ArrayList<>(numPods);
        for (int podId = 0; podId < numPods; podId++) {
            futures.add(schedule(podId, 0, TimeUnit.MILLISECONDS));
        }
        Future<Void> result = Future.future();
        CompositeFuture.join(futures).setHandler(ar -> {
            singleExecutor.shutdown();
            vertx.runOnContext(ignored -> {
                result.handle(ar.map((Void) null));
            });
        });
        return result;
    }

    private static class RestartContext {
        final Future<Void> future;
        final BackOff backOff;
        public RestartContext(Supplier<BackOff> backOffSupplier) {
            future = Future.future();
            backOff = backOffSupplier.get();
            backOff.delayMs();
        }

        @Override
        public String toString() {
            return "RestartContext{" +
                    "future=" + future +
                    ", backOff=" + backOff +
                    '}';
        }
    }

    /**
     * Schedule the rolling of the given pod at or after the given delay,
     * completed the returned Future when the pod is rolled.
     * When called multiple times with the same podId this method will return the same Future instance.
     * Pods will be rolled one-at-a-time so the delay may be overrun.
     * @param podId The pod to roll.
     * @param delay The delay.
     * @param unit The unit of the delay.
     * @return A future which completes when the pod has been rolled.
     */
    private Future<Void> schedule(int podId, long delay, TimeUnit unit) {
        RestartContext ctx = podToContext.computeIfAbsent(podId,
            k -> new RestartContext(backoffSupplier));
        singleExecutor.schedule(() -> {
            log.debug("Considering restart of pod {} after delay of {} {}", podId, delay, unit);
            try {
                restartIfNecessary(podId, ctx.backOff.done());
                ctx.future.complete();
            } catch (InterruptedException e) {
                // Let the executor deal with interruption.
                Thread.currentThread().interrupt();
            } catch (FatalException e) {
                log.info("Could not restart pod {}, giving up after {} attempts/{}ms",
                        podId, ctx.backOff.maxAttempts(), ctx.backOff.totalDelayMs(), e);
                ctx.future.fail(e);
                singleExecutor.shutdownNow();
                podToContext.forEachValue(100, f -> {
                    if (!f.future.isComplete()) {
                        f.future.fail(e);
                    }
                });
            } catch (Exception e) {
                if (ctx.backOff.done()) {
                    log.info("Could not roll pod {}, giving up after {} attempts/{}ms",
                            podId, ctx.backOff.maxAttempts(), ctx.backOff.totalDelayMs(), e);
                    ctx.future.fail(e instanceof TimeoutException ?
                            new io.strimzi.operator.common.operator.resource.TimeoutException() :
                            e);
                } else {
                    long delay1 = ctx.backOff.delayMs();
                    log.debug("Could not roll pod {} due to {}, retrying after at least {}ms",
                            podId, e, delay1);
                    schedule(podId, delay1, TimeUnit.MILLISECONDS);
                }
            }
        }, delay, unit);
        return ctx.future;
    }

    /**
     * Restart the given pod now if necessary according to {@link #podNeedsRestart}.
     * This method blocks.
     * @param podId The id of the pod to roll.
     * @param finalAttempt True if this is the last attempt to roll this pod.
     * @throws InterruptedException Interrupted while waiting.
     * @throws ForceableException Some error. Not thrown when finalAttempt==true.
     * @throws UnforceableException Some error, still thrown when finalAttempt==true.
     */
    private void restartIfNecessary(int podId, boolean finalAttempt)
            throws InterruptedException, ForceableException, UnforceableException, FatalException {
        Pod pod = podOperations.get(namespace, KafkaCluster.kafkaPodName(cluster, podId));

        if (podNeedsRestart.test(pod)) {
            log.debug("Pod {} needs to be restarted", podId);
            AdminClient adminClient = null;
            try {
                try {
                    adminClient = adminClient(podId);
                    Integer controller = controller(adminClient, 1, TimeUnit.MINUTES);
                    int stillRunning = podToContext.reduceValuesToInt(100, v -> v.future.isComplete() ? 0 : 1,
                            0, (x, y) -> x + y);
                    if (controller == podId && stillRunning > 1) {
                        log.debug("Pod {} is controller and there are other pods to roll", podId);
                        throw new ForceableException("The pod is currently the controller and there are other pods still to roll");
                    } else {
                        if (canRoll(adminClient, podId, 1, TimeUnit.MINUTES)) {
                            log.debug("Pod {} can be rolled now", podId);
                            restartWithPostBarrier(pod, 5, TimeUnit.MINUTES);
                        } else {
                            log.debug("Pod {} cannot be rolled right now", podId);
                            throw new UnforceableException("The pod currently is not rollable");
                        }
                    }
                } finally {
                    closeLoggingAnyError(adminClient);
                }
            } catch (ForceableException e) {
                if (finalAttempt) {
                    restartWithPostBarrier(pod, 5, TimeUnit.MINUTES);
                } else {
                    throw e;
                }
            }
        } else {
            log.debug("Pod {} does not need to be restarted", podId);
        }
    }

    private void closeLoggingAnyError(AdminClient adminClient) {
        if (adminClient != null) {
            try {
                adminClient.close(Duration.ofMinutes(2));
            } catch (Exception e) {
                log.warn("Ignoring exception when closing admin client", e);
            }
        }
    }

    /** Exceptions which we're prepared to ignore in the final attempt */
    static final class ForceableException extends Exception {
        ForceableException(String msg) {
            this(msg, null);
        }
        ForceableException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    /** Exceptions which we're prepared to ignore in the final attempt */
    static final class UnforceableException extends Exception {
        UnforceableException(String msg) {
            this(msg, null);
        }
        UnforceableException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    /** Immediately aborts rolling */
    static final class FatalException extends Exception {
        FatalException(String msg) {
            this(msg, null);
        }
        FatalException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    private boolean canRoll(AdminClient adminClient, int podId, long timeout, TimeUnit unit)
            throws ForceableException, InterruptedException {
        return await(availability(adminClient).canRoll(podId), timeout, unit,
            t -> new ForceableException("An error while trying to determine rollability", t));
    }

    /**
     * Asynchronously apply the pre-restart barrier, then restart the given pod
     * by deleting it and letting it be recreated by K8s, then apply the post-restart barrier.
     * Return a Future which completes when the after restart callback for the given pod has completed.
     * @param pod The Pod to restart.
     * @return a Future which completes when the after restart callback for the given pod has completed.
     */
    private void restartWithPostBarrier(Pod pod, long timeout, TimeUnit unit)
            throws InterruptedException, UnforceableException, FatalException {
        String podName = pod.getMetadata().getName();
        log.debug("Rolling pod {}", podName);
        await(restart(pod), timeout, unit, e -> new UnforceableException("An error while trying to restart a pod", e));
        String ssName = podName.substring(0, podName.lastIndexOf('-'));
        log.debug("Waiting for pod {} to become ready", podName);
        await(postRestartBarrier(pod), timeout, unit, e -> new FatalException("An error while waiting for a pod to become ready", e));
        log.debug("Pod {} is now ready", podName);
    }

    /**
     * Block waiting for up to the given timeout for the given Future to complete, returning its result.
     * @param future The future to wait for.
     * @param timeout The timeout
     * @param unit The timeout unit
     * @param exceptionMapper A function for rethrowing exceptions.
     * @param <T> The result type
     * @param <E> The exception type
     * @return
     * @throws E
     * @throws TimeoutException
     * @throws InterruptedException
     */
    static <T, E extends Exception> T await(Future<T> future, long timeout, TimeUnit unit,
                                            Function<Throwable, E> exceptionMapper)
            throws E, InterruptedException {
        CompletableFuture<T> cf = new CompletableFuture<>();
        future.setHandler(ar -> {
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
     * @return a Future which completes when the Pod has been recreated
     */
    protected Future<Void> restart(Pod pod) {
        long pollingIntervalMs = 1_000;
        String name = KafkaCluster.kafkaClusterName(cluster);
        String podName = pod.getMetadata().getName();
        Future<Void> deleteFinished = Future.future();
        log.info("Rolling update of {}/{}: Rolling pod {}", namespace, name, podName);

        // Determine generation of deleted pod
        String deleted = getPodUid(pod);

        // Delete the pod
        log.debug("Rolling update of {}/{}: Waiting for pod {} to be deleted", namespace, name, podName);
        Future<Void> podReconcileFuture =
            podOperations.reconcile(namespace, podName, null).compose(ignore -> {
                Future<Void> del = podOperations.waitFor(namespace, name, pollingIntervalMs, operationTimeoutMs, (ignore1, ignore2) -> {
                    // predicate - changed generation means pod has been updated
                    String newUid = getPodUid(podOperations.get(namespace, podName));
                    boolean done = !deleted.equals(newUid);
                    if (done) {
                        log.debug("Rolling pod {} finished", podName);
                    }
                    return done;
                });
                return del;
            });

        podReconcileFuture.setHandler(deleteResult -> {
            if (deleteResult.succeeded()) {
                log.debug("Rolling update of {}/{}: Pod {} was deleted", namespace, name, podName);
            }
            deleteFinished.handle(deleteResult);
        });
        return deleteFinished;
    }

    /**
     * Returns an AdminClient instance bootstrapped from the given pod.
     */
    protected AdminClient adminClient(Integer podId) throws ForceableException {
        try {
            String hostname = KafkaCluster.podDnsName(this.namespace, this.cluster, podName(podId)) + ":" + KafkaCluster.REPLICATION_PORT;
            log.debug("Creating AC for {}", hostname);
            return adminClientProvider.createAdminClient(hostname, this.clusterCaCertSecret, this.coKeySecret);
        } catch (RuntimeException e) {
            throw new ForceableException("An error while try to create the admin client", e);
        }
    }

    protected KafkaAvailability availability(AdminClient ac) {
        return new KafkaAvailability(ac);
    }

    protected String podName(Integer podId) {
        return KafkaCluster.kafkaPodName(this.cluster, podId);
    }

    /**
     * Completes the returned future <strong>on the context thread</strong> with the id of the controller of the cluster.
     * This will be -1 if there is not currently a controller.
     * @param ac The AdminClient
     * @return A future which completes the the node id of the controller of the cluster,
     * or -1 if there is not currently a controller.
     */
    int controller(AdminClient ac, long timeout, TimeUnit unit) throws ForceableException, InterruptedException {
        Node controllerNode = null;
        try {
            DescribeClusterResult describeClusterResult = ac.describeCluster();
            KafkaFuture<Node> controller = describeClusterResult.controller();
            controllerNode = controller.get(timeout, unit);
        } catch (ExecutionException e) {
            throw new ForceableException("An error while trying to determine the cluster controller", e.getCause());
        } catch (TimeoutException e) {
            throw new ForceableException("An error while trying to determine the cluster controller", e);
        }
        int id = Node.noNode().equals(controllerNode) ? -1 : controllerNode.id();
        log.debug("controller is {}", id);
        return id;
    }

    @Override
    public String toString() {
        return podToContext.toString();
    }

    protected Future<Void> postRestartBarrier(Pod pod) {
        String namespace = pod.getMetadata().getNamespace();
        String podName = pod.getMetadata().getName();
        return podOperations.readiness(namespace, podName, pollingIntervalMs, operationTimeoutMs)
            .recover(error -> {
                log.warn("Error waiting for pod {}/{} to become ready: {}", namespace, podName, error);
                return Future.failedFuture(error);
            });
    }

}
