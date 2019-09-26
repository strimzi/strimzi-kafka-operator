/*
 * Copyright 2018, Strimzi authors.
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
 * For each pod:
 *   1. Test whether the pod needs to be restarted.
 *       If not then:
 *         1. Continue to the next pod
 *   2. Otherwise, check whether the pod is the controller
 *       If so, and there are still pods to be maybe-rolled then:
 *         1. Add this pod to the end of the list
 *         2. Continue to the next pod
 *   3. Otherwise, check whether the pod can be restarted without "impacting availability"
 *       If not then:
 *         1. Add this pod to the end of the list
 *         2. Continue to the next pod
 *   4. Otherwise:
 *       1 Restart the pod
 *       2. Wait for it to become ready (in the kube sense)
 *       3. Continue to the next pod
 * </pre>
 *
 * <p>"impacting availability" is defined by {@link KafkaAvailability}.</p>
 *
 * <p>Note this algorithm still works if there is a spontaneous
 * change in controller while the rolling restart is happening.</p>
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

    public Future<Void> rollingRestart(Predicate<Pod> podNeedsRestart) {
        this.podNeedsRestart = podNeedsRestart;
        List<Future> futures = new ArrayList<>(numPods);
        for (int podId = 0; podId < numPods; podId++) {
            futures.add(schedule(podId, 0, TimeUnit.MILLISECONDS));
        }
        // TODO shutdown executor
        Future<Void> result = Future.future();
        CompositeFuture.join(futures).setHandler(ar -> {
            singleExecutor.shutdown();
            //singleExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            result.handle(ar.map((Void) null));
        });
        return result;
    }

    private static class RestartContext {
        final Future<Void> future;
        final BackOff backOff;
        public RestartContext(Supplier<BackOff> backOffSupplier) {
            future = Future.future();
            backOff = backOffSupplier.get();
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
     * Schedule the rolling of the given pod at or after the given delay.
     * Pods will be rolled one-at-a-time.
     * @param podId The pod to roll.
     * @param delay The delay.
     * @param unit The unit of the delay.
     * @return A future which completes when the pod has been rolled.
     * When called multiple times with the same podId this method will return the same Future instance.
     */
    private Future<Void> schedule(int podId, long delay, TimeUnit unit) {
        RestartContext ctx = podToContext.computeIfAbsent(podId,
            k -> new RestartContext(backoffSupplier));
        singleExecutor.schedule(() -> {
            try {
                restartIfNecessary(podId, ctx.backOff.done());
                ctx.future.complete();
                log.debug("Roll of pod {} complete", podId);
            } catch (InterruptedException e) {
                // Let the executor deal with interruption.
                Thread.currentThread().interrupt();
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
     * Restart the given pod now if necessary according to {@link #podNeedsRestart}
     * @param podId The pod id
     * @throws ExecutionException Something went wrong
     * @throws InterruptedException Interrupted while waiting
     * @throws TimeoutException Timed out
     * @throws AbortRollException This pod can't be rolled right now.
     */
    private void restartIfNecessary(int podId, boolean finalAttempt) throws InterruptedException, TimeoutException,
            AdminClientException, NotRollableException, RestartException, ReadinessException,
            RollabilityException, IsControllerException, ControllerError {
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
                        throw new IsControllerException();
                    } else {
                        if (canRoll(adminClient, podId, 1, TimeUnit.MINUTES)) {
                            log.debug("Pod {} can be rolled now", podId);
                            restartWithPostBarrier(pod, 5, TimeUnit.MINUTES);
                        } else {
                            log.debug("Pod {} cannot be rolled right now", podId);
                            throw new NotRollableException();
                        }
                    }
                } finally {
                    if (adminClient != null) {
                        try {
                            adminClient.close(Duration.ofMinutes(2));
                        } catch (Exception e) {
                            log.warn("Ignoring exception when closing admin client", e);
                        }
                    }
                }
            } catch (AdminClientException | ControllerError | RollabilityException | IsControllerException e) {
                if (finalAttempt) {
                    restartWithPostBarrier(pod, 5, TimeUnit.MINUTES);
                } else {
                    throw e;
                }
            } catch (RestartException | ReadinessException | NotRollableException e) {
                throw e;
            }
        } else {
            log.debug("Pod {} does not need to be restarted", podId);
        }
    }

    /** The pod is currently the controller and there are other pods still to roll */
    static class IsControllerException extends Exception {
        IsControllerException() {}
    }

    /** An error while trying to determine rollability */
    static class RollabilityException extends Exception {

    }

    /** The pod is not rollable currently */
    static class NotRollableException extends Exception {

    }

    /** An error while trying to restart a pod */
    static class RestartException extends Exception {
        public RestartException(Throwable cause) {
            super(cause);
        }
    }

    /** An error while waiting for a pod to become ready */
    static class ReadinessException extends Exception {
        public ReadinessException(Throwable cause) {
            super(cause);
        }
    }

    /** An error while try to create/close the admin client */
    static class AdminClientException extends Exception {
        AdminClientException(Exception e) {
            super(e);
        }
    }

    /** An error while trying to determine the cluster controller */
    static class ControllerError extends Exception {
        ControllerError(Throwable e) {
            super(e);
        }
    }

    private boolean canRoll(AdminClient adminClient, int podId, long timeout, TimeUnit unit) throws RollabilityException, TimeoutException, InterruptedException {
        return await(availability(adminClient).canRoll(podId), timeout, unit,
            t -> new RollabilityException());
    }

    /**
     * Asynchronously apply the pre-restart barrier, then restart the given pod
     * by deleting it and letting it be recreated by K8s, then apply the post-restart barrier.
     * Return a Future which completes when the after restart callback for the given pod has completed.
     * @param pod The Pod to restart.
     * @return a Future which completes when the after restart callback for the given pod has completed.
     */
    private void restartWithPostBarrier(Pod pod, long timeout, TimeUnit unit)
            throws InterruptedException, RestartException, ReadinessException, TimeoutException {
        String podName = pod.getMetadata().getName();
        log.debug("Rolling pod {}", podName);
        await(restart(pod), timeout, unit, e -> new RestartException(e));
        String ssName = podName.substring(0, podName.lastIndexOf('-'));
        log.debug("Rolling update of {}/{}: wait for pod {} postcondition", namespace, ssName, podName);
        await(postRestartBarrier(pod), timeout, unit, e -> new ReadinessException(e));
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
            throws E, TimeoutException, InterruptedException {
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
    protected AdminClient adminClient(Integer podId) throws AdminClientException {
        try {
            String hostname = KafkaCluster.podDnsName(this.namespace, this.cluster, podName(podId)) + ":" + KafkaCluster.REPLICATION_PORT;
            log.debug("Creating AC for {}", hostname);
            return adminClientProvider.createAdminClient(hostname, this.clusterCaCertSecret, this.coKeySecret);
        } catch (RuntimeException e) {
            throw new AdminClientException(e);
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
    int controller(AdminClient ac, long timeout, TimeUnit unit) throws ControllerError, InterruptedException, TimeoutException {
        Node controllerNode = null;
        try {
            controllerNode = ac.describeCluster().controller().get(timeout, unit);
        } catch (ExecutionException e) {
            throw new ControllerError(e.getCause());
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
