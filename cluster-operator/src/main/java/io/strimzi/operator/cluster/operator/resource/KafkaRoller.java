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
import io.strimzi.operator.common.MaxAttemptsExceededException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Node;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;

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
public class KafkaRoller {

    private static final Logger log = LogManager.getLogger(KafkaRoller.class);
    private static final String NO_UID = "NULL";

    protected final PodOperator podOperations;
    protected final long pollingIntervalMs;
    protected final long operationTimeoutMs;
    protected final Vertx vertx;
    private final String cluster;
    private final Secret clusterCaCertSecret;
    private final Secret coKeySecret;
    private final PriorityQueue<RestartAttempt> queue = new PriorityQueue<>();
    protected String namespace;
    private final AdminClientProvider adminClientProvider;

    public KafkaRoller(Vertx vertx, PodOperator podOperations,
                       long pollingIntervalMs, long operationTimeoutMs, Supplier<BackOff> backOffSupplier,
                       StatefulSet ss, Secret clusterCaCertSecret, Secret coKeySecret) {
        this(vertx, podOperations, pollingIntervalMs, operationTimeoutMs, backOffSupplier,
                ss, clusterCaCertSecret, coKeySecret, new DefaultAdminClientProvider());
    }

    public KafkaRoller(Vertx vertx, PodOperator podOperations,
                       long pollingIntervalMs, long operationTimeoutMs, Supplier<BackOff> backOffSupplier,
                       StatefulSet ss, Secret clusterCaCertSecret, Secret coKeySecret,
                       AdminClientProvider adminClientProvider) {
        this.namespace = ss.getMetadata().getNamespace();
        this.cluster = Labels.cluster(ss);
        initPods(range(0, ss.getSpec().getReplicas()).boxed().collect(Collectors.toList()), backOffSupplier);
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

    /**
     * Perform a rolling restart of the pods in the given StatefulSet.
     * Pods will be tested for whether they really need rolling using the given {@code podNeedsRestart}.
     * If a pod does indeed need restarting {@link #postRestartBarrier(Pod)} is called afterwards
     * (meaning we wait for the rolled pod to be ready).
     * The returned Future is completed when the rolling restart is finished.
     * @param podNeedsRestart Predicate for deciding whether the pod needs to be restarted.
     * @return A future which completes when all the required pods have been restarted.
     */
    public Future<Void> rollingRestart(
                                Predicate<Pod> podNeedsRestart) {
        Function<Void, Future<KafkaRoller>> x = new Function<Void, Future<KafkaRoller>>() {
            @Override
            public Future<KafkaRoller> apply(Void ignored) {
                return next(podNeedsRestart).compose(podId -> {
                    if (podId == null) {
                        log.debug("No more pods to restart");
                        return Future.succeededFuture();
                    } else {
                        return pod(podId).compose(p -> {
                            log.debug("Rolling pod {} (still to consider: {})", p.getMetadata().getName(), KafkaRoller.this);
                            Future<Void> f = restartWithPostBarrier(p);
                            return f.compose(this);
                        });
                    }
                });
            }
        };
        try {
            return x.apply(null).map((Void) null);
        } catch (Throwable t) {
            return Future.failedFuture(t);
        }
    }

    /**
     * Asynchronously apply the pre-restart barrier, then restart the given pod
     * by deleting it and letting it be recreated by K8s, then apply the post-restart barrier.
     * Return a Future which completes when the after restart callback for the given pod has completed.
     * @param pod The Pod to restart.
     * @return a Future which completes when the after restart callback for the given pod has completed.
     */
    private Future<Void> restartWithPostBarrier(Pod pod) {
        String podName = pod.getMetadata().getName();
        log.debug("Rolling pod {}", podName);
        return restart(pod).compose(i -> {
            String ssName = podName.substring(0, podName.lastIndexOf('-'));
            log.debug("Rolling update of {}/{}: wait for pod {} postcondition", namespace, ssName, podName);
            return postRestartBarrier(pod);
        });
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
     * Represents the restarting of a particular pod at a given time called its deadline.
     * {@code RestartAttempts} are stored in the {@link #queue} ordered by next deadline.
     * If a pod cannot be restarted it's can be given a new deadline ({@link #backoffAndRetry()}).
     */
    protected static class RestartAttempt implements Comparable<RestartAttempt> {
        protected final int podId;
        private long nextDeadline;
        private final BackOff backOff;
        public RestartAttempt(int podId, BackOff backOff) {
            this.podId = podId;
            this.backOff = backOff;
            this.nextDeadline = 0;
        }

        /**
         * Give this restart attempt a new deadline, or throw.
         * @return This instance.
         * @throws MaxAttemptsExceededException if the next attempt would exceed the configured number of attempts.
         */
        private RestartAttempt backoffAndRetry() {
            long delayMs = backOff.delayMs();
            log.debug("Will retry pod {} in {}ms", podId, delayMs);
            nextDeadline = System.currentTimeMillis() + delayMs;
            return this;
        }

        @Override
        public int compareTo(RestartAttempt other) {
            int cmp = Long.compare(this.nextDeadline, other.nextDeadline);
            if (cmp == 0) {
                cmp = Integer.compare(this.podId, other.podId);
            }
            return cmp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RestartAttempt restartAttempt = (RestartAttempt) o;
            return podId == restartAttempt.podId &&
                    nextDeadline == restartAttempt.nextDeadline;
        }

        @Override
        public int hashCode() {
            return Objects.hash(podId, nextDeadline);
        }

        @Override
        public String toString() {
            return "RestartAttempt{" +
                    "podId=" + podId +
                    ", nextDeadline=" + nextDeadline +
                    '}';
        }
    }

    protected Future<Integer> sort(Predicate<Pod> podNeedsRestart) {
        return filterPods(podNeedsRestart)
            .compose(pod -> {
                if (pod != null) {
                    return findNextRollable(podNeedsRestart);
                } else {
                    return Future.succeededFuture();
                }
            });
    }

    /**
     * Returns a Future which completes with an AdminClient instance.
     */
    protected Future<AdminClient> adminClient(Integer podId) {
        String hostname = KafkaCluster.podDnsName(this.namespace, this.cluster, podName(podId)) + ":" + KafkaCluster.REPLICATION_PORT;
        Future<AdminClient> result = Future.future();
        vertx.executeBlocking(
            f -> {
                try {
                    log.debug("Creating AC for {}", hostname);
                    f.complete(adminClientProvider.createAdminClient(hostname, this.clusterCaCertSecret, this.coKeySecret));
                } catch (Exception e) {
                    f.fail(e);
                }
            },
            result);
        return result;
    }

    /**
     * 1. Wait for the head of the queue is scheduled to be restarted.
     * 2. Obtain an admin client
     * 3. Determine {@linkplain #checkRollability(Predicate, RestartAttempt, AdminClient) whether the pod can be rolled right now}.
     * If the head pod is rollable complete the returned future with its id.
     * If the head pod is not rollable
     * @param podNeedsRestart Predicate for deciding whether the pod needs to be restarted.
     * @return
     */
    protected Future<Integer> findNextRollable(Predicate<Pod> podNeedsRestart) {
        Future<Integer> result = Future.future();
        pollAwait().setHandler(pollResult -> {
            if (pollResult.succeeded()) {
                RestartAttempt restartAttempt = pollResult.result();
                adminClient(restartAttempt.podId).setHandler(acResult -> {
                    if (acResult.succeeded()) {
                        AdminClient adminClient = acResult.result();
                        checkRollability(podNeedsRestart, restartAttempt, adminClient).recover(error -> {
                            if (error instanceof AbortRollException) {
                                log.warn("Aborting roll: {}", error.toString());
                                return Future.failedFuture(error);
                            } else {
                                log.warn("Non-abortive error when determining next pod to roll " +
                                        "(next pod to be rolled might not be ideal)", error);
                                return Future.succeededFuture(restartAttempt.podId);
                            }
                        }).setHandler(xx -> close(adminClient, xx, result));
                    } else {
                        // error opening admin client
                        log.warn("Error opening AdminClient, using first pod", acResult.cause());
                        result.complete(restartAttempt.podId);
                    }
                });
            } else {
                // error in poll
                result.fail(pollResult.cause());
            }
        });
        return result;
    }

    /**
     * Return a Future which completes with the pod id of the next<emph>rollable</emph> pod.
     * A pod is rollable if:
     * <ul>
     *     <li>it is not the controller, or if it is the last pod to be rolled, and</li>
     *     <li>rolling it would not affect client {@link #availability}.</li>
     * </ul>
     * If the the pod is not rollable it is requeued for reconsideration later.
     * @param podNeedsRestart
     * @param restartAttempt
     * @param adminClient
     * @return
     */
    // TODO why does this take a restartAttempt if it can return a different pod?
    private Future<Integer> checkRollability(Predicate<Pod> podNeedsRestart, RestartAttempt restartAttempt, AdminClient adminClient) {
        return controller(adminClient)
            .compose(controller -> {
                Integer podId = restartAttempt.podId;
                if (podId.equals(controller) && !isEmpty()) {
                    // Arrange to do the controller last when there are other brokers to be rolled
                    log.debug("Pod {} is the controller: Will roll other pods first", podId);
                    return requeueOrAbort(podNeedsRestart, restartAttempt);
                } else {
                    return availability(adminClient).canRoll(podId).compose(canRoll -> {
                        if (canRoll) {
                            // The first pod in the list needs rolling and is rollable: We're done
                            log.debug("Can roll pod {}", podId);
                            return Future.succeededFuture(podId);
                        } else {
                            log.debug("Cannot roll pod {} right now (would affect availability): Will roll other pods first", podId);
                            return requeueOrAbort(podNeedsRestart, restartAttempt);
                        }
                    });
                }
            });
    }

    /**
     * Asynchronously close the given AdminClient instance ignoring errors, then complete the
     * given {@code result} future with the result of {@code xx}.
     */
    private void close(AdminClient adminClient, AsyncResult<Integer> xx, Future<Integer> result) {
        vertx.executeBlocking(
            f -> {
                try {
                    log.debug("Closing AC");
                    adminClient.close(Duration.ofSeconds(10));
                    log.debug("Closed AC");
                    f.complete();
                } catch (Throwable t) {
                    log.warn("Ignoring error from closing admin client", t);
                    f.complete();
                }
            },
            fut -> {
                if (xx.failed()) {
                    if (fut.failed()) {
                        xx.cause().addSuppressed(fut.cause());
                    }
                    result.fail(xx.cause());
                } else if (fut.failed()) {
                    result.fail(fut.cause());
                } else {
                    result.handle(xx);
                }
            });
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
    Future<Integer> controller(AdminClient ac) {
        Future<Integer> result = Future.future();
        try {
            ac.describeCluster().controller().whenComplete((controllerNode, exception) -> {
                vertx.runOnContext(ignored -> {
                    if (exception != null) {
                        result.fail(exception);
                    } else {
                        int id = Node.noNode().equals(controllerNode) ? -1 : controllerNode.id();
                        log.debug("controller is {}", id);
                        result.complete(id);
                    }
                });
            });
        } catch (Throwable t) {
            result.fail(t);
        }
        return result;
    }

    protected void initPods(List<Integer> pods, Supplier<BackOff> backOffSupplier) {
        for (Integer po : pods) {
            this.queue.add(new RestartAttempt(po, backOffSupplier.get()));
        }
    }

    /**
     * Re-queue for retry, completing the returned future after a delay, or failing
     * it if it's already been retried too many times.
     * @param podNeedsRestart Predicate for deciding whether the pod needs to be restarted.
     * @param restartAttempt The restart attempt.
     * @return A Future completed with the next pod to roll, or failing with
     * {@code AbortRollException} if the given restart attempt has already been retired too many times.
     */
    protected Future<Integer> requeueOrAbort(Predicate<Pod> podNeedsRestart, RestartAttempt restartAttempt) {
        try {
            log.debug("Deferring restart of pod {}", restartAttempt.podId);
            queue.add(restartAttempt.backoffAndRetry());
            return filterAndFindNextRollable(podNeedsRestart);
        } catch (MaxAttemptsExceededException e) {
            return Future.failedFuture(new AbortRollException("Pod " + restartAttempt.podId + " is still not rollable after " + restartAttempt.backOff.maxAttempts() + " times of asking: Aborting"));
        }
    }

    /**
     * If there is no next pod then return a completed Future with null result.
     * Otherwise asynchronously get the next pod, test it with the given {@code podNeedsRestart}
     * and if that pod needs a restart then complete the returned future with it,
     * leaving it's RestartAttempt at the head of the queue.
     * If that pod didn't need a restart then remove the pod from the list of unrolled pods and recurse.
     *
     * When the returned Future completes successfully either:
     * <ul>
     *     <li>The queue is empty</li>
     *     <li>The head of the queue is a RestartAttempt whose pod needs to be rolled.</li>
     * </ul>
     *
     * If there is an error getting the pod or applying the {@code podNeedsRestart} then return a failed future.
     */
    protected final Future<Integer> filterPods(Predicate<Pod> podNeedsRestart) {
        RestartAttempt restartAttempt = queue.peek();
        if (restartAttempt == null) {
            return Future.succeededFuture(null);
        } else {
            log.debug("Checking whether pod {} needs to be restarted", restartAttempt.podId);
            return podOperations.getAsync(this.namespace, podName(restartAttempt.podId)).compose(pod -> {
                if (podNeedsRestart.test(pod)) {
                    log.debug("Pod {} needs to be restarted", restartAttempt.podId);
                    return Future.succeededFuture(restartAttempt.podId);
                } else {
                    // remove from pods and try next pod
                    log.debug("Pod {} does not need to be restarted", restartAttempt.podId);
                    this.queue.remove();
                    return filterPods(podNeedsRestart);
                }
            }).recover(error -> {
                log.debug("Error filtering pods", error);
                return Future.failedFuture(error);
            });
        }
    }

    private final Future<Integer> filterAndFindNextRollable(Predicate<Pod> podNeedsRestart) {
        return filterPods(podNeedsRestart)
                .compose(pod -> {
                    if (pod != null) {
                        return findNextRollable(podNeedsRestart);
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    protected Future<RestartAttempt> pollAwait() {
        RestartAttempt restartAttempt = this.queue.poll();
        long delay = restartAttempt.nextDeadline - System.currentTimeMillis();
        if (delay <= 0L) {
            log.debug("Proceeding with pod {}", restartAttempt.podId);
            return Future.succeededFuture(restartAttempt);
        } else {
            Future<RestartAttempt> f = Future.future();
            log.debug("Waiting {}ms before proceeding with pod {}", delay, restartAttempt.podId);
            vertx.setTimer(delay, timerId -> {
                log.debug("Proceeding with pod {}", restartAttempt.podId);
                f.complete(restartAttempt);
            });
            return f;
        }
    }

    protected boolean isEmpty() {
        return this.queue.isEmpty();
    }

    /**
     * Returns a future that completes with the next pod to roll, or null if there are no more pods to be rolled.
     */
    final Future<Integer> next(Predicate<Pod> podNeedsRestart) {
        return sort(podNeedsRestart);
    }

    @Override
    public String toString() {
        return queue.toString();
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
