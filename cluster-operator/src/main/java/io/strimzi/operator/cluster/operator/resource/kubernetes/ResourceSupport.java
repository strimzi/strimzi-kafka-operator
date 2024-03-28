/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.Deletable;
import io.fabric8.kubernetes.client.dsl.Gettable;
import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.Watchable;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.TimeoutException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Utility method for working with Kubernetes resources
 */
public class ResourceSupport {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ResourceSupport.class);

    private final Vertx vertx;

    /**
     * Constructor
     *
     * @param vertx     Vertx instance
     */
    ResourceSupport(Vertx vertx) {
        this.vertx = vertx;
    }

    /**
     * Asynchronously close the given {@code closeable} on a worker thread,
     * returning a Future which completes with the outcome.
     *
     * @param closeable The closeable
     * @return The Future
     */
    public Future<Void> closeOnWorkerThread(Closeable closeable) {
        return executeBlocking(() -> {
            LOGGER.debugOp("Closing {}", closeable);
            closeable.close();
            return null;
        });
    }

    <T> Future<T> executeBlocking(Callable<T> callable) {
        return vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(callable);
    }

    /**
     * Combines two completed AsyncResults, at least one of which has failed, returning
     * a single cause, possibly with suppressed exception.
     * If both AsyncResults have failed {@code primary} will be the main cause of failure and
     * {@code secondary} will be a suppressed exception.
     *
     * @param primary The primary failure.
     * @param secondary The secondary failure.
     * @return The cause.
     */
    private Throwable collectCauses(AsyncResult<?> primary,
                            AsyncResult<?> secondary) {
        Throwable cause = primary.cause();
        if (cause == null) {
            cause = secondary.cause();
        } else {
            if (secondary.failed()) {
                cause.addSuppressed(secondary.cause());
            }
        }
        return cause;
    }

    /**
     * Watches the given {@code watchable} using the given
     * {@code watchFn},
     * returning a Future which completes when {@code watchFn} returns non-null
     * to some event on the watchable, or after a timeout.
     *
     * The given {@code watchFn} will be invoked on a worker thread when the
     * Kubernetes resources changes, so may block.
     * When the {@code watchFn} returns non-null the watch will be closed and then
     * the future returned from this method will be completed on the context thread.
     *
     * In some cases such as resource deletion, it might happen that the resource is deleted already before the watch is
     * started and as a result the watch never completes. The {@code preCheckFn} will be invoked on a worker thread
     * after the watch has been created. It is expected to double-check if we still need to wait for the watch to fire.
     * When the {@code preCheckFn} returns non-null the watch will be closed and the future returned from this method
     * will be completed with the result of the {@code preCheckFn} on the context thread. In the deletion example
     * described above, the {@code preCheckFn} can check if the resource still exists and close the watch in case it was
     * already deleted.
     *
     * @param reconciliation Reconciliation marker used for logging
     * @param watchable The watchable - used to watch the resource.
     * @param gettable The Gettable - used to get the resource in the pre-check.
     * @param operationTimeoutMs The timeout in ms.
     * @param watchFnDescription A description of what {@code watchFn} is watching for.
     *                           E.g. "observe ${condition} of ${kind} ${namespace}/${name}".
     * @param watchFn The function to determine if the event occurred
     * @param preCheckFn Pre-check function to avoid situation when the watch is never fired because ot was started too late.
     * @param <T> The type of watched resource.
     * @param <U> The result type of the {@code watchFn}.
     *
     * @return A Futures which completes when the {@code watchFn} returns non-null
     * in response to some Kubernetes even on the watched resource(s).
     */
    <T, U> Future<U> selfClosingWatch(Reconciliation reconciliation,
                                      Watchable<T> watchable,
                                      Gettable<T> gettable,
                                      long operationTimeoutMs,
                                      String watchFnDescription,
                                      BiFunction<Watcher.Action, T, U> watchFn,
                                      Function<T, U> preCheckFn) {

        return new Watcher<T>() {
            private final Promise<Watch> watchPromise;
            private final Promise<U> donePromise;
            private final Promise<U> resultPromise;
            private final long timerId;

            /* init */
            {
                this.watchPromise = Promise.promise();
                this.donePromise = Promise.promise();
                this.resultPromise = Promise.promise();
                this.timerId = vertx.setTimer(operationTimeoutMs,
                    ignored -> donePromise.tryFail(new TimeoutException("\"" + watchFnDescription + "\" timed out after " + operationTimeoutMs + "ms")));
                Future.join(watchPromise.future(), donePromise.future()).onComplete(joinResult -> {
                    Future<Void> closeFuture;
                    if (watchPromise.future().succeeded()) {
                        closeFuture = closeOnWorkerThread(watchPromise.future().result());
                    } else {
                        closeFuture = Future.succeededFuture();
                    }

                    closeFuture.onComplete(closeResult ->
                        vertx.runOnContext(ignored2 -> {
                            LOGGER.debugCr(reconciliation, "Completing watch future");
                            if (joinResult.succeeded() && closeResult.succeeded()) {
                                resultPromise.complete(joinResult.result().resultAt(1));
                            } else {
                                resultPromise.fail(collectCauses(joinResult, closeResult));
                            }
                        }
                    ));
                });

                try {
                    Watch watch = watchable.watch(this);
                    LOGGER.debugCr(reconciliation, "Opened watch {} for evaluation of {}", watch, watchFnDescription);

                    // Pre-check is done after the watch is open to make sure we did not miss the event. In the worst
                    // case, both pre-check and watch complete the future. But at least one should always complete it.
                    U apply = preCheckFn.apply(gettable.get());
                    if (apply != null) {
                        LOGGER.debugCr(reconciliation, "Pre-check is already complete, no need to wait for the watch: {}", watchFnDescription);
                        donePromise.tryComplete(apply);
                        vertx.cancelTimer(timerId);
                    } else {
                        LOGGER.debugCr(reconciliation, "Pre-check is not complete yet, let's wait for the watch: {}", watchFnDescription);
                    }

                    watchPromise.complete(watch);
                } catch (Throwable t) {
                    watchPromise.fail(t);
                }
            }

            @Override
            public void eventReceived(Action action, T resource) {
                vertx.executeBlocking(
                        () -> {
                            try {
                                U apply = watchFn.apply(action, resource);
                                if (apply != null) {
                                    LOGGER.debugCr(reconciliation, "Satisfied: {}", watchFnDescription);
                                    donePromise.tryComplete(apply);
                                    vertx.cancelTimer(timerId);
                                } else {
                                    LOGGER.debugCr(reconciliation, "Not yet satisfied: {}", watchFnDescription);
                                }

                                return apply;
                            } catch (Throwable t) {
                                LOGGER.debugCr(reconciliation, "Ignoring exception thrown while evaluating watch {} because the future was already completed", watchFnDescription, t);
                                throw t;
                            }
                        });
            }

            @Override
            public void onClose(WatcherException cause) {

            }

        }.resultPromise.future();
    }

    /**
     * Asynchronously deletes the given resource(s), returning a Future which completes on the context thread.
     * <strong>Note: The API server can return asynchronously, meaning the resource is still accessible from the API server
     * after the returned Future completes. Use {@link #selfClosingWatch(Reconciliation, Watchable, Gettable, long, String, BiFunction, Function)}
     * to provide server-synchronous semantics.</strong>
     *
     * @param resource The resource(s) to delete.
     * @return A Future which completes on the context thread.
     */
    Future<Void> deleteAsync(Deletable resource) {
        return executeBlocking(() -> {
            // Returns TRUE when resource was deleted and FALSE when it was not found (see BaseOperation Fabric8 class)
            // In both cases we return success since the end-result has been achieved
            // Throws an exception for other errors
            resource.delete();
            return null;
        });
    }

    /**
     * Asynchronously gets the given resource, returning a Future which completes on the context thread.
     *
     * @param resource The resource(s) to get.
     * @return A Future which completes on the context thread.
     */
    <T> Future<T> getAsync(Gettable<T> resource) {
        return executeBlocking(resource::get);
    }

    /**
     * Asynchronously lists the matching resources, returning a Future which completes on the context thread.
     *
     * @param resource The resources to list.
     * @return A Future which completes on the context thread.
     */
    <T extends HasMetadata, L extends KubernetesResourceList<T>> Future<List<T>> listAsync(Listable<L> resource) {
        return executeBlocking(() -> resource.list(new ListOptionsBuilder().withResourceVersion("0").build()).getItems());
    }
}
