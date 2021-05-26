/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.Deletable;
import io.fabric8.kubernetes.client.dsl.Gettable;
import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.Watchable;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.io.Closeable;
import java.util.List;
import java.util.function.BiFunction;

public class ResourceSupport {
    public static final long DEFAULT_TIMEOUT_MS = 300_000;
    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ResourceSupport.class);

    private final Vertx vertx;

    ResourceSupport(Vertx vertx) {
        this.vertx = vertx;
    }

    /**
     * Asynchronously close the given {@code closeable} on a worker thread,
     * returning a Future which completes with the outcome.
     * @param closeable The closeable
     * @return The Future
     */
    public Future<Void> closeOnWorkerThread(Closeable closeable) {
        return executeBlocking(
            blockingFuture -> {
                try {
                    LOGGER.debugOp("Closing {}", closeable);
                    closeable.close();
                    blockingFuture.complete();
                } catch (Throwable t) {
                    blockingFuture.fail(t);
                }
            });
    }

    <T> Future<T> executeBlocking(Handler<Promise<T>> blockingCodeHandler) {
        Promise<T> result = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool")
                .executeBlocking(blockingCodeHandler, true, result);
        return result.future();
    }

    /**
     * Combines two completed AsyncResults, at least one of which has failed, returning
     * a single cause, possibly with suppressed exception.
     * If both AsyncResults have failed {@code primary} will be the main cause of failure and
     * {@code secondary} will be a suppressed exception.
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
     * The given {@code watchFn} will be invoked on a worked thread when the
     * Kubernetes resources changes, so may block.
     * When the {@code watchFn} returns non-null the watch will be closed and then
     * the future returned from this method will be completed on the context thread.
     * @param watchable The watchable.
     * @param operationTimeoutMs The timeout in ms.
     * @param watchFnDescription A description of what {@code watchFn} is watching for.
     *                           E.g. "observe ${condition} of ${kind} ${namespace}/${name}".
     * @param watchFn The function to determine
     * @param <T> The type of watched resource.
     * @param <U> The result type of the {@code watchFn}.
     *
     * @return A Futures which completes when the {@code watchFn} returns non-null
     * in response to some Kubenetes even on the watched resource(s).
     */
    <T, U> Future<U> selfClosingWatch(Watchable<Watcher<T>> watchable,
                                      long operationTimeoutMs,
                                      String watchFnDescription,
                                      BiFunction<Watcher.Action, T, U> watchFn) {

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
                CompositeFuture.join(watchPromise.future(), donePromise.future()).onComplete(joinResult -> {
                    Future<Void> closeFuture;
                    if (watchPromise.future().succeeded()) {
                        closeFuture = closeOnWorkerThread(watchPromise.future().result());
                    } else {
                        closeFuture = Future.succeededFuture();
                    }

                    closeFuture.onComplete(closeResult ->
                        vertx.runOnContext(ignored2 -> {
                            LOGGER.debugOp("Completing watch future");
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
                    LOGGER.debugOp("Opened watch {} for evaluation of {}", watch, watchFnDescription);
                    watchPromise.complete(watch);
                } catch (Throwable t) {
                    watchPromise.fail(t);
                }
            }

            @Override
            public void eventReceived(Action action, T resource) {
                vertx.executeBlocking(
                    f -> {
                        try {
                            U apply = watchFn.apply(action, resource);
                            if (apply != null) {
                                f.tryComplete(apply);
                                vertx.cancelTimer(timerId);
                            } else {
                                LOGGER.debugOp("Not yet satisfied: {}", watchFnDescription);
                            }
                        } catch (Throwable t) {
                            if (!f.tryFail(t)) {
                                LOGGER.debugOp("Ignoring exception thrown while " +
                                        "evaluating watch {} because the future was already completed", watchFnDescription, t);
                            }
                        }
                    },
                    true,
                        donePromise);
            }

            @Override
            public void onClose(WatcherException cause) {

            }

        }.resultPromise.future();
    }

    /**
     * Asynchronously deletes the given resource(s), returning a Future which completes on the context thread.
     * <strong>Note: The API server can return asynchronously, meaning the resource is still accessible from the API server
     * after the returned Future completes. Use {@link #selfClosingWatch(Watchable, long, String, BiFunction)}
     * to provide server-synchronous semantics.</strong>
     *
     * @param resource The resource(s) to delete.
     * @return A Future which completes on the context thread.
     */
    Future<Void> deleteAsync(Deletable resource) {
        return executeBlocking(
            blockingFuture -> {
                try {
                    Boolean delete = resource.delete();
                    if (!Boolean.TRUE.equals(delete)) {
                        blockingFuture.fail(new RuntimeException(resource + " could not be deleted (returned " + delete + ")"));
                    } else {
                        blockingFuture.complete();
                    }
                } catch (Throwable t) {
                    blockingFuture.fail(t);
                }
            });
    }

    /**
     * Asynchronously gets the given resource, returning a Future which completes on the context thread.
     *
     * @param resource The resource(s) to get.
     * @return A Future which completes on the context thread.
     */
    <T> Future<T> getAsync(Gettable<T> resource) {
        return executeBlocking(
            blockingFuture -> {
                try {
                    blockingFuture.complete(resource.get());
                } catch (Throwable t) {
                    blockingFuture.fail(t);
                }
            });
    }

    /**
     * Asynchronously lists the matching resources, returning a Future which completes on the context thread.
     *
     * @param resource The resources to list.
     * @return A Future which completes on the context thread.
     */
    <T extends HasMetadata, L extends KubernetesResourceList<T>> Future<List<T>> listAsync(Listable<L> resource) {
        return executeBlocking(
            blockingFuture -> {
                try {
                    blockingFuture.complete(resource.list().getItems());
                } catch (Throwable t) {
                    blockingFuture.fail(t);
                }
            });
    }
}
