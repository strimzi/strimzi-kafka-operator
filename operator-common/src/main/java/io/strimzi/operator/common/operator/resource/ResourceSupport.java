/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Deletable;
import io.fabric8.kubernetes.client.dsl.Gettable;
import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.Watchable;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.List;
import java.util.function.BiFunction;

public class ResourceSupport {

    protected static final Logger LOGGER = LogManager.getLogger(ResourceSupport.class);

    private final Vertx vertx;
    private final long operationTimeoutMs;

    ResourceSupport(Vertx vertx, long operationTimeoutMs) {
        this.vertx = vertx;
        this.operationTimeoutMs = operationTimeoutMs;
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
                    LOGGER.debug("Closing {}", closeable);
                    closeable.close();
                    blockingFuture.complete();
                } catch (Throwable t) {
                    blockingFuture.fail(t);
                }
            });
    }

    private <T> Future<T> executeBlocking(Handler<Future<T>> blockingCodeHandler) {
        Future<T> result = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-tool")
                .executeBlocking(blockingCodeHandler, true, result);
        return result;
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
     * @param watchFnDescription A description of what {@code watchFn} is watching for.
     *                           E.g. "observe ${condition} of ${kind} ${namespace}/${name}".
     * @param watchFn The function to determine
     * @param <T> The type of watched resource.
     * @param <U> The result type of the {@code watchFn}.
     *
     * @return A Futures which completes when the {@code watchFn} returns non-null
     * in response to some Kubenetes even on the watched resource(s).
     */
    <T, U> Future<U> selfClosingWatch(Watchable<Watch, Watcher<T>> watchable,
                                      String watchFnDescription,
                                      BiFunction<Watcher.Action, T, U> watchFn) {

        return new Watcher<T>() {
            private final Future<Watch> watchFuture;
            private final Future<U> doneFuture;
            private final Future<U> resultFuture;
            private final long timerId;

            /* init */
            {
                this.watchFuture = Future.future();
                this.doneFuture = Future.future();
                this.resultFuture = Future.future();
                this.timerId = vertx.setTimer(operationTimeoutMs,
                    ignored -> doneFuture.tryFail(new TimeoutException("\"" + watchFnDescription + "\" timed out after " + operationTimeoutMs + "ms")));
                CompositeFuture.join(watchFuture, doneFuture).setHandler(joinResult -> {
                    Future<Void> closeFuture;
                    if (watchFuture.succeeded()) {
                        closeFuture = closeOnWorkerThread(watchFuture.result());
                    } else {
                        closeFuture = Future.succeededFuture();
                    }
                    closeFuture.setHandler(closeResult ->
                        vertx.runOnContext(ignored2 -> {
                            LOGGER.debug("Completing watch future");
                            if (joinResult.succeeded() && closeResult.succeeded()) {
                                resultFuture.complete(joinResult.result().resultAt(1));
                            } else {
                                resultFuture.fail(collectCauses(joinResult, closeResult));
                            }
                        }
                    ));
                });
                try {
                    Watch watch = watchable.watch(this);
                    LOGGER.debug("Opened watch {} for evaluation of {}", watch, watchFnDescription);
                    watchFuture.complete(watch);
                } catch (Throwable t) {
                    watchFuture.fail(t);
                }
            }

            @Override
            public void eventReceived(Action action, T resource) {
                vertx.<U>executeBlocking(
                    f -> {
                        try {
                            U apply = watchFn.apply(action, resource);
                            if (apply != null) {
                                f.tryComplete(apply);
                                vertx.cancelTimer(timerId);
                            } else {
                                LOGGER.debug("Not yet satisfied: {}", watchFnDescription);
                            }
                        } catch (Throwable t) {
                            if (!f.tryFail(t)) {
                                LOGGER.debug("Ignoring exception thrown while " +
                                        "evaluating watch {} because the future was already completed", watchFnDescription, t);
                            }
                        }
                    },
                    true,
                    ar -> {
                        if (ar.succeeded()) {
                            doneFuture.tryComplete(ar.result());
                        } else {
                            doneFuture.tryFail(ar.cause());
                        }
                    });
            }

            @Override
            public void onClose(KubernetesClientException cause) {

            }

        }.resultFuture;
    }

    /**
     * Asynchronously deletes the given resource(s), returning a Future which completes on the context thread.
     * <strong>Note: The API server can return asynchronously, meaning the resource is still accessible from the API server
     * after the returned Future completes. Use {@link #selfClosingWatch(Watchable, String, BiFunction)}
     * to provide server-synchronous semantics.</strong>
     *
     * @param resource The resource(s) to delete.
     * @return A Future which completes on the context thread.
     */
    Future<Void> deleteAsync(Deletable<Boolean> resource) {
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
