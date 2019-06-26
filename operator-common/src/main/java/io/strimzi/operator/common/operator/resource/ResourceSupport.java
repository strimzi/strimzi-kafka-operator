/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Watchable;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.function.BiFunction;

public class ResourceSupport {

    protected static final Logger LOGGER = LogManager.getLogger(ResourceSupport.class);

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
    public Future closeOnWorkerThread(Closeable closeable) {
        Future result = Future.future();
        vertx.executeBlocking(
            blockingFuture -> {
                try {
                    LOGGER.debug("Closing {}", closeable);
                    closeable.close();
                    blockingFuture.complete();
                } catch (Throwable t) {
                    blockingFuture.fail(t);
                }
            },
            true,
            result);
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
    Throwable collectCauses(AsyncResult<? extends Object> primary,
                            AsyncResult<? extends Object> secondary) {
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
     * @param watchFn The function to determine
     * @param timeoutMs The timeout in milliseconds.
     * @param <T> The type of watched resource.
     * @param <U> The result type of the {@code watchFn}.
     *
     * @return A Futures which completes when the {@code watchFn} returns non-null
     * in response to some Kubenetes even on the watched resource(s).
     */
    public <T, U> Future<U> selfClosingWatch(Watchable<Watch, Watcher<T>> watchable,
                                             BiFunction<Watcher.Action, T, U> watchFn,
                                             long timeoutMs) {

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
                this.timerId = vertx.setTimer(timeoutMs, ignored -> {
                    doneFuture.tryFail(new TimeoutException());
                });
                CompositeFuture.join(watchFuture, doneFuture).setHandler(joinResult -> {
                    Future<Void> closeFuture;
                    if (watchFuture.succeeded()) {
                        closeFuture = closeOnWorkerThread(watchFuture.result());
                    } else {
                        closeFuture = Future.succeededFuture();
                    }
                    closeFuture.setHandler(closeResult -> {
                        vertx.runOnContext(ignored2 -> {
                            LOGGER.warn("Completing watch future");
                            if (joinResult.succeeded() && closeResult.succeeded()) {
                                resultFuture.complete(joinResult.result().resultAt(1));
                            } else {
                                resultFuture.fail(collectCauses(joinResult, closeResult));
                            }
                        });
                    });
                });
                Watch watch = watchable.watch(this);
                LOGGER.debug("Opened watch {}", watch);
                watchFuture.complete(watch);
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
                                LOGGER.debug("Not yet complete");
                            }
                        } catch (Throwable t) {
                            if (!f.tryFail(t)) {
                                LOGGER.debug("Ignoring exception thrown while " +
                                        "evaluating watch because the future was already completed", t);
                            }
                        }
                    },
                    true,
                    ar -> {
                        doneFuture.handle(ar);
                    });
            }

            @Override
            public void onClose(KubernetesClientException cause) {

            }

        }.resultFuture;
    }
}
