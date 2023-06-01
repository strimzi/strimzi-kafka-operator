/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

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
import io.strimzi.operator.common.StrimziFuture;
import io.strimzi.operator.common.Util;

/**
 * Utility method for working with Kubernetes resources
 */
public class ResourceSupport {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ResourceSupport.class);

    /**
     * Asynchronously close the given {@code closeable} on a worker thread,
     * returning a Future which completes with the outcome.
     *
     * @param closeable The closeable
     * @return The Future
     */
    public StrimziFuture<Void> closeOnWorkerThread(Closeable closeable) {
        return executeBlocking(blockingFuture -> {
            try {
                LOGGER.debugOp("Closing {}", closeable);
                closeable.close();
                blockingFuture.complete(null);
            } catch (Throwable t) {
                blockingFuture.completeExceptionally(t);
            }
        });
    }

    <T> StrimziFuture<T> executeBlocking(Consumer<CompletableFuture<T>> blockingCodeHandler) {
        StrimziFuture<T> result = new StrimziFuture<>();
        StrimziFuture.completedFuture(result).thenAcceptAsync(blockingCodeHandler);
        return result;
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
    private Throwable collectCauses(Throwable primary, Throwable secondary) {
        Throwable cause = primary;
        if (cause == null) {
            cause = secondary;
        } else {
            if (secondary != null) {
                cause.addSuppressed(secondary);
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
     * after the watch has been created. It is expected to double check if we still need to wait for the watch to fire.
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
     * @param watchFn The function to determine if the event occured
     * @param preCheckFn Pre-check function to avoid situation when the watch is never fired because ot was started too late.
     * @param <T> The type of watched resource.
     * @param <U> The result type of the {@code watchFn}.
     *
     * @return A Futures which completes when the {@code watchFn} returns non-null
     * in response to some Kubenetes even on the watched resource(s).
     */
    <T, U> StrimziFuture<U> selfClosingWatch(Reconciliation reconciliation,
                                      Watchable<T> watchable,
                                      Gettable<T> gettable,
                                      long operationTimeoutMs,
                                      String watchFnDescription,
                                      BiFunction<Watcher.Action, T, U> watchFn,
                                      Function<T, U> preCheckFn) {

        return new Watcher<T>() {
            private final CompletableFuture<Watch> watchPromise;
            private final CompletableFuture<U> donePromise;
            private final StrimziFuture<U> resultPromise;

            /* init */
            {
                this.watchPromise = new StrimziFuture<>();
                this.donePromise = new StrimziFuture<U>().orTimeout(operationTimeoutMs, TimeUnit.MILLISECONDS);
                this.resultPromise = new StrimziFuture<>();

                StrimziFuture.allOf(watchPromise, donePromise).whenComplete((nothing, thrown) -> {
                    CompletionStage<Void> closeFuture;

                    if (succeeded(watchPromise)) {
                        closeFuture = closeOnWorkerThread(watchPromise.join());
                    } else {
                        closeFuture = StrimziFuture.completedFuture();
                    }

                    closeFuture.whenComplete((closeResult, closeThrown) -> {
                        LOGGER.debugCr(reconciliation, "Completing watch future");
                        if (thrown == null && closeThrown == null) {
                            resultPromise.complete(donePromise.join());
                        } else {
                            Throwable primary;

                            if (Util.unwrap(thrown) instanceof java.util.concurrent.TimeoutException) {
                                primary = new TimeoutException("\"" + watchFnDescription + "\" timed out after " + operationTimeoutMs + "ms");
                            } else {
                                primary = thrown;
                            }

                            resultPromise.completeExceptionally(collectCauses(primary, closeThrown));
                        }
                    });
                });

                try {
                    Watch watch = watchable.watch(this);
                    LOGGER.debugCr(reconciliation, "Opened watch {} for evaluation of {}", watch, watchFnDescription);

                    // Pre-check is done after the watch is open to make sure we did not missed the event. In the worst
                    // case, both pre-check and watch complete the future. But at least one should always complete it.
                    U apply = preCheckFn.apply(gettable.get());
                    if (apply != null) {
                        LOGGER.debugCr(reconciliation, "Pre-check is already complete, no need to wait for the watch: {}", watchFnDescription);
                        donePromise.complete(apply);
                    } else {
                        LOGGER.debugCr(reconciliation, "Pre-check is not complete yet, let's wait for the watch: {}", watchFnDescription);
                    }

                    watchPromise.complete(watch);
                } catch (Throwable t) {
                    watchPromise.completeExceptionally(t);
                }
            }

            @Override
            public void eventReceived(Action action, T resource) {
                StrimziFuture.runAsync(() -> {
                    try {
                        U apply = watchFn.apply(action, resource);
                        if (apply != null) {
                            LOGGER.debugCr(reconciliation, "Satisfied: {}", watchFnDescription);
                            donePromise.complete(apply);
                        } else {
                            LOGGER.debugCr(reconciliation, "Not yet satisfied: {}", watchFnDescription);
                        }
                    } catch (Throwable t) {
                        if (!donePromise.completeExceptionally(t)) {
                            LOGGER.debugCr(reconciliation, "Ignoring exception thrown while " +
                                    "evaluating watch {} because the future was already completed", watchFnDescription, t);
                        }
                    }
                });
            }

            @Override
            public void onClose(WatcherException cause) {

            }

        }.resultPromise;
    }

    static boolean succeeded(CompletableFuture<?> future) {
        return future.isDone() && !future.isCompletedExceptionally();
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
    StrimziFuture<Void> deleteAsync(Deletable resource) {
        return executeBlocking(
            blockingFuture -> {
                try {
                    // Returns TRUE when resource was deleted and FALSE when it was not found (see BaseOperation Fabric8 class)
                    // In both cases we return success since the end-result has been achieved
                    // Throws an exception for other errors
                    resource.delete();
                    blockingFuture.complete(null);
                } catch (Throwable t) {
                    blockingFuture.completeExceptionally(t);
                }
            });
    }

    /**
     * Asynchronously gets the given resource, returning a Future which completes on the context thread.
     *
     * @param resource The resource(s) to get.
     * @return A Future which completes on the context thread.
     */
    <T> StrimziFuture<T> getAsync(Gettable<T> resource) {
        return executeBlocking(
            blockingFuture -> {
                try {
                    blockingFuture.complete(resource.get());
                } catch (Throwable t) {
                    blockingFuture.completeExceptionally(t);
                }
            });
    }

    /**
     * Asynchronously lists the matching resources, returning a Future which completes on the context thread.
     *
     * @param resource The resources to list.
     * @return A Future which completes on the context thread.
     */
    <T extends HasMetadata, L extends KubernetesResourceList<T>> StrimziFuture<List<T>> listAsync(Listable<L> resource) {
        return executeBlocking(
            blockingFuture -> {
                try {
                    blockingFuture.complete(resource.list(new ListOptionsBuilder().withResourceVersion("0").build()).getItems());
                } catch (Throwable t) {
                    blockingFuture.completeExceptionally(t);
                }
            });
    }
}
