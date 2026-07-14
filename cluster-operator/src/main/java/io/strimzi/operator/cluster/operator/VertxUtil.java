/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.StrimziTimeoutException;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Class with various utility methods that use or depend on Vert.x core.
 */
public final class VertxUtil {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(VertxUtil.class);

    private VertxUtil() {
        // Not used
    }

    /**
     * Adapts a Vert.x {@link WorkerExecutor} to a plain {@link Executor}. This is used to let the non-Vert.x resource
     * operators from the {@code io.strimzi.operator.common.operator.resource.concurrent} package run their blocking
     * Kubernetes API calls on a Vert.x worker pool, so that all the blocking operations of the operator share a single,
     * configurable worker pool without the operators themselves depending on Vert.x.
     *
     * @param workerExecutor    The Vert.x worker executor to run the submitted tasks on
     *
     * @return  An {@link Executor} that runs submitted tasks on the given Vert.x worker executor (unordered)
     */
    public static Executor asExecutor(WorkerExecutor workerExecutor) {
        return command -> workerExecutor.executeBlocking(() -> {
            command.run();
            return null;
        }, false);
    }

    /**
     * Converts a {@link CompletionStage} (as returned by the non-Vert.x resource operators from the
     * {@code io.strimzi.operator.common.operator.resource.concurrent} package) into a Vert.x {@link Future}, completing
     * it on the Vert.x {@link Context} of the calling thread.
     * <p>
     * It gets the current Vert.x context using {@link Vertx#currentContext()}. It is intended to be called from code that
     * already runs on a Vert.x context (such as the reconciliation chains in the Cluster Operator). If no Vert.x context
     * is associated with the current thread, the resulting future is completed on whichever thread completes the stage.
     *
     * @param stage The completion stage to convert
     *
     * @return  A Vert.x Future completed on the current Vert.x context once the completion stage completes
     *
     * @param <T>   Type of the result
     */
    public static <T> Future<T> toFuture(CompletionStage<T> stage) {
        Context context = Vertx.currentContext();

        if (context != null) {
            return Future.fromCompletionStage(stage, context);
        } else {
            return Future.fromCompletionStage(stage);
        }
    }

    /**
     * Executes blocking code asynchronously
     *
     * @param vertx     Vert.x instance
     * @param supplier  Supplier with the blocking code
     *
     * @return  Future for returning the result
     *
     * @param <T>   Type of the result
     */
    public static <T> Future<T> async(Vertx vertx, Supplier<T> supplier) {
        return vertx.executeBlocking(supplier::get);
    }

    /**
     * Invoke the given {@code completed} supplier on a pooled thread approximately every {@code pollIntervalMs}
     * milliseconds until it returns true or {@code timeoutMs} milliseconds have elapsed.
     * @param reconciliation The reconciliation
     * @param vertx The vertx instance.
     * @param logContext A string used for context in logging.
     * @param logState The state we are waiting for use in log messages
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs The timeout, in milliseconds.
     * @param completed Determines when the wait is complete by returning true.
     * @return A future that completes when the given {@code completed} indicates readiness.
     */
    public static Future<Void> waitFor(Reconciliation reconciliation, Vertx vertx, String logContext, String logState, long pollIntervalMs, long timeoutMs, BooleanSupplier completed) {
        return waitFor(reconciliation, vertx, logContext, logState, pollIntervalMs, timeoutMs, completed, error -> false);
    }

    /**
     * Invoke the given {@code completed} supplier on a pooled thread approximately every {@code pollIntervalMs}
     * milliseconds until it returns true or {@code timeoutMs} milliseconds have elapsed.
     * @param reconciliation The reconciliation
     * @param vertx The vertx instance.
     * @param logContext A string used for context in logging.
     * @param logState The state we are waiting for use in log messages
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs The timeout, in milliseconds.
     * @param completed Determines when the wait is complete by returning true.
     * @param failOnError Determine whether a given error thrown by {@code completed},
     *                    should result in the immediate completion of the returned Future.
     * @return A future that completes when the given {@code completed} indicates readiness.
     */
    public static Future<Void> waitFor(Reconciliation reconciliation, Vertx vertx, String logContext, String logState, long pollIntervalMs, long timeoutMs, BooleanSupplier completed,
                                       Predicate<Throwable> failOnError) {
        Promise<Void> promise = Promise.promise();
        LOGGER.debugCr(reconciliation, "Waiting for {} to get {}", logContext, logState);
        long deadline = System.currentTimeMillis() + timeoutMs;
        Handler<Long> handler = new Handler<>() {
            @Override
            public void handle(Long timerId) {
                vertx.createSharedWorkerExecutor("kubernetes-ops-pool")
                        .executeBlocking(() -> {
                            boolean result;

                            try {
                                result = completed.getAsBoolean();
                            } catch (Throwable e) {
                                LOGGER.warnCr(reconciliation, "Caught exception while waiting for {} to get {}", logContext, logState, e);
                                throw e;
                            }

                            if (result) {
                                return null;
                            } else {
                                LOGGER.traceCr(reconciliation, "{} is not {}", logContext, logState);
                                throw new RuntimeException("Not " + logState + " yet");
                            }
                        })
                        .onComplete(res -> {
                            if (res.succeeded()) {
                                LOGGER.debugCr(reconciliation, "{} is {}", logContext, logState);
                                promise.complete();
                            } else {
                                if (failOnError.test(res.cause())) {
                                    promise.fail(res.cause());
                                } else {
                                    long timeLeft = deadline - System.currentTimeMillis();
                                    if (timeLeft <= 0) {
                                        String exceptionMessage = String.format("Exceeded timeout of %dms while waiting for %s to be %s", timeoutMs, logContext, logState);
                                        LOGGER.errorCr(reconciliation, exceptionMessage);
                                        promise.fail(new StrimziTimeoutException(exceptionMessage));
                                    } else {
                                        // Schedule ourselves to run again
                                        vertx.setTimer(Math.min(pollIntervalMs, timeLeft), this);
                                    }
                                }
                            }
                        });
            }
        };

        // Call the handler ourselves the first time
        handler.handle(null);

        return promise.future();
    }
}
