/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.TimeoutException;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.common.KafkaFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
                                        promise.fail(new TimeoutException(exceptionMessage));
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

    /**
     * Converts Kafka Future to Vert.x future
     *
     * @param reconciliation    Reconciliation marker
     * @param vertx             Vert.x instance
     * @param kf                Kafka future
     *
     * @return  Vert.x future based on the Kafka future
     *
     * @param <T>   Return type of the future
     */
    public static <T> Future<T> kafkaFutureToVertxFuture(Reconciliation reconciliation, Vertx vertx, KafkaFuture<T> kf) {
        Promise<T> promise = Promise.promise();
        if (kf != null) {
            kf.whenComplete((result, error) -> vertx.runOnContext(ignored -> {
                if (error != null) {
                    promise.fail(error);
                } else {
                    promise.complete(result);
                }
            }));
            return promise.future();
        } else {
            if (reconciliation != null) {
                LOGGER.traceCr(reconciliation, "KafkaFuture is null");
            } else {
                LOGGER.traceOp("KafkaFuture is null");
            }

            return Future.succeededFuture();
        }
    }

    /**
     * Converts a {@link CompletableFuture} to a Vert.x {@link io.vertx.core.Future}.
     *
     * Unlike {@code Future.fromCompletionStage()}, this method provides customized handling
     * of exceptions when the {@code CompletableFuture} completes exceptionally.
     *
     * If the {@code CompletableFuture} completes with an exception, this method extracts the
     * root cause (instead of wrapping it in a {@code CompletionException}) and fails the
     * Vert.x {@code Future} with the underlying exception. This allows for more precise
     * error handling by avoiding the generic {@code CompletionException} wrapper.
     *
     * @param cf    the {@code CompletableFuture} to convert
     *
     * @return      Vert.x {@code Future} that reflects the same result or exception
     *
     * @param <T>   Return type of the future
     */
    public static <T> Future<T> completableFutureToVertxFuture(CompletableFuture<T> cf) {
        Promise<T> promise = Promise.promise();
        cf.whenComplete((result, exception) -> {
            if (exception != null) {
                if (exception instanceof CompletionException && exception.getCause() != null) {
                    promise.fail(exception.getCause());
                } else {
                    promise.fail(exception);
                }
            } else {
                promise.complete(result);
            }
        });
        return promise.future();
    }
}
