/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import org.apache.kafka.common.KafkaFuture;

import java.util.concurrent.CompletableFuture;

/**
 * Class with various utility methods that use or depend on CompletableFuture.
 */
public final class CompletableFutureUtil {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CompletableFutureUtil.class);

    private CompletableFutureUtil() {
        // Not used
    }


    /**
     * Converts Kafka Future to CompletableFuture
     *
     * @param reconciliation    Reconciliation marker
     * @param kf                Kafka future
     *
     * @return  CompletableFuture based on the Kafka future
     *
     * @param <T>   Return type of the CompletableFuture
     */
    public static <T> CompletableFuture<T> kafkaFutureToCompletableFuture(Reconciliation reconciliation, KafkaFuture<T> kf) {
        CompletableFuture<T> future = new CompletableFuture<>();
        if (kf != null) {
            kf.whenComplete((result, error) -> {
                if (error != null) {
                    future.completeExceptionally(error);
                } else {
                    future.complete(result);
                }
            });
            return future;
        } else {
            if (reconciliation != null) {
                LOGGER.traceCr(reconciliation, "KafkaFuture is null");
            } else {
                LOGGER.traceOp("KafkaFuture is null");
            }

            return CompletableFuture.completedFuture(null);
        }
    }
}
