/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.strimzi.operator.common.operator.resource.concurrent.Informer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utilities for working with informers
 */
public class InformerUtils {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(InformerUtils.class);

    /**
     * Synchronously stops one or more informers. It will stop them and then wait for up to the specified timeout for
     * each of them to actually stop.
     *
     * @param timeoutMs     Timeout in milliseconds for how long we will wait for each informer to stop
     * @param informers     Informers which should be stopped.
     */
    public static void stopAll(long timeoutMs, Informer<?>... informers) {
        LOGGER.infoOp("Stopping informers");
        for (Informer<?> informer : informers)    {
            informer.stop();
        }

        try {
            for (Informer<?> informer : informers)    {
                informer.stopped().toCompletableFuture().get(timeoutMs, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            // We just log the error as we are anyway shutting down
            LOGGER.warnOp("Failed to wait for the informers to stop", e);
        }
    }
}
