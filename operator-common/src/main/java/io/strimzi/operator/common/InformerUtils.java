/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.client.informers.SharedIndexInformer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utilities for working with informers
 */
public class InformerUtils {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(InformerUtils.class);

    /**
     * Logs exceptions in the informers to give us a better overview of what is happening.
     *
     * @param type          Type of the informer
     * @param isStarted     Flag indicating whether the informer is already started
     * @param throwable     Throwable describing the exception which occurred
     *
     * @return  Boolean indicating whether the informer should retry or not.
     */
    public static boolean loggingExceptionHandler(String type, boolean isStarted, Throwable throwable) {
        LOGGER.errorOp("Caught exception in the " + type + " informer which is " + (isStarted ? "started" : "not started"), throwable);
        // We always want the informer to retry => we just want to log the error
        return true;
    }

    /**
     * Watches for informers to not stop unless we are shutting down the controller. If it stops unexpectedly, we will
     * terminate the operator.
     *
     * @param type      Type of the informer
     * @param reason    Reason why the informer stopped
     * @param stopping  Flag indicating if the controller shutdown is in progress (in which case the informer is expected to stop)
     */
    public static void stoppedInformerHandler(String type, Throwable reason, boolean stopping) {
        if (!stopping) {
            // the informer is not being stopped, so this is unexpected!
            if (reason != null) {
                LOGGER.errorOp("{} informer stopped unexpectedly", type, reason);
            } else {
                LOGGER.errorOp("{} informer stopped unexpectedly without a reason", type);
            }
        } else {
            LOGGER.infoOp("{} informer stopped", type);
        }
    }

    /**
     * Synchronously stops one or more informers. It will stop them and then wait for up to the specified timeout for
     * each of them to actually stop.
     *
     * @param timeoutMs     Timeout in milliseconds for how long we will wait for each informer to stop
     * @param informers     Informers which should be stopped.
     */
    public static void stopAll(long timeoutMs, SharedIndexInformer<?>... informers) {
        LOGGER.infoOp("Stopping informers");
        for (SharedIndexInformer<?> informer : informers)    {
            informer.stop();
        }

        try {
            for (SharedIndexInformer<?> informer : informers)    {
                informer.stopped().toCompletableFuture().get(timeoutMs, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            // We just log the error as we are anyway shutting down
            LOGGER.warnOp("Failed to wait for the informers to stop", e);
        }
    }
}
