/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.controller;

import io.micrometer.core.instrument.Timer;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.metrics.ControllerMetricsHolder;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Abstract controller loop provides the shared functionality for reconciling resources in Strimzi controllers. It takes
 * an event from a queue passed in controller and reconciles it.
 */
public abstract class AbstractControllerLoop {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(AbstractControllerLoop.class);
    private static final long PROGRESS_WARNING_MS = 60_000L;

    private final String name;
    private final Thread controllerThread;
    private final ControllerQueue workQueue;
    private final ReconciliationLockManager lockManager;
    private final ScheduledExecutorService scheduledExecutor;

    private volatile boolean stop = false;
    private volatile boolean running = false;

    /**
     * Creates the controller. The controller should normally exist once per operator for cluster-wide mode or once per
     * namespace for namespaced mode.
     *
     * @param name                  The name of this controller loop. The name should help to identify what kind
     *                              of look this is and what does it control / reconciler.
     * @param workQueue             Queue from which events should be consumed
     * @param lockManager           Lock manager for making sure no parallel reconciliations for a given resource can happen
     * @param scheduledExecutor     Scheduled executor service used to run the progress warnings
     */
    public AbstractControllerLoop(String name, ControllerQueue workQueue, ReconciliationLockManager lockManager, ScheduledExecutorService scheduledExecutor) {
        this.name = name;
        this.workQueue = workQueue;
        this.lockManager = lockManager;
        this.scheduledExecutor = scheduledExecutor;
        this.controllerThread = new Thread(new Runner(), name);
    }

    /**
     * The main reconciliation logic which handles the reconciliations.
     *
     * @param reconciliation    Reconciliation identifier used for logging
     */
    protected abstract void reconcile(Reconciliation reconciliation);

    /**
     * Returns the Controller Metrics Holder instance, which is used to hold the various controller metrics
     *
     * @return Controller metrics holder instance
     */
    protected abstract ControllerMetricsHolder metrics();

    /**
     * Starts the controller: this method creates a new thread in which the controller will run
     */
    public void start() {
        LOGGER.debugOp("{}: Starting the controller loop", name);
        controllerThread.start();
    }

    /**
     * Stops the controller: this method sets the stop flag and interrupt the run loop
     *
     * @throws InterruptedException InterruptedException is thrown when interrupted while joining the thread
     */
    public void stop() throws InterruptedException {
        LOGGER.infoOp("{}: Requesting the controller loop to stop", name);
        this.stop = true;
        controllerThread.interrupt();
        controllerThread.join();
    }

    /**
     * Indicates whether the controller is inside the run loop.
     *
     * @return  True when the controller is in the run loop, false otherwise
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Indicates whether the controller loop thread is alive or not
     *
     * @return  True when the controller loop thread is alive, false otherwise
     */
    public boolean isAlive() {
        return controllerThread.isAlive();
    }

    /**
     * Wrapper method to handle obtaining the lock for the resource or re-queueing the reconciliation if the lock is in
     * use. When it gets the lock, it calls the reconcileWrapper method.
     *
     * @param reconciliation    Reconciliation marker
     */
    private void reconcileWithLock(SimplifiedReconciliation reconciliation) {
        String lockName = reconciliation.lockName();
        boolean requeue = false;

        try {
            boolean locked = lockManager.tryLock(lockName, 1_000, TimeUnit.MILLISECONDS);

            if (locked) {
                try {
                    reconcileWrapper(reconciliation.toReconciliation());
                } finally {
                    // We have to unlock the resource in any situation
                    lockManager.unlock(lockName);
                }
            } else {
                // Failed to get the lock => other reconciliation is in progress
                LOGGER.warnOp("{}: Failed to acquire lock {}. The resource will be re-queued for later.", name, lockName);
                metrics().lockedReconciliationsCounter(reconciliation.namespace).increment();
                requeue = true;
            }

        } catch (InterruptedException e) {
            LOGGER.warnOp("{}: Interrupted while trying to acquire lock {}. The resource will be re-queued for later.", name, lockName);
            metrics().lockedReconciliationsCounter(reconciliation.namespace).increment();
            requeue = true;
        }

        // Failed to get the lock. We will requeue the resource for next time
        if (requeue) {
            workQueue.enqueue(reconciliation);
        }
    }

    /**
     * Wrapper method to handle reconciliation. It is used to handle common tasks such as:
     *     - Progress warnings
     *     - Reconciliation metrics
     *
     * @param reconciliation    Reconciliation marker
     */
    private void reconcileWrapper(Reconciliation reconciliation) {
        // Tasks before reconciliation
        ScheduledFuture<?> progressWarning = scheduledExecutor
                .scheduleAtFixedRate(() -> LOGGER.infoCr(reconciliation, "Reconciliation is in progress"), PROGRESS_WARNING_MS, PROGRESS_WARNING_MS, TimeUnit.MILLISECONDS);
        metrics().reconciliationsCounter(reconciliation.namespace()).increment(); // Increase the reconciliation counter
        Timer.Sample reconciliationTimerSample = Timer.start(metrics().metricsProvider().meterRegistry()); // Start the reconciliation timer

        // Reconciliation
        try {
            reconcile(reconciliation);
        } finally   {
            // Tasks after reconciliation
            reconciliationTimerSample.stop(metrics().reconciliationsTimer(reconciliation.namespace())); // Stop the reconciliation timer
            progressWarning.cancel(true); // Stop the progress warning
        }
    }

    /**
     * Runner class which is used to run the controller loop. This is implemented as a private inner class to not expose
     * it as a public method.
     */
    private class Runner implements Runnable {
        /**
         * The run loop of the controller loop thread. It picks reconciliations from the work queue and executes them.
         */
        @Override
        public void run() {
            LOGGER.debugOp("{}: Starting", name);
            running = true; // We indicate that we are entering the controller loop

            while (!stop) {
                try {
                    LOGGER.debugOp("{}: Waiting for next event from work queue", name);
                    SimplifiedReconciliation reconciliation = workQueue.take();
                    reconcileWithLock(reconciliation);
                } catch (InterruptedException e) {
                    LOGGER.debugOp("{}: was interrupted", name, e);
                } catch (Exception e) {
                    LOGGER.warnOp("{}: reconciliation failed", name, e);
                }
            }

            LOGGER.infoOp("{}: Stopping", name);
            running = false; // We indicate that we are exiting the controller loop
        }
    }
}
