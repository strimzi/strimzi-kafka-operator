/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.controller;

import io.strimzi.operator.common.metrics.ControllerMetricsHolder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Controller queue class wraps a Blocking queue and exposes the methods used by controllers. This includes taking
 * events from the queue and enqueueing events into the queue.
 */
public class ControllerQueue {
    private final static Logger LOGGER = LogManager.getLogger(ControllerQueue.class);

    /*test*/ final BlockingQueue<SimplifiedReconciliation> queue;
    private final ControllerMetricsHolder metrics;

    /**
     * Creates the controller queue. The controller should normally exist once per operator for cluster-wide mode or once per
     * namespace for namespaced mode.
     *
     * @param queueSize     The capacity of the work queue
     * @param metrics       Holder for the controller metrics
     */
    public ControllerQueue(int queueSize, ControllerMetricsHolder metrics) {
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.metrics = metrics;
    }

    /**
     * @return  Takes the next item from the queue. Blocks if the queue is empty.
     *
     * @throws InterruptedException InterruptedException is thrown if interrupted while waiting to get the next resource from the queue (e.g. when the queue is empty)
     */
    public SimplifiedReconciliation take() throws InterruptedException {
        return queue.take();
    }

    /**
     * Enqueues the next reconciliation. It checks whether another reconciliation for the same resource is already in
     * the queue and enqueues the new event only if it is not there yet.
     *
     * @param reconciliation    Reconciliation identifier
     */
    public void enqueue(SimplifiedReconciliation reconciliation)    {
        if (!queue.contains(reconciliation)) {
            LOGGER.debug("Enqueueing {} {} in namespace {}", reconciliation.kind, reconciliation.name, reconciliation.namespace);
            if (!queue.offer(reconciliation))    {
                LOGGER.warn("Failed to enqueue an event because the controller queue is full");
            }
        } else {
            metrics.alreadyEnqueuedReconciliationsCounter(reconciliation.namespace).increment(); // Increase the metrics counter
            LOGGER.debug("{} {} in namespace {} is already enqueued => ignoring", reconciliation.kind, reconciliation.name, reconciliation.namespace);
        }
    }

}
