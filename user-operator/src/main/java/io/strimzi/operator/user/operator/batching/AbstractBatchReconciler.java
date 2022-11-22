/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.batching;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class for collecting Kafka Admin API requests and sending them to Kafka in batches. The batches are sent
 * when we collect some (configurable) amount of requests or after some (configurable) time interval
 */
public abstract class AbstractBatchReconciler<T> {
    private final static Logger LOGGER = LogManager.getLogger(AbstractBatchReconciler.class);

    private final BlockingQueue<T> queue;
    private final int maxBatchSize;
    private final int maxBatchTime;
    private final Thread batchHandlerThread;

    private volatile CountDownLatch batchSize;
    private volatile boolean stop = false;

    /**
     * Creates the BatchReconciler
     *
     * @param name          Name of the reconciler
     * @param queueSize     Size of the queue for queueing the reconciliation requests
     * @param maxBatchSize  Maximal size of the batch
     * @param maxBatchTime  Maximal time to wait before batch is executed
     */
    public AbstractBatchReconciler(String name, int queueSize, int maxBatchSize, int maxBatchTime) {
        if (maxBatchSize > queueSize)   {
            throw new IllegalArgumentException("Maximum batch size cannot be bigger than queue size");
        }

        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.batchSize = new CountDownLatch(0);
        this.maxBatchSize = maxBatchSize;
        this.maxBatchTime = maxBatchTime;
        this.batchHandlerThread = new Thread(new Runner(), name);
    }

    /**
     * Method responsible for sending the batch of requests to Apache Kafka and handling the result
     *
     * @param items Items which should be reconciled
     */
    protected abstract void reconcile(Collection<T> items);

    /**
     * Enqueues a reconciliation request
     *
     * @param item  Reconciliation request which should be enqueued
     *
     * @throws InterruptedException Thrown when interrupted while enqueuing the resource
     */
    public void enqueue(T item) throws InterruptedException {
        queue.put(item);

        if (queue.size() >= maxBatchSize)   {
            batchSize.countDown();
        }
    }

    /**
     * Starts a new batch of requests. It drains the queue and passes the batch of requests to the reconcile method.
     *
     * @param batchSizeReached  Indicates whether the batch is triggered because we reached the maximal batch size
     *                          (true) or the time limit (false)
     */
    private void handleBatch(boolean batchSizeReached)  {
        if (batchSizeReached) {
            LOGGER.trace("{}: Running the next batch of the BatchReconciler because maximum batch size was reached", batchHandlerThread.getName());
        } else {
            LOGGER.trace("{}: Running the next batch of the BatchReconciler because maximum batch time was reached", batchHandlerThread.getName());
        }

        List<T> batch = new ArrayList<>();
        int batchSize = queue.drainTo(batch, maxBatchSize);

        if (batchSize > 0)  {
            LOGGER.debug("{}: Processing batch of {} records in the BatchReconciler", batchHandlerThread.getName(), batchSize);
            reconcile(batch);
        }
    }

    /**
     * Starts the reconciler: this method creates a new thread in which the controller will run
     */
    public void start()  {
        LOGGER.info("{}: Starting the BatchReconciler", batchHandlerThread.getName());
        batchHandlerThread.start();
    }

    /**
     * Stops the reconciler: this method sets the stop flag and interrupt the run loop
     *
     * @throws InterruptedException InterruptedException is thrown when interrupted while joining the thread
     */
    public void stop() throws InterruptedException {
        LOGGER.info("{}: Requesting the BatchReconciler to stop", batchHandlerThread.getName());
        this.stop = true;
        batchHandlerThread.interrupt();
        batchHandlerThread.join();
    }

    /**
     * Runner class which is used to run the controller loop. This is implemented as a private inner class to not expose
     * it as a public method.
     */
    private class Runner implements Runnable {
        /**
         * The run loop of the batch reconciler thread. It picks up the queued Kafka Admin API requests and sends them
         * to Kafka.
         */
        @Override
        public void run() {
            LOGGER.info("{}: BatchReconciler is running", batchHandlerThread.getName());

            while (!stop)    {
                try {
                    LOGGER.trace("{}: Waiting for the next batch of the BatchReconciler", batchHandlerThread.getName());
                    boolean batchSizeReached = batchSize.await(maxBatchTime, TimeUnit.MILLISECONDS);

                    if (batchSizeReached) {
                        batchSize = new CountDownLatch(1);
                    }

                    handleBatch(batchSizeReached);
                } catch (InterruptedException e) {
                    LOGGER.debug("{}: BatchReconciler was interrupted", batchHandlerThread.getName(), e);
                }
            }

            LOGGER.info("{}: Stopping the BatchReconciler", batchHandlerThread.getName());
        }
    }
}
