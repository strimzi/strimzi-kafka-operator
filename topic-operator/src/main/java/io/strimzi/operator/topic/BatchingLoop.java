/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.informers.cache.ItemStore;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates a queue (actually a deque) of {@link TopicEvent}s and a pool of threads (see {@link LoopRunnable}) servicing
 * the reconciliation of those events using a {@link BatchingTopicController}.
 * Any given KafkaTopic is only being reconciled by a single thread at any one time.
 */
class BatchingLoop {

    static final ReconciliationLogger LOGGER = ReconciliationLogger.create(BatchingLoop.class);

    private final BatchingTopicController controller;
    private final BlockingDeque<TopicEvent> queue;

    /**
     * The set of topics currently being reconciled by a controller.
     * Guarded by the monitor of the BatchingLoop.
     * This functions as mechanism for preventing concurrent reconciliation of the same topic.
     */
    private final Set<KubeRef> inFlight = new HashSet<>(); // guarded by this
    private final LoopRunnable[] threads;
    private final int maxBatchSize;
    private final long maxBatchLingerMs;
    private final ItemStore<KafkaTopic> itemStore;
    private final Runnable stop;
    private final int maxQueueSize;
    private final TopicOperatorMetricsHolder metrics;
    private final String namespace;

    public BatchingLoop(
            int maxQueueSize,
            BatchingTopicController controller,
            int maxThreads,
            int maxBatchSize,
            long maxBatchLingerMs,
            ItemStore<KafkaTopic> itemStore,
            Runnable stop,
            TopicOperatorMetricsHolder metrics,
            String namespace) {
        this.maxQueueSize = maxQueueSize;
        this.queue = new LinkedBlockingDeque<>(maxQueueSize);
        this.controller = controller;
        this.threads = new LoopRunnable[maxThreads];
        for (int i = 0; i < maxThreads; i++) {
            threads[i] = new LoopRunnable("LoopRunnable-" + i);
        }
        this.maxBatchSize = maxBatchSize;
        this.maxBatchLingerMs = maxBatchLingerMs;
        this.itemStore = itemStore;
        this.stop = stop;
        this.metrics = metrics;
        this.namespace = namespace;
    }

    /**
     * Starts the threads
     */
    public void start() {
        for (var thread : threads) {
            thread.start();
        }
    }

    /**
     * Stops the threads
     * @throws InterruptedException If interrupted while waiting for the threads to stop.
     */
    public void stop() throws InterruptedException {
        for (var thread : threads) {
            thread.requestStop();
        }
        for (var thread : threads) {
            thread.join();
        }
    }

    /**
     * Add an event to be reconciled to the tail of the {@link #queue}.
     * @param event The event
     */
    public void offer(TopicEvent event) {
        if (queue.offerFirst(event)) {
            LOGGER.debugOp("Item {} added to front of queue", event);
            metrics.reconciliationsMaxQueueSize(namespace).getAndUpdate(size -> Math.max(size, queue.size()));
        } else {
            LOGGER.errorOp("Queue length {} exceeded, stopping operator. Please increase {} environment variable.",
                    maxQueueSize,
                    TopicOperatorConfig.MAX_QUEUE_SIZE.key());
            this.stop.run();
        }
    }

    /**
     * The loop is alive if none of the threads have been blocked for more than 2 minutes.
     * "Blocked" means they're not returned to their outermost loop.
     * @return True if the loop is alive..
     */
    boolean isAlive() {
        for (var thread : threads) {
            if (!thread.isAlive()) {
                LOGGER.warnOp("isAlive returning false because {} is not alive", thread);
                return false;
            } else if (thread.msSinceLastLoop() > 120_000L) {
                LOGGER.warnOp("isAlive returning false because {} appears to be stuck", thread);
                return false;
            }
        }
        LOGGER.traceOp("isAlive returning true");
        return true;
    }

    /**
     * The loop is ready is all the threads have started and none have stopped.
     * I.e. all the threads are alive.
     * @return True if the loop is ready.
     */
    boolean isReady() {
        for (var thread : this.threads) {
            if (!thread.isAlive()) {
                LOGGER.warnOp("isReady returning false, because {} is not alive", thread);
                return false;
            }
        }
        LOGGER.traceOp("isReady returning true");
        return true;
    }

    /**
     * A thread that services the head of the {@link #queue}.
     */
    class LoopRunnable extends Thread {

        private volatile boolean stopRequested = false;

        LoopRunnable(String name) {
            super(name);
            setDaemon(false);
        }

        static final ReconciliationLogger LOGGER = ReconciliationLogger.create(LoopRunnable.class);
        private volatile long lastLoop = System.nanoTime();

        long msSinceLastLoop() {
            return (System.nanoTime() - lastLoop) / 1_000_000;
        }

        @Override
        public void run() {
            LOGGER.debugOp("Entering run()");
            Batch batch = new Batch(maxBatchSize);
            int batchId = 0;
            lastLoop = System.nanoTime();
            while (!runOnce(batchId, batch)) {
                batchId++;
                lastLoop = System.nanoTime();
            }
            LOGGER.debugOp("Exiting run()");
        }

        public void requestStop() {
            LOGGER.infoOp("Stop requested");
            // In theory interrupting the thread should be enough to cause the InterruptedException
            // to propagate up to runOnce(), but we can't completely guarantee that
            // everything handles interruption properly, so we use stopRequested to ensure
            // we'll at least stop after the return from stopOnce(), even if the exception gets swallowed
            this.stopRequested = true;
            this.interrupt();
        }

        private boolean runOnce(int batchId, Batch batch) {
            try {
                synchronized (BatchingLoop.this) {
                    // remove the old batch from the inflight set and reset the batch
                    LOGGER.traceOp("[Batch #{}] Removing batch from inflight", batchId - 1);
                    batch.toUpdate.stream().map(TopicEvent::toRef).forEach(inFlight::remove);
                    batch.toDelete.stream().map(TopicEvent::toRef).forEach(inFlight::remove);
                    batch.clear();
                    // fill a new batch
                    fillBatch(batchId, batch);
                }

                if (batch.size() > 0) {
                    LOGGER.infoOp("[Batch #{}] Reconciling batch of {} topics", batchId, batch.size());
                    // perform reconciliation on new batch
                    if (!batch.toUpdate.isEmpty()) {
                        controller.onUpdate(batch.toUpdate.stream().map(upsert -> lookup(batchId, upsert)).filter(Objects::nonNull).toList());
                    }
                    if (!batch.toDelete.isEmpty()) {
                        controller.onDelete(batch.toDelete.stream().map(td -> new ReconcilableTopic(
                            new Reconciliation("delete", "KafkaTopic", td.namespace(), td.name()), td.topic(), TopicOperatorUtil.topicName(td.topic()))).toList());
                    }
                    LOGGER.infoOp("[Batch #{}] Batch reconciliation completed", batchId);
                } else {
                    LOGGER.traceOp("[Batch #{}] Empty batch", batchId);
                }
            } catch (InterruptedException e) {
                LOGGER.infoOp("[Batch #{}] Interrupted", batchId);
                return true;
            } catch (Exception e) {
                LOGGER.errorOp("[Batch #{}] Unexpected exception", batchId, e);
            }
            return stopRequested;
        }

        private ReconcilableTopic lookup(int batchId, TopicUpsert topicUpsert) {
            var key = Cache.namespaceKeyFunc(topicUpsert.namespace(), topicUpsert.name());
            var kt = itemStore.get(key);
            if (kt != null) {
                LOGGER.traceOp("[Batch #{}] Lookup from item store for {} yielded KafkaTopic with resourceVersion {}",
                        batchId, topicUpsert, BatchingTopicController.resourceVersion(kt));
                var r = new Reconciliation("upsert", "KafkaTopic", topicUpsert.namespace(), topicUpsert.name());
                LOGGER.debugOp("[Batch #{}] Contains {}", batchId, r);
                return new ReconcilableTopic(r, kt, TopicOperatorUtil.topicName(kt));
            } else {
                // Null can happen if the KafkaTopic has been deleted from Kube and we've not yet processed
                // the corresponding delete event
                LOGGER.traceOp("[Batch #{}] Lookup from item store for {} yielded nothing",
                        batchId, topicUpsert);
                return null;
            }
        }

        private void fillBatch(int batchId, Batch batch) throws InterruptedException {
            LOGGER.traceOp("[Batch #{}] Filling", batchId);
            List<TopicEvent> rejected = new ArrayList<>();

            final long deadlineNanoTime = System.nanoTime() + maxBatchLingerMs * 1_000_000;
            while (true) {
                if (batch.size() >= maxBatchSize) {
                    LOGGER.traceOp("[Batch #{}] Reached maxBatchSize, batch complete", batchId, maxBatchSize);
                    break;
                }

                long timeoutNs = deadlineNanoTime - System.nanoTime();
                if (timeoutNs <= 0) {
                    LOGGER.traceOp("[Batch #{}] {}ms linger expired", batchId, maxBatchLingerMs);
                    break;
                }
                LOGGER.traceOp("[Batch #{}] Taking next item from deque head with timeout {}ns", batchId, timeoutNs);
                TopicEvent topicEvent = queue.pollFirst(timeoutNs, TimeUnit.NANOSECONDS);

                if (topicEvent == null) {
                    LOGGER.traceOp("[Batch #{}] Linger expired, batch complete", batchId);
                    break;
                }
                addToBatch(batchId, batch, rejected, topicEvent);
            }
            LOGGER.traceOp("[Batch #{}] Filled with {} topics", batchId, batch.size());
            metrics.reconciliationsMaxBatchSize(namespace).getAndUpdate(size -> Math.max(size, batch.size()));

            // here we need a deque and can push `rejected` back on the front of the queue
            //      where they can be taken by the next thread.
            for (int i = rejected.size() - 1; i >= 0; i--) {
                TopicEvent item = rejected.get(i);
                offer(item);
            }
        }

        private void addToBatch(int batchId, Batch batch, List<TopicEvent> rejected, TopicEvent topicEvent) {
            // We could add logic here to cope properly with interleaved upserts and deletes
            // of the same topic in the same batch.
            // E.g. upset then delete is equivalent to just a delete
            // It's actually a bit tricky since you have to process the events in reverse order to correctly
            // simplify them, so `Batch` would have to be something like a `Map<Ref, List<TopicEvent>>`.
            KubeRef ref = topicEvent.toRef();
            if (inFlight.add(ref)) {
                // wasn't already inflight
                LOGGER.debugOp("[Batch #{}] Adding {}", batchId, topicEvent);
                if (topicEvent instanceof TopicUpsert) {
                    batch.toUpdate.add((TopicUpsert) topicEvent);
                } else {
                    batch.toDelete.add((TopicDelete) topicEvent);
                }
            } else {
                LOGGER.debugOp("[Batch #{}] Rejecting item {}, already inflight", batchId, topicEvent);
                rejected.add(topicEvent);
                metrics.lockedReconciliationsCounter(namespace).increment();
            }
        }
    }

    private record Batch(List<TopicUpsert> toUpdate, List<TopicDelete> toDelete) {
        public Batch(int maxBatchSize) {
            this(new ArrayList<>(maxBatchSize), new ArrayList<>(maxBatchSize));
        }

        public void clear() {
            toUpdate.clear();
            toDelete.clear();
        }

        public int size() {
            return toUpdate.size() + toDelete.size();
        }
    }
}
