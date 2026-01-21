/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.utils;

import io.skodjob.testframe.resources.KubeResourceManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Shared utility class for concurrent performance test execution.
 * Provides common ExecutorService management and concurrent processing patterns
 * used by both TopicOperator and UserOperator performance tests.
 */
public final class PerformanceTestExecutorService {

    private static final Logger LOGGER = LogManager.getLogger(PerformanceTestExecutorService.class);
    private static final int CONCURRENCY_HINT = Runtime.getRuntime().availableProcessors() * 10;
    private static final long COOLDOWN_PERIOD_MS = 5_000;
    private static final int EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 5;

    private static ExecutorService executorService = createFixedThreadPool();

    private PerformanceTestExecutorService() { } // Prevent instantiation

    /**
     * Creates a new fixed thread pool executor.
     * The pool size is based on the concurrency hint to provide predictable
     * resource usage during performance tests.
     *
     * @return a new ExecutorService using a fixed thread pool
     */
    private static ExecutorService createFixedThreadPool() {
        LOGGER.info("Creating fixed thread pool with {} threads", CONCURRENCY_HINT);
        return Executors.newFixedThreadPool(CONCURRENCY_HINT);
    }

    /**
     * Gets the thread pool size used for concurrent execution.
     *
     * @return the thread pool size
     */
    public static int getThreadPoolSize() {
        return CONCURRENCY_HINT;
    }

    /**
     * Gets the current ExecutorService, reinitializing if necessary.
     *
     * @return the active ExecutorService
     */
    public static ExecutorService getExecutorService() {
        if (executorService.isShutdown() || executorService.isTerminated()) {
            executorService = createFixedThreadPool();
            LOGGER.info("Reinitialized fixed thread pool ExecutorService for new test run.");
        }
        return executorService;
    }

    /**
     * Stops the executor service gracefully, waiting for tasks to complete.
     */
    public static void stopExecutor() {
        if (!executorService.isShutdown()) {
            try {
                executorService.shutdown();
                if (!executorService.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }
    }

    /**
     * Processes resources concurrently using a fixed thread pool.
     * This method handles the common pattern of processing creation, modification, and deletion
     * for each resource concurrently.
     *
     * @param numberOfResources     The number of resources to be processed.
     * @param spareEvents           The number of spare events to be consumed during the process.
     * @param warmUpTasksToProcess  The number of warmup tasks for JIT optimization (used as offset).
     * @param lifecycleAction       A BiConsumer that performs the full lifecycle for a given resource index.
     *                              First parameter is the resource index, second is the ExtensionContext.
     * @param resourceTypeName      Name of the resource type for logging (e.g., "KafkaTopic", "KafkaUser").
     *
     * @return                      The total time taken to complete all lifecycles in milliseconds.
     */
    public static long processResourcesConcurrently(
            int numberOfResources,
            int spareEvents,
            int warmUpTasksToProcess,
            BiConsumer<Integer, ExtensionContext> lifecycleAction,
            String resourceTypeName) {

        ExecutorService executor = getExecutorService();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        ExtensionContext extensionContext = KubeResourceManager.get().getTestContext();

        long startTime = System.nanoTime();

        for (int resourceIndex = warmUpTasksToProcess; resourceIndex < numberOfResources + warmUpTasksToProcess; resourceIndex++) {
            final int finalResourceIndex = resourceIndex;
            CompletableFuture<Void> future = CompletableFuture.runAsync(
                () -> lifecycleAction.accept(finalResourceIndex, extensionContext),
                executor
            );
            futures.add(future);
        }

        // consume spare events
        for (int j = 0; j < spareEvents; j++) {
            futures.add(j, CompletableFuture.completedFuture(null));
        }

        // Wait for all resources to complete their lifecycle
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        LOGGER.info("All {} lifecycles completed.", resourceTypeName);

        long allTasksTimeMs = Duration.ofNanos(System.nanoTime() - startTime).toMillis();

        if (warmUpTasksToProcess != 0) {
            performCooldown();
        }

        return allTasksTimeMs;
    }

    /**
     * Performs a cooldown period between tests to reduce interference.
     */
    private static void performCooldown() {
        LOGGER.info("Cooling down");
        try {
            Thread.sleep(COOLDOWN_PERIOD_MS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
