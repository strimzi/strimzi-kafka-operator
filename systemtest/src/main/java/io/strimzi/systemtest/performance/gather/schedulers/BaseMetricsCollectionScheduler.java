/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.gather.schedulers;

import io.strimzi.systemtest.performance.PerformanceConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class BaseMetricsCollectionScheduler provides a structured approach to gather metrics.
 * It defines a template for scheduling, collecting, and managing the lifecycle of metrics collection tasks.
 * The metrics are stored in a TreeMap, which preserves the temporal order of data collection.
 *
 * <p>This class should be extended by subclasses that provide specific implementations for the
 * {@link #collectMetrics()} and {@link #buildMetricsMap()} methods to collect and structure metrics data,
 * respectively. This class manages a single-threaded scheduler to ensure thread safety and proper sequencing
 * of metrics collection events.</p>
 *
 * <p>Usage: Extend this class and implement the abstract methods to collect and map metrics.
 * Start the metrics collection process by calling {@link #startCollecting(long, long, TimeUnit)} with
 * desired timing configurations.</p>
 *
 * <p>Note: Because it uses a TreeMap for storing metrics history, which is not thread-safe,
 * this class is designed to run as a single thread to avoid concurrent modifications and ensure data integrity.</p>
 */
public abstract class BaseMetricsCollectionScheduler {

    protected static final Logger LOGGER = LogManager.getLogger(BaseMetricsCollectionScheduler.class);

    protected final String selector;
    protected final Map<Long, Map<String, List<Double>>> metricsStore = new TreeMap<>();
    protected ScheduledExecutorService scheduler;

    public BaseMetricsCollectionScheduler(String selector) {
        this.selector = selector;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Executes the complete metrics collection process.
     * This is the template method that calls the abstract methods to collect and map metrics,
     * stores them, and prints the current metrics.
     */
    public void executeMetricsCollection() {
        collectMetrics();
        Map<String, List<Double>> metrics = buildMetricsMap();
        storeMetrics(metrics);
        printCurrentMetrics();
    }

    /**
     * Abstract method to collect metrics. This must be implemented by subclasses to gather specific metrics data.
     */
    protected abstract void collectMetrics();

    /**
     * Abstract method to build the structured map of metrics. This must be implemented by subclasses to organize
     * the collected metrics data.
     *
     * @return A map where each key is a metric identifier and the value is a list of collected data points.
     */
    protected abstract Map<String, List<Double>> buildMetricsMap();

    /**
     * Starts the periodic collection of metrics.
     *
     * @param initialDelay the initial delay before metrics collection starts, in the given time unit.
     * @param interval the interval at which metrics collection should repeat.
     * @param unit the time unit of the initial delay and interval.
     */
    public void startCollecting(long initialDelay, long interval, TimeUnit unit) {
        // Capture the context in the thread where startCollecting is called
        final ExtensionContext currentContext = ResourceManager.getTestContext();

        final Runnable task = () -> {
            // Set the context specifically for a new thread to ensure thread-local data is available
            ResourceManager.setTestContext(currentContext);
            executeMetricsCollection();
        };

        this.scheduler.scheduleAtFixedRate(task, initialDelay, interval, unit);
        LOGGER.info("Started collecting metrics every {} {}.", interval, unit);
    }

    /**
     * Starts the metrics collection process immediately with default interval settings.
     */
    public void startCollecting() {
        startCollecting(0, PerformanceConstants.DEFAULT_METRICS_POLLING_INTERVAL_SEC, TimeUnit.SECONDS);
    }

    /**
     * Stops the scheduled metrics collection.
     * Attempts to terminate the scheduler and logs any issues encountered during shutdown.
     */
    public void stopCollecting() {
        if (this.scheduler != null) {
            this.scheduler.shutdownNow();
            try {
                if (!this.scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.error("Scheduler did not terminate in the specified time.");
                }
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted during shutdown.", e);
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Stores the collected metrics along with their collection timestamp.
     *
     * @param metrics The structured map of metrics to be stored.
     */
    protected void storeMetrics(Map<String, List<Double>> metrics) {
        Long timeWhenMetricsWereCollected = System.nanoTime() / 1_000_000; // Convert nanoseconds to milliseconds
        this.metricsStore.put(timeWhenMetricsWereCollected, metrics);
    }

    public Map<Long, Map<String, List<Double>>> getMetricsStore() {
        return metricsStore;
    }

    protected void printCurrentMetrics() {
        this.metricsStore.forEach((key, valueList) -> {
            if (valueList.isEmpty()) {
                LOGGER.debug(key + " => [No data]");
            } else {
                LOGGER.debug(key + " => " + valueList);
            }
        });
    }

    /**
     * Calculates the load average per core based on system load average and CPU count.
     *
     * @param systemLoadAverage1m A list containing the 1-minute system load average.
     * @param systemCpuCount A list containing the count of CPUs.
     * @return A list containing the load average per CPU core, or an empty list if input lists are empty.
     */
    protected List<Double> calculateLoadAveragePerCore(List<Double> systemLoadAverage1m, List<Double> systemCpuCount) {
        if (!systemLoadAverage1m.isEmpty() && !systemCpuCount.isEmpty()) {
            double loadAverage = systemLoadAverage1m.get(0);
            double cpuCount = systemCpuCount.get(0);

            // Calculate load average per core
            double loadAveragePerCore = loadAverage / cpuCount;

            // Return this as a List<Double>
            return Collections.singletonList(loadAveragePerCore);
        } else {
            return Collections.emptyList();
        }
    }
}
