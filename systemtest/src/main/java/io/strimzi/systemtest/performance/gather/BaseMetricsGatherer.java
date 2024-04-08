/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.gather;

import io.strimzi.systemtest.performance.PerformanceConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class for gathering metrics. This class provides the basic framework
 * for scheduling metrics collection, storing metrics history, and managing the lifecycle
 * of the metrics collection task.
 *
 * <p>Subclasses should implement the collectMetrics method to collect specific metrics and
 * the buildMetricsMap method to structure those metrics.</p>
 */
public abstract class BaseMetricsGatherer {

    protected static final Logger LOGGER = LogManager.getLogger(BaseMetricsGatherer.class);

    protected final String selector;
    protected final Map<Long, Map<String, List<Double>>> metricsHistory = new TreeMap<>();
    protected ScheduledExecutorService scheduler;

    public BaseMetricsGatherer(String selector) {
        this.selector = selector;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    protected abstract void collectMetrics();
    protected abstract Map<String, List<Double>> buildMetricsMap();

    public void startCollecting(long initialDelay, long interval, TimeUnit unit) {
        final Runnable task = this::collectMetrics;
        this.scheduler.scheduleAtFixedRate(task, initialDelay, interval, unit);
        LOGGER.info("Started collecting metrics every {} {}.", interval, unit);
    }

    public void startCollecting() {
        startCollecting(0, PerformanceConstants.POLLING_METRICS_INTERVAL_DEFAULT, TimeUnit.SECONDS);
    }

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

    public Map<Long, Map<String, List<Double>>> getMetricsHistory() {
        return metricsHistory;
    }

    protected void printCurrentMetrics() {
        this.metricsHistory.forEach((key, valueList) -> {
            if (valueList.isEmpty()) {
                LOGGER.debug(key + " => [No data]");
            } else {
                LOGGER.debug(key + " => " + valueList);
            }
        });
    }

    protected List<Double> calculateLoadAveragePerCore(List<Double> systemLoadAverage1m, List<Double> systemCpuCount) {
        // Ensure the lists are not empty to avoid IndexOutOfBoundsException
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
