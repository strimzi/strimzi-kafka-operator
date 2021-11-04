/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.parallel;

import io.strimzi.systemtest.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Class responsible for synchronization of ParallelSuites. When ThreadPoolJoin run several tests in parallel, we
 * increment counter (runningTestSuitesInParallelCount) using shared extension context. If a counter is zero, we can proceed
 * with the IsolationSuite. If the counter is higher than zero, there is always a running ParallelSuite.
 */
public class ParallelSuiteController {

    private static final long STARTING_DELAY = Duration.ofSeconds(5).toMillis();

    private static final Logger LOGGER = LogManager.getLogger(ParallelSuiteController.class);
    private static int runningTestSuitesInParallelCount;

    public synchronized static void addParallelSuite(ExtensionContext extensionContext) {
        LOGGER.debug("Adding parallel suite: {}", extensionContext.getDisplayName());
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PARALLEL_CLASS_COUNT, ++runningTestSuitesInParallelCount);

        LOGGER.debug("Parallel suites count: {}", runningTestSuitesInParallelCount);
    }


    public synchronized static void removeParallelSuite(ExtensionContext extensionContext) {
        if (extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.PARALLEL_CLASS_COUNT) == null) {
            throw new RuntimeException("There is no parallel suite running.");
        } else {
            LOGGER.debug("Removing parallel suite: {}", extensionContext.getDisplayName());
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PARALLEL_CLASS_COUNT, --runningTestSuitesInParallelCount);
        }

        LOGGER.debug("Parallel suites count: {}", runningTestSuitesInParallelCount);
    }

    public static boolean waitUntilZeroParallelSuites() {
        // until more that 0 parallel suites running in parallel 'active waiting'
        boolean preCondition = true;

        while (preCondition) {
            LOGGER.debug("Parallel suites count: {}", runningTestSuitesInParallelCount);
            try {
                Thread.sleep(STARTING_DELAY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            preCondition = runningTestSuitesInParallelCount > 0;
        }
        return false;
    }

    public static int getRunningTestSuitesInParallelCount() {
        return runningTestSuitesInParallelCount;
    }

    public static void decrementCounter() {
        runningTestSuitesInParallelCount--;
    }
}
