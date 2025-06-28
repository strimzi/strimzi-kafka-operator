/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.parallel;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.systemtest.utils.StUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.concurrent.Semaphore;

/**
 * Class responsible for synchronization of Parallel and Isolated tests. When ThreadPoolJoin run several tests in parallel, we
 * increment counter @code{runningTestCasesInParallelCount}. If a counter is zero, we can proceed
 * with the IsolatedTest. If the counter is higher than zero, there is always a running ParallelTest.
 */
public class SuiteThreadController {

    private static final Logger LOGGER = LogManager.getLogger(SuiteThreadController.class);
    private static final String JUNIT_PARALLEL_COUNT_PROPERTY_NAME = "junit.jupiter.execution.parallel.config.fixed.parallelism";

    private static volatile SuiteThreadController instance;
    private static Integer maxTestsInParallel;
    private final Semaphore parallelSlots;

    public synchronized static SuiteThreadController getInstance() {
        if (instance == null) {
            instance = new SuiteThreadController();
        }
        return instance;
    }

    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    private SuiteThreadController() {
        if (System.getProperties().get(JUNIT_PARALLEL_COUNT_PROPERTY_NAME) != null) {
            maxTestsInParallel = Integer.parseInt((String) System.getProperties().get(JUNIT_PARALLEL_COUNT_PROPERTY_NAME));
            LOGGER.info("Going to execute {} tests in parallel", maxTestsInParallel);
        } else {
            LOGGER.warn("User did not specify junit.jupiter.execution.parallel.config.fixed.parallelism " +
                "in junit-platform.properties gonna use default as 1 (sequence mode)");
            maxTestsInParallel = 1;
        }

        parallelSlots = new Semaphore(maxTestsInParallel, /*fair*/ true);
    }

    /**
     * Add test ready to be executed and wait for available execution slot on Semaphore
     */
    public void addParallelTest(ExtensionContext ctx) {
        String testName = pretty(ctx);
        LOGGER.debug("[{}] waiting for parallel slot ({} free of {}) …",
            testName, parallelSlots.availablePermits(), maxTestsInParallel);

        try {
            parallelSlots.acquire();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for a parallel-test slot", ie);
        }

        LOGGER.debug("[{}] acquired slot ({} remaining)", testName, parallelSlots.availablePermits());
    }

    /**
     * Release acquired slot for execution.
     */
    public void removeParallelTest(ExtensionContext ctx) {
        parallelSlots.release();
        LOGGER.debug("[{}] released slot ({} free of {})",
            pretty(ctx), parallelSlots.availablePermits(), maxTestsInParallel);
    }

    private static String pretty(ExtensionContext ctx) {
        return StUtils.removePackageName(ctx.getRequiredTestClass().getName()) + "#" + ctx.getDisplayName();
    }
}