/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.listeners;

import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

import static io.strimzi.systemtest.AbstractST.TEST_LOG_DIR;

public class ExecutionListener implements TestExecutionListener {
    private static final Logger LOGGER = LogManager.getLogger(ExecutionListener.class);

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan) {
        TimeMeasuringSystem.printAndSaveResults(TEST_LOG_DIR);
    }
}
