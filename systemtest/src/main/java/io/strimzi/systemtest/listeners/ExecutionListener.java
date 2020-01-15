/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.listeners;

import io.strimzi.systemtest.Environment;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

public class ExecutionListener implements TestExecutionListener {

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan) {
        TimeMeasuringSystem.getInstance().printAndSaveResults(Environment.TEST_LOG_DIR);
    }
}
