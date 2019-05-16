/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import static io.strimzi.systemtest.AbstractST.TEST_LOG_DIR;
import static io.strimzi.test.BaseITST.kubeClient;

public class TestExecutionWatcher implements AfterTestExecutionCallback {
    private static final Logger LOGGER = LogManager.getLogger(TestExecutionWatcher.class);

    @Override
    public void afterTestExecution(ExtensionContext extensionContext) {
        Method testMethod = extensionContext.getRequiredTestMethod();
        Class testClass = extensionContext.getRequiredTestClass();
        collectLogs(testMethod.getName(), testClass.getName());
    }

    void collectLogs(String testMethod, String testClass) {
        // Get current date to create a unique folder
        String currentDate = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
        String logDir = !testMethod.isEmpty() ?
                TEST_LOG_DIR + testClass + "." + testMethod + "_" + currentDate
                : TEST_LOG_DIR + currentDate;

        LogCollector logCollector = new LogCollector(kubeClient(), new File(logDir));
        logCollector.collectEvents();
        logCollector.collectConfigMaps();
        logCollector.collectLogsFromPods();
    }
}
