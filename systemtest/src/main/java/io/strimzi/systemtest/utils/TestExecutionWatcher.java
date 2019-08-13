/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.LifecycleMethodExecutionExceptionHandler;

import java.io.File;

import static io.strimzi.systemtest.AbstractST.TEST_LOG_DIR;
import static io.strimzi.test.BaseITST.kubeClient;

public class TestExecutionWatcher implements AfterTestExecutionCallback, LifecycleMethodExecutionExceptionHandler {
    private static final Logger LOGGER = LogManager.getLogger(TestExecutionWatcher.class);

    @Override
    public void afterTestExecution(ExtensionContext extensionContext) {
        String testClass = extensionContext.getRequiredTestClass().getName();
        String testMethod = extensionContext.getRequiredTestMethod().getName();
        collectLogs(testClass, testMethod);
    }

    @Override
    public void handleBeforeAllMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        String testClass = extensionContext.getRequiredTestClass().getName();
        collectLogs(testClass, testClass);
        throw throwable;
    }

    @Override
    public void handleBeforeEachMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        String testClass = extensionContext.getRequiredTestClass().getName();
        String testMethod = extensionContext.getRequiredTestMethod().getName();
        collectLogs(testClass, testMethod);
        throw throwable;
    }

    void collectLogs(String testClass, String testMethod) {
        // Get current date to create a unique folder
        String currentDate = StUtils.getCurrentTime();
        String logDir = !testMethod.isEmpty() ?
                TEST_LOG_DIR + testClass + "." + testMethod + "_" + currentDate
                : TEST_LOG_DIR + currentDate;

        LogCollector logCollector = new LogCollector(kubeClient(), new File(logDir));
        logCollector.collectEvents();
        logCollector.collectConfigMaps();
        logCollector.collectLogsFromPods();
    }
}
