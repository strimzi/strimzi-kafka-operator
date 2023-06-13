/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.exceptions.KubernetesClusterUnstableException;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.LifecycleMethodExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.opentest4j.TestAbortedException;

import java.io.IOException;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class TestExecutionWatcher implements TestExecutionExceptionHandler, LifecycleMethodExecutionExceptionHandler {

    private static final Logger LOGGER = LogManager.getLogger(TestExecutionWatcher.class);

    @Override
    public void handleTestExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        LOGGER.error("{} - Exception {} has been thrown in @Test. Going to collect logs from components.", extensionContext.getRequiredTestClass().getSimpleName(), throwable.getMessage());
        if (!(throwable instanceof TestAbortedException || throwable instanceof KubernetesClusterUnstableException)) {
            final String testClass = extensionContext.getRequiredTestClass().getName();
            final String testMethod = extensionContext.getRequiredTestMethod().getName();
            collectLogs(extensionContext, new CollectorElement(testClass, testMethod));
        }
        throw throwable;
    }

    @Override
    public void handleBeforeAllMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        LOGGER.error("{} - Exception {} has been thrown in @BeforeAll. Going to collect logs from components.", extensionContext.getRequiredTestClass().getSimpleName(), throwable.getMessage());
        if (!(throwable instanceof TestAbortedException || throwable instanceof KubernetesClusterUnstableException)) {
            final String testClass = extensionContext.getRequiredTestClass().getName();
            collectLogs(extensionContext, new CollectorElement(testClass));
        }
        throw throwable;
    }

    @Override
    public void handleBeforeEachMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        LOGGER.error("{} - Exception {} has been thrown in @BeforeEach. Going to collect logs from components.", extensionContext.getRequiredTestClass().getSimpleName(), throwable.getMessage());
        if (!(throwable instanceof TestAbortedException || throwable instanceof KubernetesClusterUnstableException)) {
            final String testClass = extensionContext.getRequiredTestClass().getName();
            final String testMethod = extensionContext.getRequiredTestMethod().getName();
            collectLogs(extensionContext, new CollectorElement(testClass, testMethod));
        }
        throw throwable;
    }

    @Override
    public void handleAfterEachMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        LOGGER.error("{} - Exception {} has been thrown in @AfterEach. Going to collect logs from components.", extensionContext.getRequiredTestClass().getSimpleName(), throwable.getMessage());
        if (!(throwable instanceof KubernetesClusterUnstableException)) {
            final String testClass = extensionContext.getRequiredTestClass().getName();
            final String testMethod = extensionContext.getRequiredTestMethod().getName();

            collectLogs(extensionContext, new CollectorElement(testClass, testMethod));
        }
        throw throwable;
    }

    @Override
    public void handleAfterAllMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        LOGGER.error("{} - Exception {} has been thrown in @AfterAll. Going to collect logs from components.", extensionContext.getRequiredTestClass().getSimpleName(), throwable.getMessage());
        if (!(throwable instanceof KubernetesClusterUnstableException)) {
            final String testClass = extensionContext.getRequiredTestClass().getName();

            collectLogs(extensionContext, new CollectorElement(testClass));
        }
        throw throwable;
    }

    public synchronized static void collectLogs(ExtensionContext extensionContext, CollectorElement collectorElement) throws IOException {
        final LogCollector logCollector = new LogCollector(extensionContext, collectorElement, kubeClient(), Environment.TEST_LOG_DIR);
        // collecting logs for all resources inside Kubernetes cluster
        logCollector.collect();
    }
}
