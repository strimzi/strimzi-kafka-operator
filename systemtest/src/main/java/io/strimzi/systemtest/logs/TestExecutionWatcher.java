/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

import io.skodjob.testframe.LogCollector;
import io.skodjob.testframe.LogCollectorBuilder;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.exceptions.KubernetesClusterUnstableException;
import io.strimzi.systemtest.resources.NamespaceManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.LifecycleMethodExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.opentest4j.TestAbortedException;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class TestExecutionWatcher implements TestExecutionExceptionHandler, LifecycleMethodExecutionExceptionHandler {

    private static final Logger LOGGER = LogManager.getLogger(TestExecutionWatcher.class);
    private static final LogCollector LOG_COLLECTOR = LogCollectorUtils.getDefaultLogCollector();
    private static final String CURRENT_DATE;

    static {
        // Get current date to create a unique folder
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
        dateTimeFormatter = dateTimeFormatter.withZone(ZoneId.of("GMT"));
        CURRENT_DATE = dateTimeFormatter.format(LocalDateTime.now());
    }

    @Override
    public void handleTestExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        LOGGER.error("{} - Exception {} has been thrown in @Test. Going to collect logs from components.", extensionContext.getRequiredTestClass().getSimpleName(), throwable.getMessage());
        if (!(throwable instanceof TestAbortedException || throwable instanceof KubernetesClusterUnstableException)) {
            final String testClass = extensionContext.getRequiredTestClass().getName();
            final String testMethod = extensionContext.getRequiredTestMethod().getName();
            collectLogs(String.join("/", testClass, testMethod), NamespaceManager.getInstance().getListOfNamespacesForTestClassAndTestCase(testClass, testMethod));
        }
        throw throwable;
    }

    @Override
    public void handleBeforeAllMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        LOGGER.error("{} - Exception {} has been thrown in @BeforeAll. Going to collect logs from components.", extensionContext.getRequiredTestClass().getSimpleName(), throwable.getMessage());
        if (!(throwable instanceof TestAbortedException || throwable instanceof KubernetesClusterUnstableException)) {
            final String testClass = extensionContext.getRequiredTestClass().getName();
            collectLogs(testClass, NamespaceManager.getInstance().getListOfNamespacesForTestClassAndTestCase(testClass, null));
        }
        throw throwable;
    }

    @Override
    public void handleBeforeEachMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        LOGGER.error("{} - Exception {} has been thrown in @BeforeEach. Going to collect logs from components.", extensionContext.getRequiredTestClass().getSimpleName(), throwable.getMessage());
        if (!(throwable instanceof TestAbortedException || throwable instanceof KubernetesClusterUnstableException)) {
            final String testClass = extensionContext.getRequiredTestClass().getName();
            final String testMethod = extensionContext.getRequiredTestMethod().getName();
            collectLogs(String.join("/", testClass, testMethod), NamespaceManager.getInstance().getListOfNamespacesForTestClassAndTestCase(testClass, testMethod));
        }
        throw throwable;
    }

    @Override
    public void handleAfterEachMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        LOGGER.error("{} - Exception {} has been thrown in @AfterEach. Going to collect logs from components.", extensionContext.getRequiredTestClass().getSimpleName(), throwable.getMessage());
        if (!(throwable instanceof KubernetesClusterUnstableException)) {
            final String testClass = extensionContext.getRequiredTestClass().getName();
            final String testMethod = extensionContext.getRequiredTestMethod().getName();

            collectLogs(String.join("/", testClass, testMethod), NamespaceManager.getInstance().getListOfNamespacesForTestClassAndTestCase(testClass, testMethod));
        }
        throw throwable;
    }

    @Override
    public void handleAfterAllMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        LOGGER.error("{} - Exception {} has been thrown in @AfterAll. Going to collect logs from components.", extensionContext.getRequiredTestClass().getSimpleName(), throwable.getMessage());
        if (!(throwable instanceof KubernetesClusterUnstableException)) {
            final String testClass = extensionContext.getRequiredTestClass().getName();

            collectLogs(testClass, NamespaceManager.getInstance().getListOfNamespacesForTestClassAndTestCase(testClass, null));
        }
        throw throwable;
    }

    public synchronized static void collectLogs(String pathForTestCase, List<String> namespaces) {
        String fullPathToTestCaseLogs = LogCollectorUtils.checkPathAndReturnFullRootPathWithIndexFolder(String.join("/", Environment.TEST_LOG_DIR, CURRENT_DATE, pathForTestCase));

        final LogCollector testCaseCollector = new LogCollectorBuilder(LOG_COLLECTOR)
            .withRootFolderPath(fullPathToTestCaseLogs)
            .build();

        testCaseCollector.collectFromNamespaces(namespaces.toArray(new String[0]));
    }
}
