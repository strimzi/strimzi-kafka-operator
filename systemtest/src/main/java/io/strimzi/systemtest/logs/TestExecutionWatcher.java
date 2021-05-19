/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.LifecycleMethodExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.opentest4j.TestAbortedException;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class TestExecutionWatcher implements TestExecutionExceptionHandler, LifecycleMethodExecutionExceptionHandler {
    private static final Logger LOGGER = LogManager.getLogger(TestExecutionWatcher.class);

    @Override
    public void handleTestExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        if (!(throwable instanceof TestAbortedException)) {
            final String testClass = extensionContext.getRequiredTestClass().getName();
            final String testMethod = extensionContext.getRequiredTestMethod().getName();
            collectLogs(extensionContext, testClass, testMethod);
        }
        throw throwable;
    }

    @Override
    public void handleBeforeAllMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        if (!(throwable instanceof TestAbortedException)) {
            String testClass = extensionContext.getRequiredTestClass().getName();
            collectLogs(extensionContext, testClass, testClass);
        }
        throw throwable;
    }

    @Override
    public void handleBeforeEachMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        if (!(throwable instanceof TestAbortedException)) {
            final String testClass = extensionContext.getRequiredTestClass().getName();
            final String testMethod = extensionContext.getRequiredTestMethod().getName();
            collectLogs(extensionContext, testClass, testMethod);
        }
        throw throwable;
    }

    @Override
    public void handleAfterEachMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        final String testClass = extensionContext.getRequiredTestClass().getName();
        final String testMethod = extensionContext.getRequiredTestMethod().getName();
        collectLogs(extensionContext, testClass, testMethod);
        throw throwable;
    }

    @Override
    public void handleAfterAllMethodExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        final String testClass = extensionContext.getRequiredTestClass().getName();
        collectLogs(extensionContext, testClass, "");
        throw throwable;
    }

    public synchronized static void collectLogs(ExtensionContext extensionContext, String testClass, String testMethod) {
        final String namespaceName;
        final LogCollector logCollector;

        // Stop test execution time counter in case of failures
        TimeMeasuringSystem.getInstance().stopOperation(Operation.TEST_EXECUTION, testClass, testMethod);
        // Get current date to create a unique folder
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        final String currentDate = simpleDateFormat.format(Calendar.getInstance().getTime());

        String logDir = !testMethod.isEmpty() ?
            Environment.TEST_LOG_DIR + testClass + "." + testMethod + "_" + currentDate
            : Environment.TEST_LOG_DIR + testClass + currentDate;

        if (StUtils.isParallelNamespaceTest(extensionContext)) {
            namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
            logDir = !testMethod.isEmpty() ?
                Environment.TEST_LOG_DIR + testClass + "." + testMethod + "_" + namespaceName + "_" + currentDate
                : Environment.TEST_LOG_DIR + testClass + currentDate;
            logCollector = new LogCollector(namespaceName, kubeClient(), new File(logDir));
        } else {
            logCollector = new LogCollector(kubeClient().getNamespace(), kubeClient(), new File(logDir));
        }
        // collecting logs for all resources inside Kubernetes cluster
        logCollector.collectEvents();
        logCollector.collectConfigMaps();
        logCollector.collectLogsFromPods();
        logCollector.collectDeployments();
        logCollector.collectStatefulSets();
        logCollector.collectReplicaSets();
        logCollector.collectStrimzi();
        logCollector.collectClusterInfo();
    }
}
