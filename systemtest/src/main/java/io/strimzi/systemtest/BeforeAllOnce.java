/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;

/**
 * Custom Extension which executes code only once before all tests are started and after all tests finished.
 * This is temporary solution until https://github.com/junit-team/junit5/issues/456 will not be released
 */
public class BeforeAllOnce implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

    private static SetupClusterOperator clusterOperator;
    private static final Logger LOGGER = LogManager.getLogger(BeforeAllOnce.class);
    private static boolean systemReady = false;
    private static final String SYSTEM_RESOURCES = "SYSTEM_RESOURCES";
    private static ExtensionContext sharedExtensionContext;

    /**
     * Separate method with 'synchronized static' required for make sure procedure will be executed
     * only once across all simultaneously running threads
     */
    synchronized private static void systemSetup(ExtensionContext extensionContext) throws Exception {
        // 'if' is used to make sure procedure will be executed only once, not before every class
        if (!BeforeAllOnce.systemReady) {
            // get root extension context to be different from others context (BeforeAll)
            sharedExtensionContext = extensionContext.getRoot();
            BeforeAllOnce.systemReady = true;
            LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
            LOGGER.debug("{} - [BEFORE SUITE] - Going to setup testing system", extensionContext.getRequiredTestClass().getSimpleName());

            // When we set RBAC policy to NAMESPACE, we must copy all Roles to other (parallel) namespaces.
            if (Environment.isNamespaceRbacScope() && !Environment.isHelmInstall()) {
                // setup cluster operator before all suites only once
                clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
                    .withExtensionContext(sharedExtensionContext)
                    .createInstallation()
                    .runInstallation();
            } else {
                // setup cluster operator before all suites only once
                clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
                    .withExtensionContext(sharedExtensionContext)
                    .withNamespace(Constants.INFRA_NAMESPACE)
                    .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
                    .createInstallation()
                    .runInstallation();
            }
            // correction, because when @BeforeAllCallback is invoked firstly by @IsolatedSuite class it decrement counter which is not correct
//            if (StUtils.isParallelSuite(extensionContext)) {
//                SuiteThreadController.getInstance().decrementCounter();
//            }
            sharedExtensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(SYSTEM_RESOURCES, new BeforeAllOnce());
        }
    }

    /**
     * Initial setup of system. Including configuring services,
     * adding calls to callrec, users to scorecard, call media files
     *
     * @param extensionContext junit context
     */
    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        systemSetup(extensionContext);
    }

    /**
     * CloseableResource implementation, adding value into GLOBAL context is required to registers a callback hook
     * With such steps close() method will be executed only once in the end of test execution
     */
    @Override
    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    // Correctly updating a static field from a non-static method is tricky to get right and could easily lead to bugs
    // if there are multiple class instances and/or multiple threads in play.
    // In this case only `one` thread at a time will access the close() method because of `synchronized` monitor.
    public synchronized void close() throws Exception {
        // clean data from system
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("{} - [AFTER SUITE] has been called", this.getClass().getName());
        BeforeAllOnce.systemReady = false;
        clusterOperator.unInstall();
    }

    public static ExtensionContext getSharedExtensionContext() {
        return sharedExtensionContext;
    }

    public static SetupClusterOperator getClusterOperator() {
        return clusterOperator;
    }
}