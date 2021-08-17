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

public class ParallelSuiteController {

    private static final long STARTING_DELAY = Duration.ofSeconds(10).toMillis();

    private static final Logger LOGGER = LogManager.getLogger(ParallelSuiteController.class);
    private static int counter;

    public synchronized static void addParallelSuite(ExtensionContext extensionContext) {
        if (extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.PARALLEL_CLASS_COUNT) == null) {
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PARALLEL_CLASS_COUNT, ++counter);
        } else {
            LOGGER.info("Adding parallel suite {}", extensionContext.getDisplayName());
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PARALLEL_CLASS_COUNT, ++counter);
        }
        LOGGER.debug("Current parallel suites:{}", counter);
    }


    public synchronized static void removeParallelSuite(ExtensionContext extensionContext) {
        if (extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.PARALLEL_CLASS_COUNT) == null) {
            throw new RuntimeException("There is no parallel suite running.");
        } else {
            LOGGER.info("Removing parallel suite {}", extensionContext.getDisplayName());
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PARALLEL_CLASS_COUNT, --counter);
        }

        LOGGER.debug("Current parallel suites:{}", counter);
    }

    public static boolean waitUntilZeroParallelSuites() {
        try {
            Thread.sleep(STARTING_DELAY);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return counter > 0;
    }

    public static int getCounter() {
        return counter;
    }

    public static void decrementCounter() {
        counter--;
    }
}
