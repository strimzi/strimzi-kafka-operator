/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube2.logging;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Configures logging in the Fabric8 Kubernetes Mock
 */
public class JulLoggingConfiguration {
    /**
     * The MockWebServer underlying fabric8's mockk8s uses JUL for logging, so this allows setting the logging level
     * as desired when running tests, also whether it logs to stdout, or stderr (which is the current behaviour)
     * @param level - logging level
     * @param logToStdOut - log to stdout instead of stderr
     */
    public static void configureJulLogging(Level level, boolean logToStdOut) {

        // As we can only modify existing keys, not add new ones, the referenced file contains the keys (and defaults) we need
        try (InputStream ins = JulLoggingConfiguration.class.getResourceAsStream("/jul-logging.properties")) {
            LogManager.getLogManager().updateConfiguration(ins, key -> {
                if ("okhttp3.mockwebserver.level".equals(key)) {
                    return (unused, unused2) -> level.getName();
                } else if ("okhttp3.mockwebserver.handlers".equals(key) && logToStdOut) {
                    return (unused, unused2) -> StdOutConsoleHandler.class.getName();
                } else {
                    return JulLoggingConfiguration::maintainSettings;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String maintainSettings(String previousValue, String nextValue) {
        if (nextValue == null) {
            return previousValue;
        }
        return nextValue;
    }
}
