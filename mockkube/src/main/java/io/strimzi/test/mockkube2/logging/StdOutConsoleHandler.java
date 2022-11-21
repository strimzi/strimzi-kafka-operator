/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube2.logging;

import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

/**
 * The java.util.logging.ConsoleHandler writes to stderr, this is just a version to write to stdout when mock web server
 * is logging
 */
public class StdOutConsoleHandler extends StreamHandler {
    /**
     * Constructs the StdOutConsoleHandler
     */
    public StdOutConsoleHandler() {
        super(System.out, new SimpleFormatter());
    }

    @Override
    public synchronized void publish(LogRecord record) {
        super.publish(record);
        flush();
    }

    @Override
    public synchronized void close()  {
        flush();
    }
}
