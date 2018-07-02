/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import java.util.Properties;

public class InlineLogging extends Logging {

    @Override
    public String getType() {
        return "inline";
    }

    public Properties getLoggers() {
        return loggers;
    }

    public void setLoggers(Properties loggers) {
        this.loggers = loggers;
    }

    /** A Map from logger name to logger level */
    public Properties loggers = new Properties();
}
