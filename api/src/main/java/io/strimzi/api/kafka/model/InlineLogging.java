/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Logging config is given inline with the resource
 */
public class InlineLogging extends Logging {

    /** A Map from logger name to logger level */
    private Map<String, String> loggers = new HashMap<>();

    @Override
    public String getType() {
        return "inline";
    }

    public Map<String, String> getLoggers() {
        return loggers;
    }

    public void setLoggers(Map<String, String> loggers) {
        this.loggers = loggers;
    }
}
