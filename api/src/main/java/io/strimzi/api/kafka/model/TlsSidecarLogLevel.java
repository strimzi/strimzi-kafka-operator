/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum TlsSidecarLogLevel {

    EMERG,
    ALERT,
    CRIT,
    ERR,
    WARNING,
    NOTICE,
    INFO,
    DEBUG;

    @JsonCreator
    public static TlsSidecarLogLevel forValue(String value) {
        switch (value) {
            case "emerg":
                return EMERG;
            case "alert":
                return ALERT;
            case "crit":
                return CRIT;
            case "err":
                return ERR;
            case "warning":
                return WARNING;
            case "notice":
                return  NOTICE;
            case "info":
                return INFO;
            case "debug":
                return DEBUG;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case EMERG:
                return "emerg";
            case ALERT:
                return "alert";
            case CRIT:
                return "crit";
            case ERR:
                return "err";
            case WARNING:
                return "warning";
            case NOTICE:
                return "notice";
            case INFO:
                return "info";
            case DEBUG:
                return "debug";
            default:
                return null;
        }
    }
}
