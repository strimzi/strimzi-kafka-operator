/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.config.model;

/**
 * Type of the configuration option
 */
public enum Type {
    /**
     * Boolean option
     */
    BOOLEAN,

    /**
     * String option
     */
    STRING,

    /**
     * Password option
     */
    PASSWORD,

    /**
     * Class option
     */
    CLASS,

    /**
     * Short option
     */
    SHORT,

    /**
     * Int option
     */
    INT,

    /**
     * Long option
     */
    LONG,

    /**
     * Double option
     */
    DOUBLE,

    /**
     * List option
     */
    LIST;

}
