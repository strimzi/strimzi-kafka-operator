/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.config.model;

//@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum Type {
    BOOLEAN,
    STRING,
    PASSWORD,
    CLASS,
    SHORT,
    INT,
    LONG,
    DOUBLE,
    LIST;

}
