/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

public enum AssemblyType {

    KAFKA("kafka"),
    CONNECT("kafka-connect"),
    CONNECT_S2I("kafka-connect-s2i");

    public final String name;

    AssemblyType(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }

    public static AssemblyType fromName(String name) {
        switch (name) {
            case "kafka":
                return AssemblyType.KAFKA;
            case "kafka-connect":
                return AssemblyType.CONNECT;
            case "kafka-connect-s2i":
                return AssemblyType.CONNECT_S2I;
        }
        throw new IllegalArgumentException(name);
    }
}
