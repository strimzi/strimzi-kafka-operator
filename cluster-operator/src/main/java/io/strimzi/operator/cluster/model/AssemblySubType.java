/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

public enum AssemblySubType {

    KAFKA("kafka"),
    KAFKA_HEADLESS("kafka-headless"),
    ZOOKEEPER("zookeeper"),
    ZOOKEEPER_HEADLESS("zookeeper-headless"),
    KAFKA_CONNECT("kafka-connect"),
    KAFKA_CONNECT_HEADLESS("kafka-connect-headless"),
    KAFKA_CONNECT_S2I("kafka-connect-s2i"),
    KAFKA_CONNECT_S2I_HEADLESS("kafka-connect-s2i-headless");

    public final String name;

    AssemblySubType(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }

    public static AssemblySubType fromName(String name) {
        switch (name) {
            case "kafka":
                return AssemblySubType.KAFKA;
            case "kafka-headless":
                return AssemblySubType.KAFKA_HEADLESS;
            case "zookeeper":
                return AssemblySubType.ZOOKEEPER;
            case "zookeeper-headless":
                return AssemblySubType.ZOOKEEPER_HEADLESS;
            case "kafka-connect":
                return AssemblySubType.KAFKA_CONNECT;
            case "kafka-connect-headless":
                return AssemblySubType.KAFKA_CONNECT_HEADLESS;
            case "kafka-connect-s2i":
                return AssemblySubType.KAFKA_CONNECT_S2I;
            case "kafka-connect-s2i-headless":
                return AssemblySubType.KAFKA_CONNECT_S2I_HEADLESS;
        }
        throw new IllegalArgumentException(name);
    }
}
