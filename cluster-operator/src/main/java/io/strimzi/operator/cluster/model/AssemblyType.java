/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.api.kafka.model.KafkaConnectAssembly;
import io.strimzi.api.kafka.model.KafkaConnectS2IAssembly;

public enum AssemblyType {

    KAFKA(KafkaAssembly.RESOURCE_KIND),
    CONNECT(KafkaConnectAssembly.RESOURCE_KIND),
    CONNECT_S2I(KafkaConnectS2IAssembly.RESOURCE_KIND);

    public final String name;

    AssemblyType(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }

    public static AssemblyType fromName(String name) {
        switch (name) {
            case KafkaAssembly.RESOURCE_KIND:
                return AssemblyType.KAFKA;
            case KafkaConnectAssembly.RESOURCE_KIND:
                return AssemblyType.CONNECT;
            case KafkaConnectS2IAssembly.RESOURCE_KIND:
                return AssemblyType.CONNECT_S2I;
        }
        throw new IllegalArgumentException(name);
    }
}
