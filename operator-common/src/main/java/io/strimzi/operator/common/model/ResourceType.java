/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.api.kafka.model.KafkaConnectAssembly;
import io.strimzi.api.kafka.model.KafkaConnectS2IAssembly;
import io.strimzi.api.kafka.model.KafkaUser;

public enum ResourceType {

    KAFKA(KafkaAssembly.RESOURCE_KIND),
    CONNECT(KafkaConnectAssembly.RESOURCE_KIND),
    CONNECT_S2I(KafkaConnectS2IAssembly.RESOURCE_KIND),
    USER(KafkaUser.RESOURCE_KIND);

    public final String name;

    ResourceType(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }

    public static ResourceType fromName(String name) {
        switch (name) {
            case KafkaAssembly.RESOURCE_KIND:
                return ResourceType.KAFKA;
            case KafkaConnectAssembly.RESOURCE_KIND:
                return ResourceType.CONNECT;
            case KafkaConnectS2IAssembly.RESOURCE_KIND:
                return ResourceType.CONNECT_S2I;
            case KafkaUser.RESOURCE_KIND:
                return ResourceType.USER;
        }
        throw new IllegalArgumentException(name);
    }
}
