/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaUser;

public enum ResourceType {

    KAFKA(Kafka.RESOURCE_KIND),
    CONNECT(KafkaConnect.RESOURCE_KIND),
    CONNECT_S2I(KafkaConnectS2I.RESOURCE_KIND),
    USER(KafkaUser.RESOURCE_KIND),
    MIRRORMAKER(KafkaMirrorMaker.RESOURCE_KIND),
    KAFKABRIDGE(KafkaBridge.RESOURCE_KIND);

    public final String name;

    ResourceType(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }

    public static ResourceType fromName(String name) {
        switch (name) {
            case Kafka.RESOURCE_KIND:
                return ResourceType.KAFKA;
            case KafkaConnect.RESOURCE_KIND:
                return ResourceType.CONNECT;
            case KafkaConnectS2I.RESOURCE_KIND:
                return ResourceType.CONNECT_S2I;
            case KafkaUser.RESOURCE_KIND:
                return ResourceType.USER;
            case KafkaMirrorMaker.RESOURCE_KIND:
                return ResourceType.MIRRORMAKER;
            case KafkaBridge.RESOURCE_KIND:
                return ResourceType.KAFKABRIDGE;
        }
        throw new IllegalArgumentException(name);
    }
}
