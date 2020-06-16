/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.cluster.model.KafkaVersion;
import java.util.Objects;

/**
 * Specialization of {@link StatefulSetOperator.RestartReason}
 */
public class BrokerConfigChange extends StatefulSetOperator.RestartReason {

    private final String kafkaConfig;
    private final KafkaVersion kafkaVersion;

    public BrokerConfigChange(String kafkaConfig, KafkaVersion kafkaVersion) {
        super("broker config changed");
        this.kafkaConfig = kafkaConfig;
        this.kafkaVersion = kafkaVersion;
    }

    public String kafkaConfig() {
        return kafkaConfig;
    }

    public KafkaVersion kafkaVersion() {
        return kafkaVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerConfigChange that = (BrokerConfigChange) o;
        return Objects.equals(kafkaConfig, that.kafkaConfig) && Objects.equals(kafkaVersion, that.kafkaVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kafkaConfig, kafkaVersion);
    }
}
