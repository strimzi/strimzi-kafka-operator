/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

/**
 * Represents the change from one Kafka version to another
 *
 * @param from                          Current Kafka version
 * @param to                            Desired Kafka version
 * @param interBrokerProtocolVersion    Inter-broker protocol version
 * @param logMessageFormatVersion       Log-message format version
 * @param metadataVersion               KRaft metadata version
 */
public record KafkaVersionChange(KafkaVersion from, KafkaVersion to, String interBrokerProtocolVersion, String logMessageFormatVersion, String metadataVersion) {
    @Override
    public String toString() {
        if (from.compareTo(to) < 0) {
            return "Kafka version upgrade from " + from + " to " + to;
        } else if (from.compareTo(to) > 0) {
            return "Kafka version downgrade from " + from + " to " + to;
        } else {
            return "Kafka version=" + from + " (no version change)";
        }
    }

}
