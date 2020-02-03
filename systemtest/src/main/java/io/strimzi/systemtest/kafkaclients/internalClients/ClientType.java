/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

public enum ClientType {
    CLI_KAFKA_VERIFIABLE_PRODUCER,
    CLI_KAFKA_VERIFIABLE_CONSUMER;

    /**
     * Get bind kafka client type to kafka client executable
     *
     * @param client kafka client type
     * @return webClient executable
     */
    public static String getCommand(ClientType client) {
        switch (client) {
            case CLI_KAFKA_VERIFIABLE_PRODUCER:
                return "/opt/kafka/producer.sh";
            case CLI_KAFKA_VERIFIABLE_CONSUMER:
                return "/opt/kafka/consumer.sh";
            default:
                throw new IllegalArgumentException("Unexpected client type!");
        }
    }
}
