/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

/**
 * Enum with argument for external clients
 */
public enum ClientArgument {
    // Common
    TOPIC("--topic"),
    PROPERTY("--property"),
    VERBOSE("--verbose"),

    // Consumer
    BOOTSTRAP_SERVER("--bootstrap-server"),
    CONSUMER_PROPERTY("--consumer-property"),
    CONSUMER_CONFIG("--consumer.config"),
    ENABLE_SYSTEST_EVENTS("--enable-systest-events"),
    FORMATTER("--formatter"),
    FROM_BEGINNING("--from-beginning"),
    GROUP("--group"),
    ISOLATION_LEVEL("--isolation-level"),
    KEY_DESERIALIZER("--key-deserializer"),
    MAX_MESSAGES("--max-messages"),
    OFFSET("--offset"),
    PARTITION("--partition"),
    SKIP_MESSAGE_ON_ERROR("--skip-message-on-error"),
    TIMEOUT_MS("--timeout-ms"),
    VALUE_DESERIALZIER("--value-deserializer"),
    WHITELIST("--whitelist"),
    GROUP_ID("--group-id"),
    SESSION_TIMEOUT("--session-timeout"),
    ENABLE_AUTOCOMMIT("--enable-autocommit"),
    RESET_POLICY("--reset-policy"),
    ASSIGMENT_STRATEGY("--assignment-strategy"),

    // Producer
    BATCH_SIZE("--batch-size"),
    BROKER_LIST("--broker-list"),
    COMPRESSION_CODEC("--compression-codec"),
    LINE_READER("--line-reader"),
    MAX_BLOCK_MS("--max-block-ms"),
    MAX_MEMORY_BYTES("--max-memory-bytes"),
    MAX_PARTITION_MEMORY_BYTES("--max-partition-memory-bytes"),
    MESSAGE_SEND_MAX_RETRIES("--message-send-max-retries"),
    METADATA_EXPIRY_MS("--metadata-expiry-ms"),
    PRODUCER_PROPERTY("--producer-property"),
    PRODUCER_CONFIG("--producer.config"),
    REQUEST_REQUIRED_ACKS("--request-required-acks"),
    REQUEST_TIMEOUT_MS("--request-timeout-ms"),
    RETRY_BACKOFF_MS("--retry-backoff-ms"),
    SOCKET_BUFFER_SIZE("--socket-buffer-size"),
    SYCN("--sync"),
    ACKS("--acks"),
    TIMEOUT("--timeout"),
    THROUGHPUT("--throughput"),
    MESSAGE_CREATE_TIME("--message-create-time"),
    VALUE_PREFIX("--value-prefix"),
    REPEATING_KEYS("--repeating-keys");

    private String command;

    ClientArgument(String command) {
        this.command = command;
    }

    /**
     * Gets command for external webClient
     *
     * @return string command
     */
    public String command() {
        return command;
    }
}
