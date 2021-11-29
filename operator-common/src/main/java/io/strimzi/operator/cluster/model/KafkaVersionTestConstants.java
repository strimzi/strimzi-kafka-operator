/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

public class KafkaVersionTestConstants {
    public static final String KAFKA_IMAGE_STR = "strimzi/kafka:latest-kafka-";
    public static final String KAFKA_CONNECT_IMAGE_STR = "strimzi/kafka-connect:latest-kafka-";
    public static final String KAFKA_MIRROR_MAKER_IMAGE_STR = "strimzi/kafka-mirror-maker:latest-kafka-";
    public static final String KAFKA_MIRROR_MAKER_2_IMAGE_STR = "strimzi/kafka-connect:latest-kafka-";


    public static final String LATEST_KAFKA_VERSION = "3.0.0";
    public static final String LATEST_FORMAT_VERSION = "3.0";
    public static final String LATEST_PROTOCOL_VERSION = "3.0";
    public static final String LATEST_ZOOKEEPER_VERSION = "3.6.3";
    public static final String LATEST_CHECKSUM = "ABCD1234";
    public static final String LATEST_THIRD_PARTY_VERSION = "3.0.x";
    public static final String LATEST_KAFKA_IMAGE = KAFKA_IMAGE_STR + LATEST_KAFKA_VERSION;
    public static final String LATEST_KAFKA_CONNECT_IMAGE = KAFKA_CONNECT_IMAGE_STR + LATEST_KAFKA_VERSION;
    public static final String LATEST_KAFKA_MIRROR_MAKER_IMAGE = KAFKA_MIRROR_MAKER_IMAGE_STR + LATEST_KAFKA_VERSION;
    public static final String LATEST_KAFKA_MIRROR_MAKER_2_IMAGE = KAFKA_MIRROR_MAKER_2_IMAGE_STR + LATEST_KAFKA_VERSION;

    public static final String PREVIOUS_KAFKA_VERSION = "2.8.0";
    public static final String PREVIOUS_FORMAT_VERSION = "2.8";
    public static final String PREVIOUS_PROTOCOL_VERSION = "2.8";
    public static final String PREVIOUS_ZOOKEEPER_VERSION = "3.5.8";
    public static final String PREVIOUS_CHECKSUM = "ABCD1234";
    public static final String PREVIOUS_THIRD_PARTY_VERSION = "2.8.x";
    public static final String PREVIOUS_KAFKA_IMAGE = KAFKA_IMAGE_STR + PREVIOUS_KAFKA_VERSION;
    public static final String PREVIOUS_KAFKA_CONNECT_IMAGE = KAFKA_CONNECT_IMAGE_STR + PREVIOUS_KAFKA_VERSION;
    public static final String PREVIOUS_KAFKA_MIRROR_MAKER_IMAGE = KAFKA_MIRROR_MAKER_IMAGE_STR + PREVIOUS_KAFKA_VERSION;
    public static final String PREVIOUS_KAFKA_MIRROR_MAKER_2_IMAGE = KAFKA_MIRROR_MAKER_2_IMAGE_STR + PREVIOUS_KAFKA_VERSION;

    public static final String DEFAULT_KAFKA_VERSION = LATEST_KAFKA_VERSION;
    public static final String DEFAULT_KAFKA_IMAGE = LATEST_KAFKA_IMAGE;
    public static final String DEFAULT_KAFKA_CONNECT_IMAGE = LATEST_KAFKA_CONNECT_IMAGE;
    public static final String DEFAULT_KAFKA_MIRROR_MAKER_IMAGE = LATEST_KAFKA_MIRROR_MAKER_IMAGE;

}
