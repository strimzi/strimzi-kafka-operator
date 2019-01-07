/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A class for keep default constants for CO deployment.
 */
public final class ClusterOperatorConsts {
    private static final Logger LOGGER = LogManager.getLogger(ClusterOperatorConsts.class);

    public static final String DEPLOYMENT_NAME = "strimzi-cluster-operator";
    public static final String IMAGE_PULL_POLICY = "Always";
    public static final String REQUESTS_MEMORY = "512Mi";
    public static final String REQUESTS_CPU = "200m";
    public static final String LIMITS_MEMORY = "512Mi";
    public static final String LIMITS_CPU = "1000m";
    public static final String LOG_LEVEL = "INFO";
    public static final String FULL_RECONCILIATION_INTERVAL_MS = "120000";
    public static final String OPERATION_TIMEOUT_MS = "300000";
    public static final int DEFAULT_HEALTCHECK_PORT = 8080;
    public static final int INITIAL_DELAY_SECCONDS = 10;
    public static final int PERIOD_SECCONDS = 30;

    // Images
    public static final String STRIMZI_IMAGE = "strimzi/cluster-operator:latest";
    public static final String ZOOKEEPER_IMAGE = "strimzi/zookeeper:latest-kafka-2.0.0";
    public static final String TOPIC_OPERATOR_IMAGE = "strimzi/topic-operator:latest";
    public static final String USER_OPERATOR_IMAGE = "strimzi/user-operator:latest";
    public static final String KAFKA_INIT_IMAGE = "strimzi/kafka-init:latest";
    public static final String TLS_SIDECAR_ZOOKEEPER_IMAGE = "strimzi/zookeeper-stunnel:latest";
    public static final String TLS_SIDECAR_KAFKA_IMAGE = "strimzi/kafka-stunnel:latest\"";
    public static final String TLS_SIDECAR_ENTITY_OPERATOR_IMAGE = "strimzi/entity-operator-stunnel:latest";
    public static final String KAFKA_IMAGES = "2.0.0=strimzi/kafka:latest-kafka-2.0.0\n" +
            "2.0.1=strimzi/kafka:latest-kafka-2.0.1\n" +
            "2.1.0=strimzi/kafka:latest-kafka-2.1.0";
    public static final String KAFKA_CONNECT_IMAGES = "2.0.0=strimzi/kafka-connect:latest-kafka-2.0.0\n" +
            "2.0.1=strimzi/kafka-connect:latest-kafka-2.0.1\n" +
            "2.1.0=strimzi/kafka-connect:latest-kafka-2.1.0";
    public static final String KAFKA_CONNECT_S2I_IMAGES = "2.0.0=strimzi/kafka-connect-s2i:latest-kafka-2.0.0\n" +
            "2.0.1=strimzi/kafka-connect-s2i:latest-kafka-2.0.1\n" +
            "2.1.0=strimzi/kafka-connect-s2i:latest-kafka-2.1.0";
    public static final String KAFKA_MIRROR_MAKER_IMAGES = "2.0.0=strimzi/kafka-mirror-maker:latest-kafka-2.0.0\n" +
            "2.0.1=strimzi/kafka-mirror-maker:latest-kafka-2.0.1\n" +
            "2.1.0=strimzi/kafka-mirror-maker:latest-kafka-2.1.0";

    public static void printClusterOperatorInfo() {
        LOGGER.info("{}={}", "STRIMZI_IMAGE", STRIMZI_IMAGE);
        LOGGER.info("{}={}", "STRIMZI_DEFAULT_ZOOKEEPER_IMAGE", ZOOKEEPER_IMAGE);
        LOGGER.info("{}={}", "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGEE", TOPIC_OPERATOR_IMAGE);
        LOGGER.info("{}={}", "STRIMZI_DEFAULT_USER_OPERATOR_IMAGE", USER_OPERATOR_IMAGE);
        LOGGER.info("{}={}", "STRIMZI_DEFAULT_KAFKA_INIT_IMAGE", KAFKA_INIT_IMAGE);
        LOGGER.info("{}={}", "STRIMZI_DEFAULT_TLS_SIDECAR_ZOOKEEPER_IMAGE", TLS_SIDECAR_ZOOKEEPER_IMAGE);
        LOGGER.info("{}={}", "STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE", TLS_SIDECAR_KAFKA_IMAGE);
        LOGGER.info("{}={}", "STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE", TLS_SIDECAR_ENTITY_OPERATOR_IMAGE);
        LOGGER.info("{}={}", "STRIMZI_KAFKA_IMAGES", KAFKA_IMAGES);
        LOGGER.info("{}={}", "STRIMZI_KAFKA_CONNECT_IMAGES", KAFKA_CONNECT_IMAGES);
        LOGGER.info("{}={}", "STRIMZI_KAFKA_CONNECT_S2I_IMAGES", KAFKA_CONNECT_S2I_IMAGES);
        LOGGER.info("{}={}", "STRIMZI_KAFKA_MIRROR_MAKER_IMAGES", KAFKA_MIRROR_MAKER_IMAGES);
    }
}
