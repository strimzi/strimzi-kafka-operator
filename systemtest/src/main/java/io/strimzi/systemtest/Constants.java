/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import java.time.Duration;

/**
 * Interface for keep global constants used across system tests.
 */
public interface Constants {
    long TIMEOUT_FOR_DEPLOYMENT_CONFIG_READINESS = Duration.ofMinutes(7).toMillis();
    long TIMEOUT_FOR_RESOURCE_CREATION = Duration.ofMinutes(5).toMillis();
    long TIMEOUT_FOR_RESOURCE_READINESS = Duration.ofMinutes(7).toMillis();
    long TIMEOUT_FOR_MIRROR_JOIN_TO_GROUP = Duration.ofMinutes(2).toMillis();
    long TIMEOUT_FOR_TOPIC_CREATION = Duration.ofMinutes(1).toMillis();
    long POLL_INTERVAL_FOR_RESOURCE_CREATION = Duration.ofSeconds(3).toMillis();
    long POLL_INTERVAL_FOR_RESOURCE_READINESS = Duration.ofSeconds(1).toMillis();
    long WAIT_FOR_ROLLING_UPDATE_INTERVAL = Duration.ofSeconds(5).toMillis();
    long WAIT_FOR_ROLLING_UPDATE_TIMEOUT = Duration.ofMinutes(7).toMillis();

    long TIMEOUT_FOR_SEND_RECEIVE_MSG = Duration.ofSeconds(30).toMillis();
    long TIMEOUT_AVAILABILITY_TEST = Duration.ofMinutes(1).toMillis();
    long TIMEOUT_SEND_MESSAGES = Duration.ofMinutes(1).toMillis();
    long TIMEOUT_RECV_MESSAGES = Duration.ofMinutes(1).toMillis();

    long TIMEOUT_FOR_CLUSTER_STABLE = Duration.ofMinutes(20).toMillis();
    long TIMEOUT_FOR_ZK_CLUSTER_STABILIZATION = Duration.ofMinutes(7).toMillis();

    long GET_BROKER_API_TIMEOUT = Duration.ofMinutes(1).toMillis();
    long GET_BROKER_API_INTERVAL = Duration.ofSeconds(5).toMillis();
    long TIMEOUT_FOR_GET_SECRETS = Duration.ofMinutes(1).toMillis();
    long TIMEOUT_TEARDOWN = Duration.ofSeconds(10).toMillis();
    long GLOBAL_TIMEOUT = Duration.ofMinutes(5).toMillis();
    long GLOBAL_POLL_INTERVAL = Duration.ofSeconds(1).toMillis();

    long CO_OPERATION_TIMEOUT = Duration.ofMinutes(1).toMillis();
    long CO_OPERATION_TIMEOUT_WAIT = CO_OPERATION_TIMEOUT + Duration.ofSeconds(20).toMillis();
    long CO_OPERATION_TIMEOUT_POLL = Duration.ofSeconds(2).toMillis();

    String KAFKA_CLIENTS = "kafka-clients";
    String STRIMZI_DEPLOYMENT_NAME = "strimzi-cluster-operator";
    String IMAGE_PULL_POLICY = "Always";

    int HTTP_BRIDGE_DEFAULT_PORT = 8080;

    /**
     * Default value which allows execution of tests with any tags
     */
    String DEFAULT_TAG = "all";

    /**
     * Tag for acceptance tests, which are triggered for each push/pr/merge on travis-ci
     */
    String ACCEPTANCE = "acceptance";
    /**
     * Tag for regression tests which are stable.
     */
    String REGRESSION = "regression";
    /**
     * Tag for tests, which results are not 100% reliable on all testing environments.
     */
    String FLAKY = "flaky";
    /**
     * Tag for tests, which are failing only on CCI VMs
     */
    String CCI_FLAKY = "cci_flaky";
    /**
     * Tag for strimzi bridge tests.
     */
    String BRIDGE = "bridge";
}
