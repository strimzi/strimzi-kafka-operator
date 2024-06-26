/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model.cruisecontrol;

/**
 * Secret keys and usernames used by internal components like the:
 *   (a) Rebalance Operator
 *   (b) Topic Operator
 *   (c) Cruise Control healthcheck script
 * for accessing Cruise Control API.
 */
public class CruiseControlApiProperties {
    /**
     * Rebalance Operator username for Cruise Control API.
     */
    public static final String REBALANCE_OPERATOR_USERNAME = "rebalance-operator";
    
    /**
     * Healthcheck username for Cruise Control API.
     */
    public static final String HEALTHCHECK_USERNAME = "healthcheck";

    /**
     * Topic Operator username for Cruise Control API.
     */
    public static final String TOPIC_OPERATOR_USERNAME = "topic-operator";
    
    /**
     * The key, within the data section of a Kubernetes Secret, for the Rebalance Operator password.
     */
    public static final String REBALANCE_OPERATOR_PASSWORD_KEY = "rebalance-operator.password";

    /**
     * The key, within the data section of a Kubernetes Secret, for the healthcheck password.
     */
    public static final String HEALTHCHECK_PASSWORD_KEY = "healthcheck.password";

    /**
     * The key, within the data section of a Kubernetes Secret, for Cruise Control authentication file.
     */
    public static final String AUTH_FILE_KEY = "cruise-control.authFile";
    
    /**
     * The key, within the data section of a Kubernetes Secret, for the Topic Operator username.
     */
    public static final String TOPIC_OPERATOR_USERNAME_KEY = "topic-operator.apiAdminName";
    
    /**
     * The key, within the data section of a Kubernetes Secret, for the Topic Operator password.
     */
    public static final String TOPIC_OPERATOR_PASSWORD_KEY = "topic-operator.apiAdminPassword";
}
