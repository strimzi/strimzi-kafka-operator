/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model.cruisecontrol;

/**
 * Cruise Control API properties.
 */
public class CruiseControlApiProperties {
    /**
     * Admin role.
     */
    public static final String API_ADMIN_ROLE = "ADMIN";

    /**
     * User role.
     */
    public static final String API_USER_ROLE = "USER";

    /**
     * Username with the admin role.
     */
    public static final String API_ADMIN_NAME = "admin";
    
    /**
     * Username with the user role.
     */
    public static final String API_USER_NAME = "user";

    /**
     * Username with admin role.
     */
    public static final String API_TO_ADMIN_NAME = "topic-operator-admin";
    
    /**
     * The key, within the data section of a Kubernetes Secret, for the admin password used by the Rebalance Operator.
     */
    public static final String API_ADMIN_PASSWORD_KEY = "cruise-control.apiAdminPassword";

    /**
     * The key, within the data section of a Kubernetes Secret, for the user password used the Rebalance Operator.
     */
    public static final String API_USER_PASSWORD_KEY = "cruise-control.apiUserPassword";

    /**
     * The key, within the data section of a Kubernetes Secret, for the authentication file used by Cruise Control.
     */
    public static final String API_AUTH_FILE_KEY = "cruise-control.apiAuthFile";
    
    /**
     * The key, within the data section of a Kubernetes Secret, for the admin username used by the Topic Operator.
     */
    public static final String API_TO_ADMIN_NAME_KEY = "topic-operator.apiAdminName";
    
    /**
     * The key, within the data section of a Kubernetes Secret, for the admin password used by the Topic Operator.
     */
    public static final String API_TO_ADMIN_PASSWORD_KEY = "topic-operator.apiAdminPassword";
}
