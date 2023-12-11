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
     * Properties prefix.
     */
    public static final String COMPONENT_TYPE = "cruise-control";
    
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
    public static final String API_TO_ADMIN_NAME = "toadmin";
    
    /**
     * Secret key for the admin password.
     */
    public static final String API_ADMIN_PASSWORD_KEY = COMPONENT_TYPE + ".apiAdminPassword";

    /**
     * Secret key for the user password.
     */
    public static final String API_USER_PASSWORD_KEY = COMPONENT_TYPE + ".apiUserPassword";

    /**
     * Secret key for the Cruise Control auth file.
     */
    public static final String API_AUTH_FILE_KEY = COMPONENT_TYPE + ".apiAuthFile";
    
    /**
     * Secret key for the username used by the Topic Operator.
     */
    public static final String API_TO_ADMIN_NAME_KEY = "topic-operator.apiAdminName";
    
    /**
     * Secret key for the password for Topic Operator.
     */
    public static final String API_TO_ADMIN_PASSWORD_KEY = "topic-operator.apiAdminPassword";
}
