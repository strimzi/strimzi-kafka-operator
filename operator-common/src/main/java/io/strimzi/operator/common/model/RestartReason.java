/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.common.model;

/**
 * Defines all the (existing) reasons a pod may need to be restarted
 */
public enum RestartReason {
    ADMIN_CLIENT_CANNOT_CONNECT_TO_BROKER,
    CA_CERT_CHANGED,
    CA_CERT_HAS_OLD_GENERATION,
    CA_CERT_KEY_REPLACED,
    CA_CERT_RENEWED,
    CA_CERT_REMOVED,
    CUSTOM_LISTENER_CA_CERT_CHANGE,
    FILE_SYSTEM_RESIZE_NEEDED,
    JBOD_VOLUMES_CHANGED,
    MANUAL_ROLLING_UPDATE,
    POD_FORCE_ROLL_ON_ERROR,
    POD_HAS_OLD_GENERATION,
    ROLL_BEFORE_UPGRADE,
    SERVER_CERT_CHANGE;
}

