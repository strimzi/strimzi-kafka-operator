/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.common.model;

/**
 * Defines all the (existing) reasons a pod may need to be restarted
 */
public enum RestartReason {
    ADMIN_CLIENT_CANNOT_CONNECT_TO_BROKER("Unable to connect to broker with admin client"),
    ENTITY_OPERATOR_CERTS_CHANGED("Existing Entity Operator certs changed"),
    CRUISE_CONTROL_CERTS_CHANGED("Existing Cruise Control certs changed"),
    KAFKA_EXPORTER_CERTS_CHANGED("Existing Kafka Exporter certs changed"),
    CA_CERT_HAS_OLD_GENERATION("CA cert has old generation"),
    CLUSTER_CA_CERT_KEY_REPLACED("Trust new cluster CA certificate signed by new key"),
    CLIENT_CA_CERT_KEY_REPLACED("Trust new clients CA certificate signed by new key"),
    CA_CERT_RENEWED("CA certificate renewed"),
    CA_CERT_REMOVED("CA certificate removed"),
    CUSTOM_LISTENER_CA_CERT_CHANGE("Custom certificate one or more listeners changed"),
    FILE_SYSTEM_RESIZE_NEEDED("File system needs to be resized"),
    JBOD_VOLUMES_CHANGED("JBOD volumes were added or removed"),
    MANUAL_ROLLING_UPDATE("Pod was manually annotated to be rolled"),
    POD_FORCE_ROLL_ON_ERROR("Pod rolled due to error"),
    POD_HAS_OLD_GENERATION("Pod has old generation"),
    ROLL_BEFORE_UPGRADE("Rolling pod prior to upgrade"),
    SERVER_CERT_CHANGE("Server certificates changed");

    //Used in logging and Kubernetes event notes
    private final String defaultNote;

    RestartReason(String defaultNote) {
        this.defaultNote = defaultNote;
    }


    public String getDefaultNote() {
        return defaultNote;
    }
}

