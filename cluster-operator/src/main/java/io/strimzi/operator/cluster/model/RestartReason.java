/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Defines all the (existing) reasons a Kafka broker pod may need to be restarted
 */
public enum RestartReason {
    /**
     * CA has old generation
     */
    CA_CERT_HAS_OLD_GENERATION("CA cert has old generation"),

    /**
     * CA certificate has been removed
     */
    CA_CERT_REMOVED("CA certificate removed"),

    /**
     * CA has been renewed
     */
    CA_CERT_RENEWED("CA certificate renewed"),

    /**
     * Clients CA provate key was replaced
     */
    CLIENT_CA_CERT_KEY_REPLACED("Trust new clients CA certificate signed by new key"),

    /**
     * Cluster CA private key was replaced
     */
    CLUSTER_CA_CERT_KEY_REPLACED("Trust new cluster CA certificate signed by new key"),

    /**
     * Configuration change
     */
    CONFIG_CHANGE_REQUIRES_RESTART("Pod needs to be restarted, because reconfiguration cannot be done dynamically"),

    /**
     * Custom listener certificate changed
     */
    CUSTOM_LISTENER_CA_CERT_CHANGE("Custom certificate one or more listeners changed"),

    /**
     * File system resize is needed
     */
    FILE_SYSTEM_RESIZE_NEEDED("File system needs to be resized"),

    /**
     * JBOD volumes configuration changed
     */
    JBOD_VOLUMES_CHANGED("JBOD volumes were added or removed"),

    /**
     * Mnaual rolling update was requested
     */
    MANUAL_ROLLING_UPDATE("Pod was manually annotated to be rolled"),

    /**
     * Force restart is needed due to an error
     */
    POD_FORCE_RESTART_ON_ERROR("Pod needs to be forcibly restarted due to an error"),

    /**
     * Pod has an old genenration
     */
    POD_HAS_OLD_GENERATION("Pod has old generation"),

    /**
     * Pod has an old revision
     */
    POD_HAS_OLD_REVISION("Pod has old revision"),

    /**
     * Pod is stuck
     */
    POD_STUCK("Pod needs to be restarted, because it seems to be stuck and restart might help"),

    /**
     * Pod is unresponsive
     */
    POD_UNRESPONSIVE("Pod needs to be restarted, because it does not seem to responding to connection attempts"),

    /**
     * Kafka TLS certificates changed
     */
    KAFKA_CERTIFICATES_CHANGED("Kafka broker TLS certificates updated");

    //Used in logging and Kubernetes event notes
    private final String defaultNote;

    // Matches first character or characters following an underscore
    private static final Pattern PASCAL_CASE_HELPER = Pattern.compile("^.|_.");

    /**
     * Constructor
     *
     * @param defaultNote   The default note
     */
    RestartReason(String defaultNote) {
        this.defaultNote = defaultNote;
    }

    /**
     * @return  The default note
     */
    public String getDefaultNote() {
        return defaultNote;
    }

    /**
     * @return  The pascal-cased variant of the reason (this is used by Kubernetes and Golang)
     */
    public String pascalCased() {
        Matcher matcher = PASCAL_CASE_HELPER.matcher(this.name().toLowerCase(Locale.ROOT));
        return matcher.replaceAll(result -> result.group().replace("_", "").toUpperCase(Locale.ROOT));
    }
}

