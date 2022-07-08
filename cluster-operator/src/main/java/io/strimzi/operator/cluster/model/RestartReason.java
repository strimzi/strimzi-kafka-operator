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
    CA_CERT_HAS_OLD_GENERATION("CA cert has old generation"),
    CA_CERT_REMOVED("CA certificate removed"),
    CA_CERT_RENEWED("CA certificate renewed"),
    CLIENT_CA_CERT_KEY_REPLACED("Trust new clients CA certificate signed by new key"),
    CLUSTER_CA_CERT_KEY_REPLACED("Trust new cluster CA certificate signed by new key"),
    CONFIG_CHANGE_REQUIRES_RESTART("Pod needs to be restarted, because reconfiguration cannot be done dynamically"),
    CUSTOM_LISTENER_CA_CERT_CHANGE("Custom certificate one or more listeners changed"),
    FILE_SYSTEM_RESIZE_NEEDED("File system needs to be resized"),
    JBOD_VOLUMES_CHANGED("JBOD volumes were added or removed"),
    MANUAL_ROLLING_UPDATE("Pod was manually annotated to be rolled"),
    POD_FORCE_RESTART_ON_ERROR("Pod needs to be forcibly restarted due to an error"),
    POD_HAS_OLD_GENERATION("Pod has old generation"),
    POD_HAS_OLD_REVISION("Pod has old revision"),
    POD_STUCK("Pod needs to be restarted, because it seems to be stuck and restart might help"),
    POD_UNRESPONSIVE("Pod needs to be restarted, because it does not seem to responding to connection attempts"),
    KAFKA_CERTIFICATES_CHANGED("Kafka broker TLS certificates updated");

    //Used in logging and Kubernetes event notes
    private final String defaultNote;

    // Matches first character or characters following an underscore
    private static final Pattern PASCAL_CASE_HELPER = Pattern.compile("^.|_.");

    RestartReason(String defaultNote) {
        this.defaultNote = defaultNote;
    }

    public String getDefaultNote() {
        return defaultNote;
    }

    // Go loves PascalCase so Kubernetes does too
    public String pascalCased() {
        Matcher matcher = PASCAL_CASE_HELPER.matcher(this.name().toLowerCase(Locale.ROOT));
        return matcher.replaceAll(result -> result.group().replace("_", "").toUpperCase(Locale.ROOT));
    }
}

