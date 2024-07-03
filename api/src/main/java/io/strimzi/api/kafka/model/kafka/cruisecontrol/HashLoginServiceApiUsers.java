/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.cruisecontrol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Cruise Control's API users config
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "valueFrom"})
@EqualsAndHashCode
public class HashLoginServiceApiUsers extends ApiUsers {
    public static final String TYPE_HASH_LOGIN_SERVICE = "hashLoginService";

    // Regex to match an entry in Jetty's HashLoginService's file format: username: password, rolename
    public static final Pattern HASH_LOGIN_SERVICE_PATTERN = Pattern.compile("^[\\w-]+\\s*:\\s*\\w+\\s*,\\s*\\w+\\s*$");

    /**
     * Parse String containing API credential config into map of API user entries.
     *
     * @param config API credential config file as a String.
     *
     * @return map of API credential entries containing username, password, and role.
     */
    public Map<String, UserEntry> parseEntriesFromString(String config) {
        Map<String, UserEntry> entries = new HashMap<>();

        if (config.isEmpty()) {
            return entries;
        }

        for (String line : config.split("\n")) {
            Matcher matcher = HASH_LOGIN_SERVICE_PATTERN.matcher(line);
            if (matcher.matches()) {
                String[] parts = line.replaceAll("\\s", "").split(":");
                String username = parts[0];
                String password = parts[1].split(",")[0];
                Role role = Role.fromString(parts[1].split(",")[1]);
                if (entries.containsKey(username)) {
                    throw new IllegalArgumentException("Duplicate username found: " + "\"" + username + "\". "
                            + "Cruise Control API credentials config must contain unique usernames");
                }
                entries.put(username, new UserEntry(username, password, role));
            } else {
                throw new IllegalArgumentException("Invalid configuration provided: " +  "\"" + line + "\". " +
                        "Cruise Control API credentials config must follow " +
                        "HashLoginService's file format `username: password [,rolename ...]`");
            }
        }
        return entries;
    }

    /**
     * Returns Cruise Control API auth credentials file as a String.
     *
     * @param entries Map of API user entries containing API user credentials.
     *
     * @return Returns Cruise Control API auth credentials file as a String.
     */
    public String generateApiAuthFileAsString(Map<String, UserEntry> entries) {
        StringBuilder sb = new StringBuilder();
        // Follows  Jetty's HashLoginService's file format: username: password [,rolename ...]
        for (UserEntry e : entries.values()) {
            sb.append(e.getUsername()).append(": ").append(e.getPassword()).append(", ").append(e.getRole()).append("\n");
        }
        return sb.toString();
    }
}
