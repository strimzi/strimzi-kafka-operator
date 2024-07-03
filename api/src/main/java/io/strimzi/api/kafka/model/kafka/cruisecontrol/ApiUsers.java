/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.cruisecontrol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.PasswordSource;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.api.kafka.model.kafka.cruisecontrol.HashLoginServiceApiUsers.TYPE_HASH_LOGIN_SERVICE;

/**
 * Abstract baseclass for different representations of Cruise Control's API users config, discriminated by {@link #getType() type}.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "valueFrom"})
@EqualsAndHashCode
public abstract class ApiUsers implements UnknownPropertyPreserving {
    private String type;
    private PasswordSource valueFrom;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Type of the Cruise Control API users configuration. "
            + "Supported format is: " + "`" + TYPE_HASH_LOGIN_SERVICE + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(required = true)
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Description("Secret from which the custom Cruise Control API authentication credentials are read. ")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(required = true)
    public PasswordSource getValueFrom() {
        return valueFrom;
    }

    public void setValueFrom(PasswordSource valueFrom) {
        this.valueFrom = valueFrom;
    }

    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    /**
     * Factory method to create and return an API Users object of the given type
     *
     * @return API Users object of given type
     */
    public static ApiUsers create(CruiseControlSpec ccSpec) {
        if (ccSpec.getApiUsers() == null || ccSpec.getApiUsers().getType() == null) {
            // default configuration used for Strimzi-managed API users
            return new HashLoginServiceApiUsers();
        }

        String type = ccSpec.getApiUsers().getType();
        switch (type) {
            case TYPE_HASH_LOGIN_SERVICE:
                return new HashLoginServiceApiUsers();
            default:
                throw new IllegalArgumentException("Unknown ApiUsers type: " + type);
        }
    }

    /**
     * Parse String containing API credential config into map of API user entries.
     *
     * @param config API credential config file as a String.
     *
     * @return map of API credential entries containing username, password, and role.
     */
    public abstract Map<String, UserEntry> parseEntriesFromString(String config);

    /**
     * Returns Cruise Control API auth credentials file as a String.
     *
     * @param entries Map of API user entries containing API user credentials.
     *
     * @return Returns Cruise Control API auth credentials file as a String.
     */
    public abstract String generateApiAuthFileAsString(Map<String, UserEntry> entries);

    /**
     * By default Cruise Control defines three roles: VIEWER, USER and ADMIN.
     * For more information checkout the upstream Cruise Control Wiki here:
     * <a href="https://github.com/linkedin/cruise-control/wiki/Security#authorization">Cruise Control Security</a>
     */
    public enum Role {
        /**
         * VIEWER: has access to the most lightweight kafka_cluster_state, user_tasks, and review_board endpoints.
         */
        VIEWER,
        /**
         * USER: has access to all the GET endpoints except bootstrap and train.
         */
        USER,
        /**
         * ADMIN: has access to all endpoints.
         */
        ADMIN;

        public static Role fromString(String s) {
            switch (s) {
                case "VIEWER":
                    return VIEWER;
                case "USER":
                    return USER;
                case "ADMIN":
                    return ADMIN;
                default:
                    throw new IllegalArgumentException("Unknown role: " + s);
            }
        }
    }

    /**
     * Represents a single API user entry including name, password, and role.
     */
    public static final class UserEntry {
        private final String username;
        private final String password;
        private final Role role;

        public UserEntry(String username, String password, Role role) {
            this.username = username;
            this.password = password;
            this.role = role;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public Role getRole() {
            return role;
        }
    }
}
