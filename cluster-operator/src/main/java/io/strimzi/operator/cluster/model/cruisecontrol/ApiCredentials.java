/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.HashLoginServiceApiUsers;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.AUTH_FILE_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.HEALTHCHECK_PASSWORD_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.HEALTHCHECK_USERNAME;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.REBALANCE_OPERATOR_PASSWORD_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.REBALANCE_OPERATOR_USERNAME;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.TOPIC_OPERATOR_PASSWORD_KEY;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.TOPIC_OPERATOR_USERNAME;
import static io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties.TOPIC_OPERATOR_USERNAME_KEY;

/**
 * Uses information in a Kafka Custom Resource to generate a API credentials configuration file to be used for
 * authenticating to Cruise Control's REST API.
 */
public class ApiCredentials {
    // Regex to match an entry in Jetty's HashLoginService's file format: username: password, rolename
    private static final Pattern HASH_LOGIN_SERVICE_PATTERN = Pattern.compile("^[\\w-]+\\s*:\\s*\\w+\\s*,\\s*\\w+\\s*$");
    private static final List<String> FORBIDDEN_USERNAMES = Arrays.asList(
            HEALTHCHECK_USERNAME,
            REBALANCE_OPERATOR_USERNAME,
            TOPIC_OPERATOR_USERNAME
    );

    private String userManagedApiSecretName;
    private String userManagedApiSecretKey;
    private final String namespace;
    private final String cluster;
    private final Labels labels;
    private final OwnerReference ownerReference;

    /**
     * Constructs the Api Credentials Model for managing API users for Cruise Control API.
     *
     * @param namespace         Namespace of the cluster
     * @param cluster           Name of the cluster to which this component belongs
     * @param labels            Labels for Cruise Control instance
     * @param ownerReference    Owner reference for Cruise Control instance
     * @param specSection       Custom resource section configuring Cruise Control API users
     */
    public ApiCredentials(String namespace, String cluster, Labels labels, OwnerReference ownerReference, CruiseControlSpec specSection) {
        this.namespace = namespace;
        this.cluster = cluster;
        this.labels = labels;
        this.ownerReference = ownerReference;

        if (validateApiUsersConfig(specSection)) {
            HashLoginServiceApiUsers apiUsers = specSection.getApiUsers();
            userManagedApiSecretName = apiUsers.getValueFrom().getSecretKeyRef().getName();
            userManagedApiSecretKey = apiUsers.getValueFrom().getSecretKeyRef().getKey();
        }
    }

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

        private static Role fromString(String s) {
            return switch (s) {
                case "VIEWER" -> VIEWER;
                case "USER" -> USER;
                case "ADMIN" -> ADMIN;
                default -> throw new InvalidConfigurationException("Unknown role: " + s);
            };
        }
    }

    /**
     * Represents a single API user entry including name, password, and role.
     */
    public record UserEntry(String username, String password, Role role) { }

    /**
     * @return  Returns user-managed API credentials secret name
     */
    public String getUserManagedApiSecretName() {
        return userManagedApiSecretName;
    }

    /**
     * Checks if Cruise Control spec has valid ApiUsers config.
     *
     * @param ccSpec The Cruise Control spec to check.
     */
    private static boolean validateApiUsersConfig(CruiseControlSpec ccSpec)  {
        HashLoginServiceApiUsers apiUsers = ccSpec.getApiUsers();
        if (apiUsers != null)    {
            if (apiUsers.getValueFrom() == null
                    || apiUsers.getValueFrom().getSecretKeyRef() == null
                    || apiUsers.getValueFrom().getSecretKeyRef().getName() == null
                    || apiUsers.getValueFrom().getSecretKeyRef().getKey() == null) {
                throw new InvalidResourceException("The configuration of the Cruise Control REST API users " +
                        "referenced in spec.cruiseControl.apiUsers is invalid.");
            } else {
                return true;
            }
        }
        return false;
    }

    /**
     * Parse String containing API credential config into map of API user entries.
     *
     * @param config API credential config file as a String.
     *
     * @return map of API credential entries containing username, password, and role.
     */
    public static Map<String, UserEntry> parseEntriesFromString(String config) {
        Map<String, UserEntry> entries = new HashMap<>();
        for (String line : config.split("\n")) {
            Matcher matcher = HASH_LOGIN_SERVICE_PATTERN.matcher(line);
            if (matcher.matches()) {
                String[] parts = line.replaceAll("\\s", "").split(":");
                String username = parts[0];
                String password = parts[1].split(",")[0];
                Role role = Role.fromString(parts[1].split(",")[1]);
                if (entries.containsKey(username)) {
                    throw new InvalidConfigurationException("Duplicate username found: " + "\"" + username + "\". "
                            + "Cruise Control API credentials config must contain unique usernames");
                }
                entries.put(username, new UserEntry(username, password, role));
            } else {
                throw new InvalidConfigurationException("Invalid configuration provided: " +  "\"" + line + "\". " +
                        "Cruise Control API credentials config must follow " +
                        "HashLoginService's file format `username: password [,rolename ...]`");
            }
        }
        return entries;
    }

    /**
     * Parses auth data from existing Topic Operator API user secret into map of API user entries.
     *
     * @param secret API user secret
     *
     * @return Map of API user entries containing user-managed API user credentials
     */
    public static Map<String, UserEntry> generateToManagedApiCredentials(Secret secret) {
        Map<String, UserEntry> entries = new HashMap<>();
        if (secret != null) {
            if (secret.getData().containsKey(TOPIC_OPERATOR_USERNAME_KEY) && secret.getData().containsKey(TOPIC_OPERATOR_PASSWORD_KEY)) {
                String username = Util.decodeFromBase64(secret.getData().get(TOPIC_OPERATOR_USERNAME_KEY));
                String password = Util.decodeFromBase64(secret.getData().get(TOPIC_OPERATOR_PASSWORD_KEY));
                entries.put(username, new UserEntry(username, password, Role.ADMIN));
            }
        }
        return entries;
    }

    /**
     * Parses auth data from existing user-managed API user secret into List of API user entries.
     *
     * @param secret API user secret
     * @param secretKey API user secret key
     *
     * @return Map of API user entries containing user-managed API user credentials
     */
    public static Map<String, UserEntry> generateUserManagedApiCredentials(Secret secret, String secretKey) {
        Map<String, UserEntry> entries = new HashMap<>();
        if (secret != null) {
            if (secretKey != null && secret.getData().containsKey(secretKey)) {
                String credentialsAsString = Util.decodeFromBase64(secret.getData().get(secretKey));
                entries.putAll(parseEntriesFromString(credentialsAsString));
            }
        }
        for (UserEntry entry : entries.values()) {
            if (FORBIDDEN_USERNAMES.contains(entry.username())) {
                throw new InvalidConfigurationException("The following usernames for Cruise Control API are forbidden: " + FORBIDDEN_USERNAMES
                        + " User provided Cruise Control API credentials contain illegal username: " + entry.username());
            } else if (entry.role() == Role.ADMIN) {
                throw new InvalidConfigurationException("The following roles for Cruise Control API are forbidden: " + Role.ADMIN
                        + " User provided Cruise Control API credentials contain contains illegal role: " +  entry.role());
            }
        }
        return entries;
    }

    /**
     * Parses auth data from existing Cluster Operator managed API user secret into map of API user entries.
     *
     * @param passwordGenerator The password generator for API users
     * @param secret API user secret
     *
     * @return Map of API user entries containing Strimzi-managed API user credentials
     */
    public static Map<String, UserEntry> generateCoManagedApiCredentials(PasswordGenerator passwordGenerator, Secret secret) {
        Map<String, UserEntry> entries = new HashMap<>();

        if (secret != null) {
            if (secret.getData().containsKey(AUTH_FILE_KEY)) {
                String credentialsAsString = Util.decodeFromBase64(secret.getData().get(AUTH_FILE_KEY));
                entries.putAll(parseEntriesFromString(credentialsAsString));
            }
        }

        if (!entries.containsKey(REBALANCE_OPERATOR_USERNAME)) {
            entries.put(REBALANCE_OPERATOR_USERNAME, new UserEntry(REBALANCE_OPERATOR_USERNAME, passwordGenerator.generate(), ApiCredentials.Role.ADMIN));
        }

        if (!entries.containsKey(HEALTHCHECK_USERNAME)) {
            entries.put(HEALTHCHECK_USERNAME, new UserEntry(HEALTHCHECK_USERNAME, passwordGenerator.generate(), ApiCredentials.Role.USER));
        }

        return entries;
    }

    /**
     * Creates map with API usernames, passwords, and credentials file for Strimzi-managed API users secret.
     *
     * @param entries List of API user entries containing API user credentials
     *
     * @return Map containing Cruise Control API auth credentials
     */
    public static Map<String, String> generateMapWithApiCredentials(Map<String, UserEntry> entries) {
        Map<String, String> data = new HashMap<>(3);
        data.put(REBALANCE_OPERATOR_PASSWORD_KEY, Util.encodeToBase64(entries.get(REBALANCE_OPERATOR_USERNAME).password()));
        data.put(HEALTHCHECK_PASSWORD_KEY, Util.encodeToBase64(entries.get(HEALTHCHECK_USERNAME).password()));
        data.put(AUTH_FILE_KEY, Util.encodeToBase64(generateApiAuthFileAsString(entries)));
        return data;
    }

    /**
     * Returns Cruise Control API auth credentials file following Jetty's
     * HashLoginService's file format: username: password [,rolename ...]
     * as a String.
     *
     * @param entries Map of API user entries containing API user credentials.
     *
     * @return Returns Cruise Control API auth credentials file as a String.
     */
    private static String generateApiAuthFileAsString(Map<String, UserEntry> entries) {
        StringBuilder sb = new StringBuilder();
        for (UserEntry e : entries.values()) {
            sb.append(e.username).append(": ").append(e.password).append(", ").append(e.role).append("\n");
        }
        return sb.toString();
    }

    /**
     * Generates a new API secret for Cruise Control by aggregating credentials from various sources.
     * This method collects API credentials from three potential sources:
     *   (1) Old Cruise Control API secret
     *   (2) User-managed API secret
     *   (3) Topic operator-managed API secret.
     * It uses these credentials to create a comprehensive map of API credentials, which is then used to generate a new API secret
     * for Cruise Control.
     *
     * @param passwordGenerator the password generator used for creating new credentials.
     * @param oldCruiseControlApiSecret the existing centralized API secret, containing previously stored credentials.
     * @param userManagedApiSecret the secret managed by the user, containing user-defined API credentials.
     * @param topicOperatorManagedApiSecret the secret managed by the topic operator, containing credentials for the topic operator.
     * @return a new Secret object containing the aggregated API credentials for Cruise Control.
     */
    public Secret generateApiSecret(PasswordGenerator passwordGenerator,
                                    Secret oldCruiseControlApiSecret,
                                    Secret userManagedApiSecret,
                                    Secret topicOperatorManagedApiSecret) {

        Map<String, ApiCredentials.UserEntry> apiCredentials = new HashMap<>();
        apiCredentials.putAll(generateCoManagedApiCredentials(passwordGenerator, oldCruiseControlApiSecret));
        apiCredentials.putAll(generateUserManagedApiCredentials(userManagedApiSecret, userManagedApiSecretKey));
        apiCredentials.putAll(generateToManagedApiCredentials(topicOperatorManagedApiSecret));

        Map<String, String> mapWithApiCredentials = generateMapWithApiCredentials(apiCredentials);
        return ModelUtils.createSecret(CruiseControlResources.apiSecretName(cluster), namespace, labels, ownerReference,
                mapWithApiCredentials, Collections.emptyMap(), Collections.emptyMap());
    }
}
