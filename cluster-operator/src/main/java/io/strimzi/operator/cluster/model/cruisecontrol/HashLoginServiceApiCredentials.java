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
public class HashLoginServiceApiCredentials {
    // Regex to match an entry in Jetty's HashLoginService's file format: username: password, rolename
    private static final Pattern HASH_LOGIN_SERVICE_PATTERN = Pattern.compile("^[\\w-]+\\s*:\\s*\\w+\\s*,\\s*\\w+\\s*$");

    private static final List<String> FORBIDDEN_USERNAMES = Arrays.asList(
            HEALTHCHECK_USERNAME,
            REBALANCE_OPERATOR_USERNAME,
            TOPIC_OPERATOR_USERNAME
    );

    private final String userManagedApiSecretName;
    private final String userManagedApiSecretKey;
    private final String namespace;
    private final String cluster;
    private final Labels labels;
    private final CruiseControlSpec ccSpec;
    private final OwnerReference ownerReference;

    /**
     * Constructs the Api Credentials Model for managing API users for Cruise Control API.
     *
     * @param namespace      Namespace of the cluster
     * @param cluster        Name of the cluster to which this component belongs
     * @param labels         Labels for Cruise Control instance
     * @param ownerReference Owner reference for Cruise Control instance
     * @param ccSpec         Custom resource section configuring Cruise Control API users
     */
    public HashLoginServiceApiCredentials(String namespace, String cluster, Labels labels, OwnerReference ownerReference, CruiseControlSpec ccSpec) {
        this.namespace = namespace;
        this.cluster = cluster;
        this.labels = labels;
        this.ownerReference = ownerReference;
        this.ccSpec = ccSpec;

        if (ccSpec.getApiUsers() != null) {
            if (ccSpec.getApiUsers() instanceof HashLoginServiceApiUsers apiUsers) {
                validateHashLoginServiceApiUsersConfiguration(apiUsers);
                this.userManagedApiSecretName = apiUsers.getValueFrom().getSecretKeyRef().getName();
                this.userManagedApiSecretKey = apiUsers.getValueFrom().getSecretKeyRef().getKey();
            } else {
                throw new InvalidResourceException("Unsupported API user configuration type " + ccSpec.getApiUsers().getType());
            }
        } else {
            this.userManagedApiSecretName = null;
            this.userManagedApiSecretKey = null;
        }
    }

    /**
     * @return Returns user-managed API credentials secret name
     */
    public String getUserManagedApiSecretName() {
        return this.userManagedApiSecretName;
    }

    /**
     * @return Returns user-managed API credentials secret key
     */
    /* test */ String getUserManagedApiSecretKey() {
        return this.userManagedApiSecretKey;
    }

    /**
     * Checks if Cruise Control spec has valid ApiUsers config.
     *
     * @param apiUsers The API users configuration
     */
    private static void validateHashLoginServiceApiUsersConfiguration(HashLoginServiceApiUsers apiUsers)  {
        if (apiUsers.getValueFrom() == null
                || apiUsers.getValueFrom().getSecretKeyRef() == null
                || apiUsers.getValueFrom().getSecretKeyRef().getName() == null
                || apiUsers.getValueFrom().getSecretKeyRef().getKey() == null) {
            throw new InvalidResourceException("The configuration of the Cruise Control REST API users " +
                    "referenced in spec.cruiseControl.apiUsers is invalid.");
        }
    }

    /**
     * Parse String containing API credential config into map of API user entries.
     *
     * @param config API credential config file as a String.
     *
     * @return map of API credential entries containing username, password, and role.
     */
    /* test */ static Map<String, UserEntry> parseEntriesFromString(String config) {
        Map<String, UserEntry> entries = new HashMap<>();

        if (config.isEmpty()) {
            return entries;
        }

        config.lines().forEach(line -> {
            Matcher matcher = HASH_LOGIN_SERVICE_PATTERN.matcher(line);
            if (matcher.matches()) {
                String[] parts = line.replaceAll("\\s", "").split(":");
                String username = parts[0];
                String password = parts[1].split(",")[0];
                Role role = Role.fromString(parts[1].split(",")[1]);
                if (entries.containsKey(username)) {
                    throw new InvalidConfigurationException("Duplicate username found: \"" + username + "\". "
                            + "Cruise Control API credentials config must contain unique usernames.");
                }
                entries.put(username, new UserEntry(username, password, role));
            } else {
                throw new InvalidConfigurationException("Invalid configuration provided: \"" + line + "\". " +
                        "Cruise Control API credentials config must follow " +
                        "HashLoginService's file format `username: password [,rolename ...]`.");
            }
        });
        return entries;
    }

    /**
     * Returns Cruise Control API auth credentials file as a String.
     *
     * @param entries Map of API user entries containing API user credentials.
     *
     * @return Returns Cruise Control API auth credentials file as a String.
     */
    private static String generateApiAuthFileAsString(Map<String, UserEntry> entries) {
        StringBuilder sb = new StringBuilder();
        // Follows  Jetty's HashLoginService's file format: username: password [,rolename ...]
        for (UserEntry e : entries.values()) {
            sb.append(e.username).append(": ").append(e.password).append(", ").append(e.role()).append("\n");
        }
        return sb.toString();
    }

    /**
     * Parses auth data from existing Topic Operator API user secret into map of API user entries.
     *
     * @param entries Map of API user entries containing API user credentials.
     * @param secret API user secret
     *
     * @return Map of API user entries containing user-managed API user credentials
     */
    /* test */ static void generateTOManagedApiCredentials(Map<String, UserEntry> entries, Secret secret) {
        if (secret != null) {
            if (secret.getData().containsKey(TOPIC_OPERATOR_USERNAME_KEY) && secret.getData().containsKey(TOPIC_OPERATOR_PASSWORD_KEY)) {
                String username = Util.decodeFromBase64(secret.getData().get(TOPIC_OPERATOR_USERNAME_KEY));
                String password = Util.decodeFromBase64(secret.getData().get(TOPIC_OPERATOR_PASSWORD_KEY));
                entries.put(username, new UserEntry(username, password, Role.ADMIN));
            }
        }
    }

    /**
     * Parses auth data from existing user-managed API user secret into List of API user entries.
     *
     * @param entries Map of API user entries containing API user credentials.
     * @param secret API user secret
     * @param secretKey API user secret key
     *
     * @return Map of API user entries containing user-managed API user credentials
     */
    /* test */ static void generateUserManagedApiCredentials(Map<String, UserEntry> entries, Secret secret, String secretKey) {
        if (secret != null) {
            if (secretKey != null && secret.getData().containsKey(secretKey)) {
                String credentialsAsString = Util.decodeFromBase64(secret.getData().get(secretKey));
                entries.putAll(parseEntriesFromString(credentialsAsString));
            }
        }
        for (UserEntry entry : entries.values()) {
            if (FORBIDDEN_USERNAMES.contains(entry.username())) {
                throw new InvalidConfigurationException("The following usernames for Cruise Control API are forbidden: " + FORBIDDEN_USERNAMES
                        + ". User provided Cruise Control API credentials contain illegal username: " + entry.username + ".");
            } else if (entry.role == Role.ADMIN) {
                throw new InvalidConfigurationException("The following roles for Cruise Control API are forbidden: " + Role.ADMIN
                        + ". User provided Cruise Control API credentials contain contains illegal role: " +  entry.role() + ".");
            }
        }
    }

    /**
     * Parses auth data from existing Cluster Operator managed API user secret into map of API user entries.
     *
     * @param entries Map of API user entries containing API user credentials.
     * @param passwordGenerator The password generator for API users
     * @param secret API user secret
     *
     * @return Map of API user entries containing Strimzi-managed API user credentials
     */
    /* test */ static void generateCoManagedApiCredentials(Map<String, UserEntry> entries, PasswordGenerator passwordGenerator, Secret secret) {
        if (secret != null) {
            if (secret.getData().containsKey(AUTH_FILE_KEY)) {
                String credentialsAsString = Util.decodeFromBase64(secret.getData().get(AUTH_FILE_KEY));
                entries.putAll(parseEntriesFromString(credentialsAsString));
            }
        }

        if (!entries.containsKey(REBALANCE_OPERATOR_USERNAME)) {
            entries.put(REBALANCE_OPERATOR_USERNAME, new UserEntry(REBALANCE_OPERATOR_USERNAME, passwordGenerator.generate(), Role.ADMIN));
        }

        if (!entries.containsKey(HEALTHCHECK_USERNAME)) {
            entries.put(HEALTHCHECK_USERNAME, new UserEntry(HEALTHCHECK_USERNAME, passwordGenerator.generate(), Role.USER));
        }

    }

    /**
     * Creates map with API usernames, passwords, and credentials file for Strimzi-managed API users secret.
     *
     * @param passwordGenerator the password generator used for creating new credentials.
     * @param cruiseControlApiSecret the existing Cruise Control API secret containing all API credentials.
     * @param userManagedApiSecret the secret managed by the user, containing user-defined API credentials.
     * @param topicOperatorManagedApiSecret the secret managed by the topic operator, containing credentials for the topic operator.
     *
     * @return Map with API usernames, passwords, and credentials file for Strimzi-managed CC API secret.
     */
    private Map<String, String> generateMapWithApiCredentials(PasswordGenerator passwordGenerator,
                                                              Secret cruiseControlApiSecret,
                                                              Secret userManagedApiSecret,
                                                              Secret topicOperatorManagedApiSecret) {
        Map<String, UserEntry> userEntries = new HashMap<>();
        generateUserManagedApiCredentials(userEntries, userManagedApiSecret, userManagedApiSecretKey);
        generateTOManagedApiCredentials(userEntries, topicOperatorManagedApiSecret);
        generateCoManagedApiCredentials(userEntries, passwordGenerator, cruiseControlApiSecret);

        Map<String, String> data = new HashMap<>(3);
        data.put(REBALANCE_OPERATOR_PASSWORD_KEY, Util.encodeToBase64(userEntries.get(REBALANCE_OPERATOR_USERNAME).password));
        data.put(HEALTHCHECK_PASSWORD_KEY, Util.encodeToBase64(userEntries.get(HEALTHCHECK_USERNAME).password));
        data.put(AUTH_FILE_KEY, Util.encodeToBase64(generateApiAuthFileAsString(userEntries)));
        return data;
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
     * @param oldCruiseControlApiSecret the existing Cruise Control API secret, containing previously stored credentials.
     * @param userManagedApiSecret the secret managed by the user, containing user-defined API credentials.
     * @param topicOperatorManagedApiSecret the secret managed by the topic operator, containing credentials for the topic operator.
     * @return a new Secret object containing the aggregated API credentials for Cruise Control.
     */
    public Secret generateApiSecret(PasswordGenerator passwordGenerator,
                                    Secret oldCruiseControlApiSecret,
                                    Secret userManagedApiSecret,
                                    Secret topicOperatorManagedApiSecret) {
        if (this.ccSpec.getApiUsers() != null && userManagedApiSecret == null) {
            throw new InvalidResourceException("The configuration of the Cruise Control REST API users " +
                    "references a secret: \"" +  userManagedApiSecretName + "\" that does not exist.");
        }
        Map<String, String> mapWithApiCredentials = generateMapWithApiCredentials(passwordGenerator,
                oldCruiseControlApiSecret, userManagedApiSecret, topicOperatorManagedApiSecret);
        return ModelUtils.createSecret(CruiseControlResources.apiSecretName(cluster), namespace, labels, ownerReference,
                mapWithApiCredentials, Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * By default Cruise Control defines three roles: VIEWER, USER and ADMIN.
     * For more information checkout the upstream Cruise Control Wiki here:
     * <a href="https://github.com/linkedin/cruise-control/wiki/Security#authorization">Cruise Control Security</a>
     */
    /* test */ enum Role {
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
    /* test */ record UserEntry(String username, String password, Role role) { }
}
