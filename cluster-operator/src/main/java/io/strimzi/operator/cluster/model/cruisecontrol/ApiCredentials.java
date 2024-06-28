/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.ApiUsers;
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
    private static final List<String> FORBIDDEN_USERNAMES = Arrays.asList(
            HEALTHCHECK_USERNAME,
            REBALANCE_OPERATOR_USERNAME,
            TOPIC_OPERATOR_USERNAME
    );

    private final Type type;
    private final String userManagedApiSecretName;
    private final String userManagedApiSecretKey;
    private final String namespace;
    private final String cluster;
    private final Labels labels;
    private final CruiseControlSpec specSection;
    private final OwnerReference ownerReference;

    /**
     * Constructs the Api Credentials Model for managing API users for Cruise Control API.
     *
     * @param namespace         Namespace of the cluster
     * @param cluster           Name of the cluster to which this component belongs
     * @param labels            Labels for Cruise Control instance
     * @param ownerReference    Owner reference for Cruise Control instance
     * @param ccSection         Custom resource section configuring Cruise Control API users
     */
    public ApiCredentials(String namespace, String cluster, Labels labels, OwnerReference ownerReference, CruiseControlSpec ccSection) {
        this.namespace = namespace;
        this.cluster = cluster;
        this.labels = labels;
        this.ownerReference = ownerReference;
        this.specSection = ccSection;

        if (checkApiUsersConfig(ccSection)) {
            type = Type.fromString(ccSection.getApiUsers().getType());
            userManagedApiSecretName = ccSection.getApiUsers().getValueFrom().getSecretKeyRef().getName();
            userManagedApiSecretKey = ccSection.getApiUsers().getValueFrom().getSecretKeyRef().getKey();
        } else {
            type = null;
            userManagedApiSecretName = null;
            userManagedApiSecretKey = null;
        }
    }

    /**
     * @return  Returns user-managed API credentials secret name
     */
    public String getUserManagedApiSecretName() {
        return userManagedApiSecretName;
    }

    /**
     * @return  Returns user-managed API credentials secret key
     */
    /* test */ String getUserManagedApiSecretKey() {
        return userManagedApiSecretKey;
    }

    /**
     * Checks if Cruise Control spec has valid ApiUsers config.
     *
     * @param ccSpec The Cruise Control spec to check.
     */
    private static boolean checkApiUsersConfig(CruiseControlSpec ccSpec)  {
        HashLoginServiceApiUsers apiUsers = ccSpec.getApiUsers();
        if (apiUsers != null)    {
            if (apiUsers.getType() == null
                    || apiUsers.getValueFrom() == null
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
     * @param pattern Regex pattern used to parse Cruise Control API Users configuration
     *
     * @return map of API credential entries containing username, password, and role.
     */
    /* test */ static Map<String, UserEntry> parseEntriesFromString(String config, Pattern pattern) {
        Map<String, UserEntry> entries = new HashMap<>();
        for (String line : config.split("\n")) {
            Matcher matcher = pattern.matcher(line);
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
    /* test */ static Map<String, UserEntry> generateToManagedApiCredentials(Secret secret) {
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
     * @param pattern Regex pattern used to parse Cruise Control API Users configuration
     *
     * @return Map of API user entries containing user-managed API user credentials
     */
    /* test */ static Map<String, UserEntry> generateUserManagedApiCredentials(Secret secret, String secretKey, Pattern pattern) {
        Map<String, UserEntry> entries = new HashMap<>();
        if (secret != null) {
            if (secretKey != null && secret.getData().containsKey(secretKey)) {
                String credentialsAsString = Util.decodeFromBase64(secret.getData().get(secretKey));
                entries.putAll(parseEntriesFromString(credentialsAsString, pattern));
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
     * @param pattern Regex pattern used to parse Cruise Control API Users configuration
     *
     * @return Map of API user entries containing Strimzi-managed API user credentials
     */
    /* test */ static Map<String, UserEntry> generateCoManagedApiCredentials(PasswordGenerator passwordGenerator, Secret secret, Pattern pattern) {
        Map<String, UserEntry> entries = new HashMap<>();

        if (secret != null) {
            if (secret.getData().containsKey(AUTH_FILE_KEY)) {
                String credentialsAsString = Util.decodeFromBase64(secret.getData().get(AUTH_FILE_KEY));
                entries.putAll(parseEntriesFromString(credentialsAsString, pattern));
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

        Pattern pattern = getApiUsersObject(type).getPattern();
        Map<String, ApiCredentials.UserEntry> apiCredentials = new HashMap<>();
        apiCredentials.putAll(generateCoManagedApiCredentials(passwordGenerator, cruiseControlApiSecret, pattern));
        apiCredentials.putAll(generateUserManagedApiCredentials(userManagedApiSecret, userManagedApiSecretKey, pattern));
        apiCredentials.putAll(generateToManagedApiCredentials(topicOperatorManagedApiSecret));

        Map<String, String> data = new HashMap<>(3);
        data.put(REBALANCE_OPERATOR_PASSWORD_KEY, Util.encodeToBase64(apiCredentials.get(REBALANCE_OPERATOR_USERNAME).password()));
        data.put(HEALTHCHECK_PASSWORD_KEY, Util.encodeToBase64(apiCredentials.get(HEALTHCHECK_USERNAME).password()));
        data.put(AUTH_FILE_KEY, Util.encodeToBase64(generateApiAuthFileAsString(type, apiCredentials)));
        return data;
    }

    /**
     * Returns Cruise Control API auth credentials file as a String.
     *
     * @param type Type of the Cruise Control API Users configuration.
     * @param entries Map of API user entries containing API user credentials.
     *
     * @return Returns Cruise Control API auth credentials file as a String.
     */
    private static String generateApiAuthFileAsString(Type type, Map<String, UserEntry> entries) {
        StringBuilder sb = new StringBuilder();

        if (type == Type.HASH_LOGIN_SERVICE) {
            // Follows  Jetty's HashLoginService's file format: username: password [,rolename ...]
            for (UserEntry e : entries.values()) {
                sb.append(e.username).append(": ").append(e.password).append(", ").append(e.role).append("\n");
            }
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
     * @param oldCruiseControlApiSecret the existing Cruise Control API secret, containing previously stored credentials.
     * @param userManagedApiSecret the secret managed by the user, containing user-defined API credentials.
     * @param topicOperatorManagedApiSecret the secret managed by the topic operator, containing credentials for the topic operator.
     * @return a new Secret object containing the aggregated API credentials for Cruise Control.
     */
    public Secret generateApiSecret(PasswordGenerator passwordGenerator,
                                    Secret oldCruiseControlApiSecret,
                                    Secret userManagedApiSecret,
                                    Secret topicOperatorManagedApiSecret) {
        if (specSection.getApiUsers() != null && userManagedApiSecret == null) {
            throw new InvalidResourceException("The configuration of the Cruise Control REST API users " +
                    "references a secret: " +  "\"" + getUserManagedApiSecretName() + "\" that does not exist.");
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
     * Type of the Cruise Control API Users configuration.
     */
    private enum Type {
        /**
         * HASH_LOGIN_SERVICE: Jetty's HashLoginService' file format: username: password [,rolename ...]
         */
        HASH_LOGIN_SERVICE;

        private static Type fromString(String s) {
            return switch (s) {
                case  HashLoginServiceApiUsers.TYPE_HASH_LOGIN_SERVICE -> HASH_LOGIN_SERVICE;
                default -> throw new InvalidConfigurationException("Unknown ApiUsers type: " + s);
            };
        }
    }

    /**
     * @return API Users object of given type
     */
    private static ApiUsers getApiUsersObject(Type t) {
        return switch (t) {
            case HASH_LOGIN_SERVICE -> new HashLoginServiceApiUsers();
            // Add other cases here for different subclasses
            default -> throw new IllegalArgumentException("Unknown ApiUsers type: " + t);
        };
    }

    /**
     * Represents a single API user entry including name, password, and role.
     */
    /* test */ record UserEntry(String username, String password, Role role) { }
}
