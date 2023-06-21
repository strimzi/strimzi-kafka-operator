/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol.api;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.Password;
import io.strimzi.api.kafka.model.balancing.ApiUser;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.cluster.model.CruiseControl.API_USER_KEY_PASSWORD;

/**
 * Class for handling Cruise Control REST API configuration
 */
public class CruiseControlRestApiUtil {
    protected static final String COMPONENT_TYPE = "cruise-control";

    /**
     * API user and roles used exclusively for KafkaRebalance Operator
     * for accessing Cruise Control REST API.
     */
    private static final String API_ADMIN_ROLE = "ADMIN";
    private static final String API_HEALTHCHECK_ROLE = "USER";

    /**
     * Keys for accessing admin and healthcheck password within API auth credential file secret
     */
    public static final String API_ADMIN_PASSWORD_KEY = COMPONENT_TYPE + ".apiAdminPassword";
    private static final String API_HEALTHCHECK_PASSWORD_KEY = COMPONENT_TYPE + ".apiHealthcheckPassword";
    private static final String API_AUTH_FILE_KEY = COMPONENT_TYPE + ".apiAuthFile";

    /**
     * Returns the key of the secret in the provided ApiUser object.
     *
     * @param user Name of API user
     * @return Key of the Kubernetes Secret of the provided ApiUser object or null if not set.
     */
    private static String getUserSecretKey(ApiUser user) {
        Password password = user.getPassword();
        if (password != null) {
            if (password.getValueFrom() == null
                    || password.getValueFrom().getSecretKeyRef() == null
                    || password.getValueFrom().getSecretKeyRef().getKey() == null) {
                throw new InvalidResourceException("Resource requests custom password but doesn't specify the secret key");
            } else {
                return password.getValueFrom().getSecretKeyRef().getKey();
            }
        } else {
            return API_USER_KEY_PASSWORD;
        }
    }

    /**
     * Returns the name of the secret in the provided ApiUser object.
     *
     * @param clusterName Name of cluster
     * @param user Name of API user
     * @return Name of the Kubernetes Secret in the provided Password object or null if not set.
     */
    public static String getUserSecretName(String clusterName, ApiUser user) {
        Password password = user.getPassword();
        if (password != null) {
            if (password.getValueFrom() == null
                    || password.getValueFrom().getSecretKeyRef() == null
                    || password.getValueFrom().getSecretKeyRef().getName() == null) {
                throw new InvalidResourceException("Resource requests custom password but doesn't specify the secret name");
            } else {
                return password.getValueFrom().getSecretKeyRef().getName();
            }
        } else {
            return CruiseControlResources.apiAuthUserSecretName(clusterName, user.getName());
        }
    }

    private static String getUserPassword(String clusterName, ApiUser apiUser, List<Secret> apiUserSecrets) {
        String secretName = getUserSecretName(clusterName, apiUser);
        String secretKey = getUserSecretKey(apiUser);
        for (Secret s : apiUserSecrets) {
            if (s.getMetadata().getName().equals(secretName)) {
                return s.getData().get(secretKey);
            }
        }
        return null;
    }

    private static String createCredentialEntry(String name, String password, String role) {
        return name + ": " + password + ", " + role + "\n";
    }

    /**
     * Generates map containing data for Cruise Control API auth credentials file
     *
     * @param clusterName name of cluster
     * @param apiUsers list of specified API users
     * @param apiUserSecrets List of secrets containing passwords for specified API users
     *
     * @return Map containing Cruise Control API auth credentials
     */
    public static Map<String, String> generateCruiseControlApiCredentials(String clusterName, List<ApiUser> apiUsers, List<Secret> apiUserSecrets) {
        PasswordGenerator passwordGenerator = new PasswordGenerator(16);
        String apiAdminPassword = passwordGenerator.generate();
        String apiHealthcheckPassword = passwordGenerator.generate();

        /**
         * Create Cruise Control API auth credentials file following Jetty's
         * HashLoginService's file format: username: password [,rolename ...]
         */
        StringBuilder authCredentialsFile = new StringBuilder();
        authCredentialsFile.append(createCredentialEntry(CruiseControl.API_ADMIN_NAME, apiAdminPassword, API_ADMIN_ROLE));
        authCredentialsFile.append(createCredentialEntry(CruiseControl.API_HEALTHCHECK_NAME, apiHealthcheckPassword, API_HEALTHCHECK_ROLE));
        for (ApiUser user : apiUsers) {
            authCredentialsFile.append(createCredentialEntry(user.getName(), getUserPassword(clusterName, user, apiUserSecrets), user.getRole()));
        }

        Map<String, String> data = new HashMap<>(3);
        data.put(API_ADMIN_PASSWORD_KEY, Util.encodeToBase64(apiAdminPassword));
        data.put(API_HEALTHCHECK_PASSWORD_KEY, Util.encodeToBase64(apiHealthcheckPassword));

        data.put(API_AUTH_FILE_KEY, Util.encodeToBase64(authCredentialsFile.toString()));

        return data;
    }
}
