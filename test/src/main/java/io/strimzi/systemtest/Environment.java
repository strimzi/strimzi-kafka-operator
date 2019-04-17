/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.systemtest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Environment {

    private static final Logger LOGGER = LogManager.getLogger(Environment.class);

    public static final String useMinikubeEnv = "USE_MINIKUBE";
    public static final String keycloakAdminPasswordEnv = "KEYCLOAK_ADMIN_PASSWORD";
    public static final String keycloakAdminUserEnv = "KEYCLOAK_ADMIN_USER";
    public static final String testLogDirEnv = "TEST_LOGDIR";
    public static final String namespaceEnv = "KUBERNETES_NAMESPACE";
    public static final String urlEnv = "KUBERNETES_API_URL";
    public static final String tokenEnv = "KUBERNETES_API_TOKEN";
    public static final String upgradeEnv = "SYSTEMTESTS_UPGRADED";

    private final String token = System.getenv(tokenEnv);
    private final String url = System.getenv(urlEnv);
    private final String namespace = System.getenv(namespaceEnv);
    private final String testLogDir = System.getenv().getOrDefault(testLogDirEnv, "/tmp/testlogs");
    private final String keycloakAdminUser = System.getenv().getOrDefault(keycloakAdminUserEnv, "admin");
    private final String keycloakAdminPassword = System.getenv(keycloakAdminPasswordEnv);
    private final boolean useMinikube = Boolean.parseBoolean(System.getenv().getOrDefault(useMinikubeEnv, "false"));
    private final boolean upgrade = Boolean.parseBoolean(System.getenv().getOrDefault(upgradeEnv, "false"));

    public Environment() {
        String debugFormat = "{}:{}";
        LOGGER.debug(debugFormat, useMinikubeEnv, useMinikube);
        LOGGER.debug(debugFormat, keycloakAdminPasswordEnv, keycloakAdminPassword);
        LOGGER.debug(debugFormat, keycloakAdminUserEnv, keycloakAdminUser);
        LOGGER.debug(debugFormat, testLogDirEnv, testLogDir);
        LOGGER.debug(debugFormat, namespaceEnv, namespace);
        LOGGER.debug(debugFormat, urlEnv, url);
        LOGGER.debug(debugFormat, tokenEnv, token);
        LOGGER.debug(debugFormat, upgradeEnv, upgrade);
    }

    /**
     * Create dummy address in shared address-spaces due to faster deploy of next addresses
     */
    private final boolean useDummyAddress = Boolean.parseBoolean(System.getenv("USE_DUMMY_ADDRESS"));

    /**
     * Skip removing address-spaces
     */
    private final boolean skipCleanup = Boolean.parseBoolean(System.getenv("SKIP_CLEANUP"));

    /**
     * Store screenshots every time
     */
    private final boolean storeScreenshots = Boolean.parseBoolean(System.getenv("STORE_SCREENSHOTS"));

    public String getApiUrl() {
        return url;
    }

    public String getApiToken() {
        return token;
    }

    public String namespace() {
        return namespace;
    }

    public String testLogDir() {
        return testLogDir;
    }

    public boolean useMinikube() {
        return useMinikube;
    }

    public boolean useDummyAddress() {
        return useDummyAddress;
    }

    public boolean skipCleanup() {
        return skipCleanup;
    }

    public boolean storeScreenshots() {
        return storeScreenshots;
    }

    public boolean isUpgraded() {
        return upgrade;
    }
}
