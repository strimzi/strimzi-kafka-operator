/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.systemtest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Environment {

    private static final Logger LOGGER = LogManager.getLogger(Environment.class);


    public static final String KUBERNETES_NAMESPACE = "KUBERNETES_NAMESPACE";
    public static final String KUBERNETES_API_URL = "KUBERNETES_API_URL";
    public static final String KUBERNETES_API_TOKEN = "KUBERNETES_API_TOKEN";

    private final String token = System.getenv(KUBERNETES_API_TOKEN);
    private final String url = System.getenv(KUBERNETES_API_URL);
    private final String namespace = System.getenv(KUBERNETES_NAMESPACE);

    public Environment() {
        String debugFormat = "{}:{}";
        LOGGER.debug(debugFormat, KUBERNETES_NAMESPACE, namespace);
        LOGGER.debug(debugFormat, KUBERNETES_API_URL, url);
        LOGGER.debug(debugFormat, KUBERNETES_API_TOKEN, token);
    }

    public String getApiUrl() {
        return url;
    }

    public String getApiToken() {
        return token;
    }

    public String namespace() {
        return namespace;
    }
}
