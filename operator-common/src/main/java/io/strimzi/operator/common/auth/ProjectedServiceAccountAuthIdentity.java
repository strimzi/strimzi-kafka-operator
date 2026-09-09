/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.auth;

import org.apache.kafka.common.config.SaslConfigs;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the identity used during Service Account token authentication using a projected / mounted token volume.
 */
public class ProjectedServiceAccountAuthIdentity implements AuthIdentity {
    private final String serviceAccountTokenPath;

    /**
     * Constructs the ProjectedServiceAccountAuthIdentity.
     *
     * @param serviceAccountTokenPath   The path to the projected service account token file
     */
    public ProjectedServiceAccountAuthIdentity(String serviceAccountTokenPath) {
        this.serviceAccountTokenPath = serviceAccountTokenPath;
    }

    @Override
    public boolean isSasl() {
        return true;
    }

    @Override
    public Map<String, String> kafkaClientProperties() {
        Map<String, String> config = new HashMap<>();

        config.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
        config.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
        config.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "oauth.access.token.location=\"" + serviceAccountTokenPath + "\";");

        return config;
    }
}
