/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.auth;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.AuthIdentity;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the identity used during Service Account token authentication using a token obtained directly from the Kubernetes API.
 */
public class RequestedServiceAccountAuthIdentity implements AuthIdentity {
    private final String namespace;
    private final String serviceAccountName;
    private final String audience;
    private final long expirationSeconds;

    /**
     * Constructs the RequestedServiceAccountAuthIdentity.
     *
     * @param reconciliation        The reconciliation marker
     * @param audience              The audience for the token
     * @param expirationSeconds     The expiration time for the token in seconds
     */
    public RequestedServiceAccountAuthIdentity(Reconciliation reconciliation, String audience, long expirationSeconds) {
        this.namespace = reconciliation.namespace();
        this.serviceAccountName = KafkaResources.clusterOperatorServiceAccount(reconciliation.name());
        this.audience = audience;
        this.expirationSeconds = expirationSeconds;
    }

    @Override
    public boolean isSasl() {
        return true;
    }

    @Override
    public Map<String, String> kafkaClientProperties() {
        Map<String, String> config = new HashMap<>();

        config.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
        config.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, KubernetesRequestedServiceAccountTokenLoginCallbackHandler.class.getName());

        String jaasConfig = String.format("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
                        + "%s=\"%s\" "
                        + "%s=\"%s\" "
                        + "%s=\"%s\" "
                        + "%s=\"%s\";",
                KubernetesRequestedServiceAccountTokenLoginCallbackHandler.NAMESPACE_CONFIG, namespace,
                KubernetesRequestedServiceAccountTokenLoginCallbackHandler.SERVICE_ACCOUNT_CONFIG, serviceAccountName,
                KubernetesRequestedServiceAccountTokenLoginCallbackHandler.AUDIENCE_CONFIG, audience,
                KubernetesRequestedServiceAccountTokenLoginCallbackHandler.EXPIRATION_SECONDS_CONFIG, expirationSeconds);
        config.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);

        return config;
    }

    /**
     * Returns the namespace of the service account
     *
     * @return  The namespace of the service account
     */
    public String namespace() {
        return namespace;
    }

    /**
     * Returns the service account name
     *
     * @return  The service account name
     */
    public String serviceAccountName() {
        return serviceAccountName;
    }

    /**
     * Returns the audience for the token
     *
     * @return  The token audience
     */
    public String audience()    {
        return audience;
    }

    /**
     * Returns the expiration time for the token in seconds
     *
     * @return  The token expiration time in seconds
     */
    public long expirationSeconds()    {
        return expirationSeconds;
    }
}
