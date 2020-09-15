/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd.kafkaclients;

import io.fabric8.kubernetes.api.model.batch.DoneableJob;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.keycloak.KeycloakInstance;

import java.security.InvalidParameterException;

public class KafkaOauthExampleClients extends KafkaBasicExampleClients {

    private String oauthClientId;
    private String oauthClientSecret;
    private String oauthTokenEndpointUri;
    private String userName;

    public static class Builder extends KafkaBasicExampleClients.Builder<Builder> {
        private String oauthClientId;
        private String oauthClientSecret;
        private String oauthTokenEndpointUri;
        private String userName;

        public Builder withOAuthClientId(String oauthClientId) {
            this.oauthClientId = oauthClientId;
            return self();
        }

        public Builder withOAuthClientSecret(String oauthClientSecret) {
            this.oauthClientSecret = oauthClientSecret;
            return self();
        }

        public Builder withOAuthTokenEndpointUri(String oauthTokenEndpointUri) {
            this.oauthTokenEndpointUri = oauthTokenEndpointUri;
            return self();
        }

        public Builder withUserName(String userName) {
            this.userName = userName;
            return self();
        }

        @Override
        public KafkaOauthExampleClients build() {
            return new KafkaOauthExampleClients(this);
        }

        @Override
        protected KafkaOauthExampleClients.Builder self() {
            return this;
        }
    }

    private KafkaOauthExampleClients(KafkaOauthExampleClients.Builder builder) {
        super(builder);
        if (builder.oauthClientId == null || builder.oauthClientId.isEmpty()) throw new InvalidParameterException("OAuth client id is not set.");
        if (builder.oauthClientSecret == null || builder.oauthClientSecret.isEmpty()) throw new InvalidParameterException("OAuth client secret is not set.");
        if (builder.oauthTokenEndpointUri == null || builder.oauthTokenEndpointUri.isEmpty()) throw new InvalidParameterException("OAuth token endpoint url is not set.");
        if (builder.userName == null || builder.userName.isEmpty()) builder.userName = builder.oauthClientId;

        oauthClientId = builder.oauthClientId;
        oauthClientSecret = builder.oauthClientSecret;
        oauthTokenEndpointUri = builder.oauthTokenEndpointUri;
        userName = builder.userName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public DoneableJob producerStrimziOauthPlain() {

        return producerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addNewEnv()
                                .withName("OAUTH_CLIENT_ID")
                                .withValue(oauthClientId)
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CLIENT_SECRET")
                                .editOrNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(oauthClientSecret)
                                        .withKey("clientSecret")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_TOKEN_ENDPOINT_URI")
                                .withValue(oauthTokenEndpointUri)
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CRT")
                                .editOrNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                        .withKey(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }

    public DoneableJob producerStrimziOauthTls(String clusterName) {

        return producerStrimziOauthPlain()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addNewEnv()
                                // disable hostname verification
                                .withName("OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM")
                                .withValue("")
                            .endEnv()
                            .addNewEnv()
                                .withName("CA_CRT")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(KafkaResources.clusterCaCertificateSecretName(clusterName))
                                        .withKey("ca.crt")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("USER_CRT")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(userName)
                                        .withKey("user.crt")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("USER_KEY")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(userName)
                                        .withKey("user.key")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }

    public DoneableJob consumerStrimziOauthPlain() {

        return consumerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addNewEnv()
                                .withName("OAUTH_CLIENT_ID")
                                .withValue(oauthClientId)
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CLIENT_SECRET")
                                .editOrNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(oauthClientSecret)
                                        .withKey("clientSecret")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_TOKEN_ENDPOINT_URI")
                                .withValue(oauthTokenEndpointUri)
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CRT")
                                .editOrNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                        .withKey(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("LOG_LEVEL")
                                .withValue("DEBUG")
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }

    public DoneableJob consumerStrimziOauthTls(String clusterName) {

        return consumerStrimziOauthPlain()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addNewEnv()
                                // disable hostname verification
                                .withName("OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM")
                                .withValue("")
                            .endEnv()
                            .addNewEnv()
                                .withName("CA_CRT")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(KafkaResources.clusterCaCertificateSecretName(clusterName))
                                        .withKey("ca.crt")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("USER_CRT")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(userName)
                                        .withKey("user.crt")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("USER_KEY")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(userName)
                                        .withKey("user.key")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }
}
