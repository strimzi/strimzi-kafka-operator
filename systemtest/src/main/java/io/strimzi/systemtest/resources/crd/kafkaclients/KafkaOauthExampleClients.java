/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd.kafkaclients;

import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.keycloak.KeycloakInstance;

import java.security.InvalidParameterException;

public class KafkaOauthExampleClients extends KafkaBasicExampleClients {

    private final String oauthClientId;
    private String oauthProducerClientId = null;
    private String oauthConsumerClientId = null;
    private final String oauthClientSecret;
    private String oauthProducerSecret = null;
    private String oauthConsumerSecret = null;
    private final String oauthTokenEndpointUri;
    private final String userName;

    public static class Builder extends KafkaBasicExampleClients.Builder {
        private String oauthClientId;
        private String oauthProducerClientId;
        private String oauthConsumerClientId;
        private String oauthClientSecret;
        private String oauthProducerSecret;
        private String oauthConsumerSecret;
        private String oauthTokenEndpointUri;
        private String userName;

        public Builder withOAuthClientId(String oauthClientId) {
            this.oauthClientId = oauthClientId;
            return this;
        }

        public Builder withOAuthProducerClientId(String oauthProducerClientId) {
            this.oauthProducerClientId = oauthProducerClientId;
            return this;
        }

        public Builder withOAuthConsumerClientId(String oauthConsumerClientId) {
            this.oauthConsumerClientId = oauthConsumerClientId;
            return this;
        }

        public Builder withOAuthClientSecret(String oauthClientSecret) {
            this.oauthClientSecret = oauthClientSecret;
            return this;
        }

        public Builder withOAuthProducerSecret(String oauthProducerSecret) {
            this.oauthProducerSecret = oauthProducerSecret;
            return this;
        }

        public Builder withOAuthConsumerSecret(String oauthConsumerSecret) {
            this.oauthConsumerSecret = oauthConsumerSecret;
            return this;
        }

        public Builder withOAuthTokenEndpointUri(String oauthTokenEndpointUri) {
            this.oauthTokenEndpointUri = oauthTokenEndpointUri;
            return this;
        }

        public Builder withUserName(String userName) {
            this.userName = userName;
            return this;
        }

        @Override
        public Builder withProducerName(String producerName) {
            return (Builder) super.withProducerName(producerName);
        }

        @Override
        public Builder withConsumerName(String consumerName) {
            return (Builder) super.withConsumerName(consumerName);
        }

        @Override
        public Builder withBootstrapAddress(String bootstrapAddress) {
            return (Builder) super.withBootstrapAddress(bootstrapAddress);
        }

        @Override
        public Builder withTopicName(String topicName) {
            return (Builder) super.withTopicName(topicName);
        }

        @Override
        public Builder withMessageCount(int messageCount) {
            return (Builder) super.withMessageCount(messageCount);
        }

        @Override
        public Builder withAdditionalConfig(String additionalConfig) {
            return (Builder) super.withAdditionalConfig(additionalConfig);
        }

        @Override
        public Builder withConsumerGroup(String consumerGroup) {
            return (Builder) super.withConsumerGroup(consumerGroup);
        }

        @Override
        public Builder withDelayMs(long delayMs) {
            return (Builder) super.withDelayMs(delayMs);
        }

        @Override
        public KafkaOauthExampleClients build() {
            return new KafkaOauthExampleClients(this);
        }
    }

    protected KafkaOauthExampleClients(KafkaOauthExampleClients.Builder builder) {
        super(builder);
        if ((builder.oauthClientId == null || builder.oauthClientId.isEmpty()) &&
                (builder.oauthConsumerClientId == null && builder.oauthProducerClientId == null)) throw new InvalidParameterException("OAuth clientId is not set.");
        if (builder.oauthClientSecret == null || builder.oauthClientSecret.isEmpty() &&
                (builder.oauthProducerSecret == null && builder.oauthConsumerSecret == null)) throw new InvalidParameterException("OAuth client secret is not set.");

        if (builder.oauthTokenEndpointUri == null || builder.oauthTokenEndpointUri.isEmpty()) throw new InvalidParameterException("OAuth token endpoint url is not set.");
        if (builder.userName == null || builder.userName.isEmpty()) builder.userName = builder.oauthClientId;

        if (builder.oauthProducerClientId != null) oauthProducerClientId = builder.oauthProducerClientId;
        if (builder.oauthProducerSecret != null) oauthProducerSecret = builder.oauthProducerSecret;
        if (builder.oauthConsumerClientId != null) oauthConsumerClientId = builder.oauthConsumerClientId;
        if (builder.oauthConsumerSecret != null) oauthConsumerSecret = builder.oauthConsumerSecret;

        oauthClientId = builder.oauthClientId;
        oauthClientSecret = builder.oauthClientSecret;
        oauthTokenEndpointUri = builder.oauthTokenEndpointUri;
        userName = builder.userName;
    }

    @Override
    protected Builder newBuilder() {
        return new Builder();
    }

    protected Builder updateBuilder(Builder builder) {
        super.updateBuilder(builder);
        return builder
            .withOAuthClientId(getOauthClientId())
            .withOAuthProducerClientId(getOauthProducerClientId())
            .withOAuthProducerSecret(getOauthProducerSecret())
            .withOAuthConsumerClientId(getOauthConsumerClientId())
            .withOAuthConsumerSecret(getOauthConsumerSecret())
            .withOAuthClientSecret(getOauthClientSecret())
            .withOAuthTokenEndpointUri(getOauthTokenEndpointUri())
            .withUserName(getClientUserName());
    }

    @Override
    public Builder toBuilder() {
        return updateBuilder(newBuilder());
    }

    public String getOauthClientId() {
        return oauthClientId;
    }

    public String getOauthProducerClientId() {
        return oauthProducerClientId;
    }

    public String getOauthConsumerClientId() {
        return oauthConsumerClientId;
    }

    public String getOauthProducerSecret() {
        return oauthProducerSecret;
    }

    public String getOauthConsumerSecret() {
        return oauthConsumerSecret;
    }

    public String getOauthClientSecret() {
        return oauthClientSecret;
    }

    public String getOauthTokenEndpointUri() {
        return oauthTokenEndpointUri;
    }

    public String getClientUserName() {
        return userName;
    }

    public JobBuilder producerStrimziOauthPlain() {
        return defaultProducerStrimziOauthPlain();
    }

    private JobBuilder defaultProducerStrimziOauthPlain() {

        return defaultProducerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addNewEnv()
                                .withName("OAUTH_CLIENT_ID")
                                .withValue(oauthProducerClientId != null ? oauthProducerClientId : oauthClientId)
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CLIENT_SECRET")
                                .editOrNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(oauthProducerSecret != null ? oauthProducerSecret : oauthClientSecret)
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

    public JobBuilder producerStrimziOauthTls(String clusterName) {

        return defaultProducerStrimziOauthPlain()
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
                                        .withName(oauthProducerClientId != null ? oauthProducerClientId : userName)
                                        .withKey("user.crt")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("USER_KEY")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(oauthProducerClientId != null ? oauthProducerClientId : userName)
                                        .withKey("user.key")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }

    public JobBuilder consumerStrimziOauthPlain() {
        return defaultConsumerStrimziOauth();
    }

    private JobBuilder defaultConsumerStrimziOauth() {

        return defaultConsumerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addNewEnv()
                                .withName("OAUTH_CLIENT_ID")
                                .withValue(oauthConsumerClientId != null ? oauthConsumerClientId : oauthClientId)
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CLIENT_SECRET")
                                .editOrNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(oauthConsumerSecret != null ? oauthConsumerSecret : oauthClientSecret)
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

    public JobBuilder consumerStrimziOauthTls(String clusterName) {

        return defaultConsumerStrimziOauth()
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
                                        .withName(oauthConsumerClientId != null ? oauthConsumerClientId : userName)
                                        .withKey("user.crt")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("USER_KEY")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(oauthConsumerClientId != null ? oauthConsumerClientId : userName)
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
