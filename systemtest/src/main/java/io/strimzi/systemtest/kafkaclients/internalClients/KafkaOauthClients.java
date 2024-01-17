/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.sundr.builder.annotations.Buildable;

import java.security.InvalidParameterException;

@Buildable(editableEnabled = false)
public class KafkaOauthClients extends KafkaClients {
    private String oauthClientId;
    private String oauthProducerClientId;
    private String oauthConsumerClientId;
    private String oauthClientSecret;
    private String oauthProducerSecret;
    private String oauthConsumerSecret;
    private String oauthTokenEndpointUri;
    private String clientUserName;

    public String getOauthClientId() {
        return oauthClientId;
    }

    public void setOauthClientId(String oauthClientId) {
        this.oauthClientId = oauthClientId;
    }

    public String getOauthProducerClientId() {
        return oauthProducerClientId;
    }

    public void setOauthProducerClientId(String oauthProducerClientId) {
        this.oauthProducerClientId = oauthProducerClientId;
    }

    public String getOauthConsumerClientId() {
        return oauthConsumerClientId;
    }

    public void setOauthConsumerClientId(String oauthConsumerClientId) {
        this.oauthConsumerClientId = oauthConsumerClientId;
    }

    public String getOauthProducerSecret() {
        return oauthProducerSecret;
    }

    public void setOauthProducerSecret(String oauthProducerSecret) {
        this.oauthProducerSecret = oauthProducerSecret;
    }

    public String getOauthConsumerSecret() {
        return oauthConsumerSecret;
    }

    public void setOauthConsumerSecret(String oauthConsumerSecret) {
        this.oauthConsumerSecret = oauthConsumerSecret;
    }

    public String getOauthClientSecret() {
        return oauthClientSecret;
    }

    public void setOauthClientSecret(String oauthClientSecret) {
        this.oauthClientSecret = oauthClientSecret;
    }

    public String getOauthTokenEndpointUri() {
        return oauthTokenEndpointUri;
    }

    public void setOauthTokenEndpointUri(String oauthTokenEndpointUri) {
        if (oauthTokenEndpointUri == null || oauthTokenEndpointUri.isEmpty()) {
            throw new InvalidParameterException("OAuth token endpoint url is not set.");
        }
        this.oauthTokenEndpointUri = oauthTokenEndpointUri;
    }

    public String getClientUserName() {
        return clientUserName;
    }

    public void setClientUserName(String clientUserName) {
        this.clientUserName = clientUserName;
    }

    public Job producerStrimziOauthPlain() {
        return defaultProducerStrimziOauthPlain().build();
    }

    private JobBuilder defaultProducerStrimziOauthPlain() {
        checkParameters();
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
                                .withName("OAUTH_SSL_TRUSTSTORE_CERTIFICATES")
                                .editOrNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                        .withKey(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_SSL_TRUSTSTORE_TYPE")
                                .withValue("PEM")
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }

    public Job producerStrimziOauthTls(String clusterName) {

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
                                        .withName(oauthProducerClientId != null ? oauthProducerClientId : clientUserName)
                                        .withKey("user.crt")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("USER_KEY")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(oauthProducerClientId != null ? oauthProducerClientId : clientUserName)
                                        .withKey("user.key")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }

    public Job consumerStrimziOauthPlain() {
        return defaultConsumerStrimziOauth().build();
    }

    private JobBuilder defaultConsumerStrimziOauth() {
        checkParameters();
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
                                .withName("OAUTH_SSL_TRUSTSTORE_CERTIFICATES")
                                .editOrNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                        .withKey(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_SSL_TRUSTSTORE_TYPE")
                                .withValue("PEM")
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

    public Job consumerStrimziOauthTls(String clusterName) {

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
                                        .withName(oauthConsumerClientId != null ? oauthConsumerClientId : clientUserName)
                                        .withKey("user.crt")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("USER_KEY")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName(oauthConsumerClientId != null ? oauthConsumerClientId : clientUserName)
                                        .withKey("user.key")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }

    private void checkParameters() {
        if ((this.getOauthClientId() == null || this.getOauthClientId().isEmpty()) &&
            (this.getOauthConsumerClientId() == null && this.getOauthProducerClientId() == null)) {
            throw new InvalidParameterException("OAuth clientId is not set.");
        }
        if (this.getOauthClientSecret() == null || this.getOauthClientSecret().isEmpty() &&
            (this.getOauthProducerSecret() == null && this.getOauthConsumerSecret() == null)) {
            throw new InvalidParameterException("OAuth client Secret is not set.");
        }
        if (this.getClientUserName() == null || this.getClientUserName().isEmpty()) {
            this.setClientUserName(this.getOauthClientId());
        }
    }
}
