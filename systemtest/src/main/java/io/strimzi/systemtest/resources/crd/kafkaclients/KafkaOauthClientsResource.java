/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd.kafkaclients;

import io.fabric8.kubernetes.api.model.batch.DoneableJob;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.keycloak.KeycloakInstance;

public class KafkaOauthClientsResource extends KafkaBasicClientResource {

    private final String oauthClientId;
    private final String oauthClientSecret;
    private final String oauthTokenEndpointUri;
    private final String userName;

    public KafkaOauthClientsResource(
        String producerName, String consumerName, String bootstrapServer, String topicName, int messageCount,
        String additionalConfig, String consumerGroup, String oauthClientId, String oauthClientSecret, String oauthTokenEndpointUri) {

        super(producerName, consumerName, bootstrapServer, topicName, messageCount, additionalConfig, consumerGroup, 0);
        this.oauthClientId = oauthClientId;
        this.oauthClientSecret = oauthClientSecret;
        this.oauthTokenEndpointUri = oauthTokenEndpointUri;
        this.userName = oauthClientId;
    }

    // from existing client create new client with random consumer group (immutability)
    public KafkaOauthClientsResource(KafkaOauthClientsResource kafkaOauthClientsResource) {
        super(kafkaOauthClientsResource);
        this.oauthClientId = kafkaOauthClientsResource.oauthClientId;
        this.oauthClientSecret = kafkaOauthClientsResource.oauthClientSecret;
        this.oauthTokenEndpointUri = kafkaOauthClientsResource.oauthTokenEndpointUri;
        this.userName = kafkaOauthClientsResource.userName;

    }

    // from existing client create new client with new specific consumer group and topicName (immutability)
    public KafkaOauthClientsResource(KafkaOauthClientsResource kafkaOauthClientsResource, String topicName, String consumerGroup) {
        super(kafkaOauthClientsResource, topicName, consumerGroup);
        this.oauthClientId = kafkaOauthClientsResource.oauthClientId;
        this.oauthClientSecret = kafkaOauthClientsResource.oauthClientSecret;
        this.oauthTokenEndpointUri = kafkaOauthClientsResource.oauthTokenEndpointUri;
        this.userName = kafkaOauthClientsResource.userName;

    }

    // from existing client create new username (immutability)
    public KafkaOauthClientsResource(KafkaOauthClientsResource kafkaOauthClientsResource, String userName) {
        super(kafkaOauthClientsResource);
        this.oauthClientId = kafkaOauthClientsResource.oauthClientId;
        this.oauthClientSecret = kafkaOauthClientsResource.oauthClientSecret;
        this.oauthTokenEndpointUri = kafkaOauthClientsResource.oauthTokenEndpointUri;
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
