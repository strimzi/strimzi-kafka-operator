/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd.kafkaclients;

import io.fabric8.kubernetes.api.model.batch.DoneableJob;

public class KafkaOauthClientsResource extends KafkaBasicClientResource {

    final String oauthClientId;
    final String oauthClientSecret;
    final String oauthTokenEndpointUri;

    public KafkaOauthClientsResource(
        String producerName, String consumerName, String bootstrapServer, String topicName, int messageCount,
        String additionalConfig, String consumerGroup, String oauthClientId, String oauthClientSecret, String oauthTokenEndpointUri) {

        super(producerName, consumerName, bootstrapServer, topicName, messageCount, additionalConfig, consumerGroup);
        this.oauthClientId = oauthClientId;
        this.oauthClientSecret = oauthClientSecret;
        this.oauthTokenEndpointUri = oauthTokenEndpointUri;
    }

    // from existing client create new client with different consumer group (immutability)
    public KafkaOauthClientsResource(KafkaOauthClientsResource kafkaOauthClientsResource) {
        super(kafkaOauthClientsResource);
        this.oauthClientId = kafkaOauthClientsResource.oauthClientId;
        this.oauthClientSecret = kafkaOauthClientsResource.oauthClientSecret;
        this.oauthTokenEndpointUri =kafkaOauthClientsResource.oauthTokenEndpointUri;
    }

    public DoneableJob producerStrimziOauth() {

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
                                        .withName("x509-https-secret")
                                        .withKey("tls.crt")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }

    public DoneableJob consumerStrimziOauth() {

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
                                        .withName("x509-https-secret")
                                        .withKey("tls.crt")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }
}
