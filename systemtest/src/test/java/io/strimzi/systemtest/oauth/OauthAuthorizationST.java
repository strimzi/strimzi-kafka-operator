/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.oauth;

import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaAuthorizationKeycloakBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePortBuilder;
import io.strimzi.systemtest.kafkaclients.ClientFactory;
import io.strimzi.systemtest.kafkaclients.EClientType;
import io.strimzi.systemtest.kafkaclients.externalClients.OauthKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.vertx.core.cli.annotations.Description;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.KeyStoreException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(OAUTH)
@Tag(REGRESSION)
@Tag(NODEPORT_SUPPORTED)
public class OauthAuthorizationST extends OauthBaseST {

    private OauthKafkaClient teamAOauthKafkaClient = (OauthKafkaClient) ClientFactory.getClient(EClientType.OAUTH);
    private OauthKafkaClient teamBOauthKafkaClient = (OauthKafkaClient) ClientFactory.getClient(EClientType.OAUTH);

    private static final String TEAM_A_CLIENT = "team-a-client";
    private static final String TEAM_B_CLIENT = "team-b-client";
    private static final String KAFKA_CLIENT_ID = "kafka";

    private static final String TEAM_A_CLIENT_SECRET = "team-a-client-secret";
    private static final String TEAM_B_CLIENT_SECRET = "team-b-client-secret";

    private static final int MESSAGE_COUNT = 100;
    private static final int TIMEOUT_SEND_RECV_MESSAGES = 10;

    private static final String TOPIC_A = "a-topic";
    private static final String TOPIC_B = "b-topic";
    private static final String TOPIC_X = "x-topic";


    @Description("As a member of team A, I should be able to read and write to all topics starting with a-")
    @Test
    void smokeTestForClients() throws IOException, KeyStoreException, InterruptedException, ExecutionException, TimeoutException {
        Future<Integer> producer = teamAOauthKafkaClient.sendMessagesTls(TOPIC_A, NAMESPACE, CLUSTER_NAME, TEAM_A_CLIENT,
                MESSAGE_COUNT, "SSL");

        Future<Integer> consumer = teamAOauthKafkaClient.receiveMessagesTls(TOPIC_A, NAMESPACE, CLUSTER_NAME, TEAM_A_CLIENT,
                MESSAGE_COUNT, "SSL", "a_consumer_group");

        assertThat(producer.get(2, TimeUnit.MINUTES), is(MESSAGE_COUNT));
        assertThat(consumer.get(2, TimeUnit.MINUTES), is(MESSAGE_COUNT));
    }

    @Description("As a member of team A, I should be able to write to topics that starts with x- on any cluster and " +
            "and should also write and read to topics starting with 'a-'")
    @Test
    void testTeamAWriteToTopic() throws IOException, KeyStoreException, InterruptedException, ExecutionException, TimeoutException {
        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_NAME);

        LOGGER.info("Producer will not produce messages because authorization topic will failed. Team A can write only to topic starting with 'x-'");

        assertThrows(Exception.class, () -> {
            Future<Integer> invalidProducer = teamAOauthKafkaClient.sendMessagesTls(TOPIC_NAME, NAMESPACE,
                    CLUSTER_NAME, TEAM_A_CLIENT, MESSAGE_COUNT, "SSL", TIMEOUT_SEND_RECV_MESSAGES);
            invalidProducer.get(TIMEOUT_SEND_RECV_MESSAGES, TimeUnit.SECONDS);
        });

        String topicXName = TOPIC_X + "-example-1";
        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, topicXName);

        // Team A can not create topic starting with 'x-' only write to existing on
        KafkaTopicResource.topic(CLUSTER_NAME, topicXName).done();

        Future<Integer> producer = teamAOauthKafkaClient.sendMessagesTls(topicXName, NAMESPACE, CLUSTER_NAME, TEAM_A_CLIENT,
                MESSAGE_COUNT, "SSL");

        assertThat(producer.get(1, TimeUnit.MINUTES), is(MESSAGE_COUNT));

        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_A);

        producer = teamAOauthKafkaClient.sendMessagesTls(TOPIC_A, NAMESPACE, CLUSTER_NAME, TEAM_A_CLIENT,
                MESSAGE_COUNT, "SSL");

        assertThat(producer.get(1, TimeUnit.MINUTES), is(MESSAGE_COUNT));
    }

    @Description("As a member of team A, I should be able only read from consumer that starts with a_")
    @Test
    void testTeamAReadFromTopic() throws IOException, KeyStoreException, InterruptedException, ExecutionException, TimeoutException {
        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_A);

        Future<Integer> producer = teamAOauthKafkaClient.sendMessagesTls(TOPIC_A, NAMESPACE, CLUSTER_NAME, TEAM_A_CLIENT,
                MESSAGE_COUNT, "SSL");

        assertThat(producer.get(1, TimeUnit.MINUTES), is(MESSAGE_COUNT));

        assertThrows(Exception.class, () -> {
            Future<Integer> invalidConsumer = teamAOauthKafkaClient.receiveMessagesTls(TOPIC_A, NAMESPACE, CLUSTER_NAME,
                    TEAM_A_CLIENT, MESSAGE_COUNT, "SSL", "bad_consumer_group", TIMEOUT_SEND_RECV_MESSAGES);
            invalidConsumer.get(TIMEOUT_SEND_RECV_MESSAGES, TimeUnit.SECONDS);
        });

        Future<Integer> consumerWithCorrectConsumerGroup = teamAOauthKafkaClient.receiveMessagesTls(TOPIC_A, NAMESPACE, CLUSTER_NAME, TEAM_A_CLIENT,
                MESSAGE_COUNT, "SSL", "a_correct_consumer_group");

        assertThat(consumerWithCorrectConsumerGroup.get(2, TimeUnit.MINUTES), is(MESSAGE_COUNT));

    }

    @Description("As a member of team B, I should be able to write and read from topics that starts with b-")
    @Test
    void testTeamBWriteToTopic() throws IOException, KeyStoreException, ExecutionException, InterruptedException, TimeoutException {
        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_NAME);

        // Producer will not produce messages because authorization topic will failed. Team A can write only to topic starting with 'x-'
        assertThrows(Exception.class, () -> {
            Future<Integer> invalidProducer = teamBOauthKafkaClient.sendMessagesTls(TOPIC_NAME, NAMESPACE, CLUSTER_NAME,
                    TEAM_B_CLIENT, MESSAGE_COUNT, "SSL", TIMEOUT_SEND_RECV_MESSAGES);
            invalidProducer.get(TIMEOUT_SEND_RECV_MESSAGES, TimeUnit.SECONDS);
        });

        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_B);

        Future<Integer> producer = teamBOauthKafkaClient.sendMessagesTls(TOPIC_B, NAMESPACE, CLUSTER_NAME, TEAM_B_CLIENT,
                MESSAGE_COUNT, "SSL");

        assertThat(producer.get(2, TimeUnit.MINUTES), is(MESSAGE_COUNT));

        Future<Integer> consumer = teamBOauthKafkaClient.receiveMessagesTls(TOPIC_B, NAMESPACE, CLUSTER_NAME, TEAM_B_CLIENT,
                MESSAGE_COUNT, "SSL", "x_" + CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        assertThat(consumer.get(2, TimeUnit.MINUTES), is(MESSAGE_COUNT));
    }

    @Description("As a member of team A, I can write to topics starting with 'x-' and " +
            "as a member of team B can read from topics starting with 'x-'")
    @Test
    void testTeamAWriteToTopicStartingWithXAndTeamBReadFromTopicStartingWithX() throws IOException, KeyStoreException, InterruptedException, ExecutionException, TimeoutException {
        // only write means that Team A can not create new topic 'x-.*'
        String topicName = TOPIC_X + "-example";

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        Future<Integer> producer = teamAOauthKafkaClient.sendMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, TEAM_A_CLIENT,
                MESSAGE_COUNT, "SSL");

        assertThat(producer.get(2, TimeUnit.MINUTES), is(MESSAGE_COUNT));

        Future<Integer> consumer = teamBOauthKafkaClient.receiveMessagesTls(topicName, NAMESPACE, CLUSTER_NAME, TEAM_B_CLIENT,
                MESSAGE_COUNT, "SSL", "x_consumer_group_b");

        assertThat(consumer.get(2, TimeUnit.MINUTES), is(MESSAGE_COUNT));
    }

    @Disabled("Will be implemented in next PR")
    @Test
    void testListTopics() {
        // TODO: in the new PR add AdminClient support with operations listTopics(), etc.
    }

    @Disabled("Will be implemented in next PR")
    @Test
    void testClusterVerification() {
        // TODO: create more examples via cluster wide stuff
    }

    @BeforeAll
    void setUp()  {
        LOGGER.info("Replacing validIssuerUri: {} to pointing to kafka-authz realm", validIssuerUri);
        LOGGER.info("Replacing jwksEndpointUri: {} to pointing to kafka-authz realm", jwksEndpointUri);
        LOGGER.info("Replacing oauthTokenEndpointUri: {} to pointing to kafka-authz realm", oauthTokenEndpointUri);

        validIssuerUri = "https://" + keycloakIpWithPortHttps + "/auth/realms/kafka-authz";
        jwksEndpointUri = "https://" + keycloakIpWithPortHttps + "/auth/realms/kafka-authz/protocol/openid-connect/certs";
        oauthTokenEndpointUri = "https://" + keycloakIpWithPortHttps + "/auth/realms/kafka-authz/protocol/openid-connect/token";

        LOGGER.info("Setting producer and consumer properties");

        teamAOauthKafkaClient.setClientId(TEAM_A_CLIENT);
        teamAOauthKafkaClient.setClientSecretName(TEAM_A_CLIENT_SECRET);
        teamAOauthKafkaClient.setOauthTokenEndpointUri(oauthTokenEndpointUri);

        teamBOauthKafkaClient.setClientId(TEAM_B_CLIENT);
        teamBOauthKafkaClient.setClientSecretName(TEAM_B_CLIENT_SECRET);
        teamBOauthKafkaClient.setOauthTokenEndpointUri(oauthTokenEndpointUri);

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().getListeners().setExternal(
                new KafkaListenerExternalNodePortBuilder()
                    .withNewKafkaListenerAuthenticationOAuth()
                        .withValidIssuerUri(validIssuerUri)
                        .withJwksEndpointUri(jwksEndpointUri)
                        .withJwksExpirySeconds(JWKS_EXPIRE_SECONDS)
                        .withJwksRefreshSeconds(JWKS_REFRESH_SECONDS)
                        .withUserNameClaim(userNameClaim)
                        .withTlsTrustedCertificates(
                            new CertSecretSourceBuilder()
                                .withSecretName(SECRET_OF_KEYCLOAK)
                                .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                                .build())
                        .withDisableTlsHostnameVerification(true)
                    .endKafkaListenerAuthenticationOAuth()
                    .build());

            kafka.getSpec().getKafka().setAuthorization(
                    new KafkaAuthorizationKeycloakBuilder()
                        .withClientId(KAFKA_CLIENT_ID)
                        .withDisableTlsHostnameVerification(true)
                        .withDelegateToKafkaAcls(false)
                            // ca.crt a tls.crt
                        .withTlsTrustedCertificates(
                            new CertSecretSourceBuilder()
                                .withSecretName(SECRET_OF_KEYCLOAK)
                                .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                                .build()
                        )
                        .withTokenEndpointUri(oauthTokenEndpointUri)
                    .build());

        });

        KafkaUserResource.tlsUser(CLUSTER_NAME, TEAM_A_CLIENT).done();
        KafkaUserResource.tlsUser(CLUSTER_NAME, TEAM_B_CLIENT).done();

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);
    }
}
