/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.oauth;

import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaAuthorizationKeycloak;
import io.strimzi.api.kafka.model.KafkaAuthorizationKeycloakBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePortBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.externalClients.OauthExternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.test.WaitException;
import io.vertx.core.cli.annotations.Description;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(OAUTH)
@Tag(REGRESSION)
@Tag(NODEPORT_SUPPORTED)
@Tag(EXTERNAL_CLIENTS_USED)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OauthAuthorizationST extends OauthBaseST {

    private OauthExternalKafkaClient teamAOauthKafkaClient;
    private OauthExternalKafkaClient teamBOauthKafkaClient;

    private static final String TEAM_A_CLIENT = "team-a-client";
    private static final String TEAM_B_CLIENT = "team-b-client";
    private static final String KAFKA_CLIENT_ID = "kafka";

    private static final String TEAM_A_CLIENT_SECRET = "team-a-client-secret";
    private static final String TEAM_B_CLIENT_SECRET = "team-b-client-secret";

    private static final String TOPIC_A = "a-topic";
    private static final String TOPIC_B = "b-topic";
    private static final String TOPIC_X = "x-topic";

    @Description("As a member of team A, I should be able to read and write to all topics starting with a-")
    @Test
    @Order(1)
    void smokeTestForClients() {
        teamAOauthKafkaClient.verifyProducedAndConsumedMessages(
            teamAOauthKafkaClient.sendMessagesTls(),
            teamAOauthKafkaClient.receiveMessagesTls()
        );
    }

    @Description("As a member of team A, I should be able to write to topics that starts with x- on any cluster and " +
            "and should also write and read to topics starting with 'a-'")
    @Test
    @Order(2)
    void testTeamAWriteToTopic() {
        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_NAME);

        LOGGER.info("Producer will not produce messages because authorization topic will failed. Team A can write only to topic starting with 'x-'");

        teamAOauthKafkaClient.setTopicName(TOPIC_NAME);

        assertThrows(WaitException.class, () -> teamAOauthKafkaClient.sendMessagesTls(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT));

        String topicXName = TOPIC_X + "-example-1";
        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, topicXName);

        // Team A can not create topic starting with 'x-' only write to existing on
        KafkaTopicResource.topic(CLUSTER_NAME, topicXName).done();

        teamAOauthKafkaClient.setTopicName(topicXName);

        assertThat(teamAOauthKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_A);

        teamAOauthKafkaClient.setTopicName(TOPIC_A);

        assertThat(teamAOauthKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));
    }

    @Description("As a member of team A, I should be able only read from consumer that starts with a_")
    @Test
    @Order(3)
    void testTeamAReadFromTopic() {
        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_A);

        assertThat(teamAOauthKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        teamAOauthKafkaClient.setConsumerGroup("bad_consumer_group");

        assertThrows(WaitException.class, () -> teamAOauthKafkaClient.receiveMessagesTls(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT));

        teamAOauthKafkaClient.setConsumerGroup("a_correct_consumer_group");

        assertThat(teamAOauthKafkaClient.receiveMessagesTls(), is(MESSAGE_COUNT));
    }

    @Description("As a member of team B, I should be able to write and read from topics that starts with b-")
    @Test
    @Order(4)
    void testTeamBWriteToTopic() {
        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_NAME);

        // Producer will not produce messages because authorization topic will failed. Team A can write only to topic starting with 'x-'
        assertThrows(WaitException.class, () -> teamBOauthKafkaClient.sendMessagesTls(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT));

        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_B);
        teamBOauthKafkaClient.setTopicName(TOPIC_B);

        teamBOauthKafkaClient.verifyProducedAndConsumedMessages(
            teamBOauthKafkaClient.sendMessagesTls(),
            teamBOauthKafkaClient.receiveMessagesTls()
        );
    }

    @Disabled("Will be fixed in the new PR.")
    @Description("As a member of team A, I can write to topics starting with 'x-' and " +
            "as a member of team B can read from topics starting with 'x-'")
    @Test
    @Order(5)
    void testTeamAWriteToTopicStartingWithXAndTeamBReadFromTopicStartingWithX() {
        // only write means that Team A can not create new topic 'x-.*'
        String topicName = TOPIC_X + "-example";

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        teamAOauthKafkaClient.setTopicName(topicName);

        assertThat(teamAOauthKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        teamBOauthKafkaClient.setTopicName(topicName);
        teamBOauthKafkaClient.setConsumerGroup("x_consumer_group_b");

        assertThat(teamBOauthKafkaClient.receiveMessagesTls(), is(MESSAGE_COUNT));
    }

    @Description("As a superuser of team A and team B, i am able to break defined authorization rules")
    @Test
    void testSuperUserWithOauthAuthorization() {

        LOGGER.info("Verifying that team B is not able write to topic starting with 'x-' because in kafka cluster" +
                "does not have super-users to break authorization rules");

        teamBOauthKafkaClient.setTopicName(TOPIC_X);
        teamBOauthKafkaClient.setKafkaUsername(USER_NAME);

        assertThrows(WaitException.class, () -> teamBOauthKafkaClient.sendMessagesTls(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT));

        LOGGER.info("Verifying that team A is not able read to topic starting with 'x-' because in kafka cluster" +
                "does not have super-users to break authorization rules");

        teamAOauthKafkaClient.setTopicName(TOPIC_X);
        teamAOauthKafkaClient.setKafkaUsername(USER_NAME);
        teamAOauthKafkaClient.setConsumerGroup("x_consumer_group_b1");

        assertThrows(Exception.class, () -> teamAOauthKafkaClient.receiveMessagesTls(Constants.GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT));

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {

            List<String> superUsers = new ArrayList<>(2);
            superUsers.add("service-account-" + TEAM_A_CLIENT);
            superUsers.add("service-account-" + TEAM_B_CLIENT);

            ((KafkaAuthorizationKeycloak) kafka.getSpec().getKafka().getAuthorization()).setSuperUsers(superUsers);
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME).done();
        KafkaUserUtils.waitForKafkaUserCreation(USER_NAME);

        LOGGER.info("Verifying that team B is able to write to topic starting with 'x-' and break authorization rule");

        assertThat(teamBOauthKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        teamAOauthKafkaClient.setConsumerGroup("x_consumer_group_b2");

        LOGGER.info("Verifying that team A is able to write to topic starting with 'x-' and break authorization rule");

        assertThat(teamAOauthKafkaClient.receiveMessagesTls(), is(MESSAGE_COUNT));
    }

    @Disabled("Will be implemented in next PR")
    @Test
    @Order(6)
    void testListTopics() {
        // TODO: in the new PR add AdminClient support with operations listTopics(), etc.
    }

    @Disabled("Will be implemented in next PR")
    @Test
    @Order(7)
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

        KafkaUserResource.tlsUser(CLUSTER_NAME, TEAM_A_CLIENT).done();
        KafkaUserResource.tlsUser(CLUSTER_NAME, TEAM_B_CLIENT).done();

        KafkaUserUtils.waitForKafkaUserCreation(TEAM_A_CLIENT);
        KafkaUserUtils.waitForKafkaUserCreation(TEAM_B_CLIENT);

        teamAOauthKafkaClient = new OauthExternalKafkaClient.Builder()
            .withTopicName(TOPIC_A)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(TEAM_A_CLIENT)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SASL_SSL)
            .withConsumerGroupName("a_consumer_group")
            .withOauthClientId(TEAM_A_CLIENT)
            .withClientSecretName(TEAM_A_CLIENT_SECRET)
            .withOauthTokenEndpointUri(oauthTokenEndpointUri)
            .build();

        teamBOauthKafkaClient = new OauthExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(TEAM_A_CLIENT)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SASL_SSL)
            .withConsumerGroupName("x_" + CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .withOauthClientId(TEAM_B_CLIENT)
            .withClientSecretName(TEAM_B_CLIENT_SECRET)
            .withOauthTokenEndpointUri(oauthTokenEndpointUri)
            .build();

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

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

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
    }
}
