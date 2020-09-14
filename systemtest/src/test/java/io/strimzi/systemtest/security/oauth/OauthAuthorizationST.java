/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaAuthorizationKeycloak;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaOauthExampleClients;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.test.WaitException;
import io.vertx.core.cli.annotations.Description;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(OAUTH)
@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OauthAuthorizationST extends OauthAbstractST {
    protected static final Logger LOGGER = LogManager.getLogger(OauthAuthorizationST.class);

    private KafkaOauthExampleClients teamAOauthClientJob;
    private KafkaOauthExampleClients teamBOauthClientJob;

    private static final String TEAM_A_CLIENT = "team-a-client";
    private static final String TEAM_B_CLIENT = "team-b-client";
    private static final String KAFKA_CLIENT_ID = "kafka";

    private static final String TEAM_A_CLIENT_SECRET = "team-a-client-secret";
    private static final String TEAM_B_CLIENT_SECRET = "team-b-client-secret";

    private static final String TOPIC_A = "a-topic";
    private static final String TOPIC_B = "b-topic";
    private static final String TOPIC_X = "x-topic";

    private static final String TEAM_A_PRODUCER_NAME = TEAM_A_CLIENT + "-producer";
    private static final String TEAM_A_CONSUMER_NAME = TEAM_A_CLIENT + "-consumer";
    private static final String TEAM_B_PRODUCER_NAME = TEAM_B_CLIENT + "-producer";
    private static final String TEAM_B_CONSUMER_NAME = TEAM_B_CLIENT + "-consumer";

    @Description("As a member of team A, I should be able to read and write to all topics starting with a-")
    @Test
    @Order(1)
    void smokeTestForClients() {
        teamAOauthClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(TEAM_A_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);
        teamAOauthClientJob.consumerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(TEAM_A_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As a member of team A, I should be able to write to topics that starts with x- on any cluster and " +
            "and should also write and read to topics starting with 'a-'")
    @Test
    @Order(2)
    void testTeamAWriteToTopic() {
        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_NAME);
        LOGGER.info("Producer will not produce messages because authorization topic will failed. Team A can write only to topic starting with 'x-'");

        teamAOauthClientJob.setConsumerGroup("a-consumer_group");
        teamAOauthClientJob.setTopicName(TOPIC_NAME);

        teamAOauthClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        assertThrows(WaitException.class, () -> JobUtils.waitForJobFailure(TEAM_A_PRODUCER_NAME, NAMESPACE, 30_000));
        JobUtils.deleteJobWithWait(NAMESPACE, TEAM_A_PRODUCER_NAME);

        String topicXName = TOPIC_X + "-example-1";
        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, topicXName);

        teamAOauthClientJob.setConsumerGroup("a-consumer_group");
        teamAOauthClientJob.setTopicName(topicXName);

        teamAOauthClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        assertThrows(WaitException.class, () -> JobUtils.waitForJobFailure(TEAM_A_PRODUCER_NAME, NAMESPACE, 30_000));
        JobUtils.deleteJobWithWait(NAMESPACE, TEAM_A_PRODUCER_NAME);

        // Team A can not create topic starting with 'x-' only write to existing on
        KafkaTopicResource.topic(CLUSTER_NAME, topicXName).done();
        teamAOauthClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(TEAM_A_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, TEAM_A_PRODUCER_NAME);

        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_A);

        teamAOauthClientJob.setConsumerGroup("a-consumer_group");
        teamAOauthClientJob.setTopicName(TOPIC_A);

        teamAOauthClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(TEAM_A_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As a member of team A, I should be able only read from consumer that starts with a_")
    @Test
    @Order(3)
    void testTeamAReadFromTopic() {
        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_A);
        teamAOauthClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(TEAM_A_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, TEAM_A_PRODUCER_NAME);

        // team A client shouldn't be able to consume messages with wrong consumer group

        teamAOauthClientJob.setConsumerGroup("bad_consumer_group");
        teamAOauthClientJob.setTopicName(TOPIC_A);

        teamAOauthClientJob.consumerStrimziOauthTls(CLUSTER_NAME).done();
        assertThrows(WaitException.class, () -> JobUtils.waitForJobFailure(TEAM_A_CONSUMER_NAME, NAMESPACE, 30_000));
        JobUtils.deleteJobWithWait(NAMESPACE, TEAM_A_PRODUCER_NAME);

        // team A client should be able to consume messages with correct consumer group

        teamAOauthClientJob.setConsumerGroup("a-correct_consumer_group");
        teamAOauthClientJob.setTopicName(TOPIC_A);

        teamAOauthClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(TEAM_A_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As a member of team B, I should be able to write and read from topics that starts with b-")
    @Test
    @Order(4)
    void testTeamBWriteToTopic() {
        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_NAME);
        // Producer will not produce messages because authorization topic will failed. Team A can write only to topic starting with 'x-'
        teamBOauthClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        assertThrows(WaitException.class, () -> JobUtils.waitForJobFailure(TEAM_B_PRODUCER_NAME, NAMESPACE, 30_000));
        JobUtils.deleteJobWithWait(NAMESPACE, TEAM_B_PRODUCER_NAME);

        LOGGER.info("Sending {} messages to broker with topic name {}", MESSAGE_COUNT, TOPIC_B);

        teamBOauthClientJob.setTopicName(TOPIC_B);
        teamBOauthClientJob.setConsumerGroup("x-consumer_group_b");

        teamBOauthClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        teamBOauthClientJob.consumerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitTillContinuousClientsFinish(TEAM_B_PRODUCER_NAME, TEAM_B_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As a member of team A, I can write to topics starting with 'x-' and " +
            "as a member of team B can read from topics starting with 'x-'")
    @Test
    @Order(5)
    void testTeamAWriteToTopicStartingWithXAndTeamBReadFromTopicStartingWithX() {
        // only write means that Team A can not create new topic 'x-.*'
        String topicName = TOPIC_X + "-example";
        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        teamAOauthClientJob.setConsumerGroup("a-consumer_group");
        teamAOauthClientJob.setTopicName(topicName);
        
        teamAOauthClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(TEAM_A_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);

        teamBOauthClientJob.setConsumerGroup("x-consumer_group_b");
        teamBOauthClientJob.setTopicName(topicName);

        teamBOauthClientJob.consumerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(TEAM_B_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);
    }

    @Description("As a superuser of team A and team B, i am able to break defined authorization rules")
    @Test
    @Order(6)
    void testSuperUserWithOauthAuthorization() {

        LOGGER.info("Verifying that team B is not able write to topic starting with 'x-' because in kafka cluster" +
                "does not have super-users to break authorization rules");

        KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME).done();

        teamBOauthClientJob.setConsumerGroup("x-consumer_group_b");
        teamBOauthClientJob.setTopicName(TOPIC_X);
        teamBOauthClientJob.setUserName(USER_NAME);

        teamBOauthClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        assertThrows(WaitException.class, () -> JobUtils.waitForJobFailure(TEAM_B_PRODUCER_NAME, NAMESPACE, 30_000));
        JobUtils.deleteJobWithWait(NAMESPACE, TEAM_B_PRODUCER_NAME);

        LOGGER.info("Verifying that team A is not able read to topic starting with 'x-' because in kafka cluster" +
                "does not have super-users to break authorization rules");

        teamAOauthClientJob.setConsumerGroup("x-consumer_group_b1");
        teamAOauthClientJob.setTopicName(TOPIC_X);
        teamAOauthClientJob.setUserName(USER_NAME);

        teamAOauthClientJob.consumerStrimziOauthTls(CLUSTER_NAME).done();
        assertThrows(WaitException.class, () -> JobUtils.waitForJobFailure(TEAM_A_CONSUMER_NAME, NAMESPACE, 30_000));
        JobUtils.deleteJobWithWait(NAMESPACE, TEAM_A_CONSUMER_NAME);

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {

            List<String> superUsers = new ArrayList<>(2);
            superUsers.add("service-account-" + TEAM_A_CLIENT);
            superUsers.add("service-account-" + TEAM_B_CLIENT);

            ((KafkaAuthorizationKeycloak) kafka.getSpec().getKafka().getAuthorization()).setSuperUsers(superUsers);
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        LOGGER.info("Verifying that team B is able to write to topic starting with 'x-' and break authorization rule");

        teamBOauthClientJob.producerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(TEAM_B_PRODUCER_NAME, NAMESPACE, MESSAGE_COUNT);

        LOGGER.info("Verifying that team A is able to write to topic starting with 'x-' and break authorization rule");

        teamAOauthClientJob.setConsumerGroup("x-consumer_group_b2");
        teamAOauthClientJob.setTopicName(TOPIC_X);

        teamAOauthClientJob.consumerStrimziOauthTls(CLUSTER_NAME).done();
        ClientUtils.waitForClientSuccess(TEAM_A_CONSUMER_NAME, NAMESPACE, MESSAGE_COUNT);
    }

    @Disabled("Will be implemented in next PR")
    @Test
    @Order(7)
    void testListTopics() {
        // TODO: in the new PR add AdminClient support with operations listTopics(), etc.
    }

    @Disabled("Will be implemented in next PR")
    @Test
    @Order(8)
    void testClusterVerification() {
        // TODO: create more examples via cluster wide stuff
    }

    @BeforeAll
    void setUp()  {
        keycloakInstance.setRealm("kafka-authz", true);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName("tls")
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationOAuth()
                                .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .withTlsTrustedCertificates(
                                    new CertSecretSourceBuilder()
                                        .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                        .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                        .build())
                                .withDisableTlsHostnameVerification(true)
                            .endKafkaListenerAuthenticationOAuth()
                        .endGenericKafkaListener()
                    .endListeners()
                    .withNewKafkaAuthorizationKeycloak()
                        .withClientId(KAFKA_CLIENT_ID)
                        .withDisableTlsHostnameVerification(true)
                        .withDelegateToKafkaAcls(false)
                        // ca.crt a tls.crt
                        .withTlsTrustedCertificates(
                            new CertSecretSourceBuilder()
                                .withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME)
                                .withCertificate(KeycloakInstance.KEYCLOAK_SECRET_CERT)
                                .build()
                        )
                        .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .endKafkaAuthorizationKeycloak()
                .endKafka()
            .endSpec()
            .done();

        LOGGER.info("Setting producer and consumer properties");

        KafkaUserResource.tlsUser(CLUSTER_NAME, TEAM_A_CLIENT).done();
        KafkaUserResource.tlsUser(CLUSTER_NAME, TEAM_B_CLIENT).done();

        teamAOauthClientJob = new KafkaOauthExampleClients.KafkaOauthClientsBuilder()
            .withProducerName(TEAM_A_PRODUCER_NAME)
            .withConsumerName(TEAM_A_CONSUMER_NAME)
            .withBootstrapServer(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
            .withTopicName(TOPIC_A)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroup("a-consumer_group")
            .withOAuthClientId(TEAM_A_CLIENT)
            .withOAuthClientSecret(TEAM_A_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        teamBOauthClientJob = new KafkaOauthExampleClients.KafkaOauthClientsBuilder()
            .withProducerName(TEAM_B_PRODUCER_NAME)
            .withConsumerName(TEAM_B_CONSUMER_NAME)
            .withBootstrapServer(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
            .withTopicName(TOPIC_A)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroup("x-" + ClientUtils.generateRandomConsumerGroup())
            .withOAuthClientId(TEAM_B_CLIENT)
            .withOAuthClientSecret(TEAM_B_CLIENT_SECRET)
            .withOAuthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();
    }
}
