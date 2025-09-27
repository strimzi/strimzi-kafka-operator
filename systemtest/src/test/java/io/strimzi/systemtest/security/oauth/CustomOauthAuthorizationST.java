/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.FIPSNotSupported;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaOauthClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaOauthClientsBuilder;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Map;

import static io.strimzi.systemtest.TestTags.OAUTH;
import static io.strimzi.systemtest.TestTags.REGRESSION;

@Tag(OAUTH)
@Tag(REGRESSION)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@FIPSNotSupported("Keycloak is not customized to run on FIPS env - https://github.com/strimzi/strimzi-kafka-operator/issues/8331")
public class CustomOauthAuthorizationST extends OauthAbstractST {
    protected static final Logger LOGGER = LogManager.getLogger(CustomOauthAuthorizationST.class);

    private final String oauthClusterName = "oauth-cluster-authz-name";

    private static final String TEAM_A_CLIENT = "team-a-client";
    private static final String TEAM_B_CLIENT = "team-b-client";

    private static final String TEAM_A_CLIENT_SECRET = "team-a-client-secret";
    private static final String TEAM_B_CLIENT_SECRET = "team-b-client-secret";

    private static final String TOPIC_A = "a-topic";
    private static final String TOPIC_B = "b-topic";
    private static final String TOPIC_X = "x-topic";

    private static final String TEAM_A_PRODUCER_NAME = TEAM_A_CLIENT + "-producer";
    private static final String TEAM_A_CONSUMER_NAME = TEAM_A_CLIENT + "-consumer";
    private static final String TEAM_B_PRODUCER_NAME = TEAM_B_CLIENT + "-producer";
    private static final String TEAM_B_CONSUMER_NAME = TEAM_B_CLIENT + "-consumer";

    private static final String TEST_REALM = "kafka-authz";

    /**
     * As a member of team A, I should be able to read and write to all topics starting with a-.
     */
    @ParallelTest
    @Order(1)
    void smokeTestForClients() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String teamAProducerName = TEAM_A_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String teamAConsumerName = TEAM_A_CONSUMER_NAME + "-" + testStorage.getClusterName();
        String topicName = TOPIC_A + "-" + testStorage.getTopicName();
        String consumerGroup = "a-consumer_group-" + testStorage.getClusterName();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, topicName, oauthClusterName).build());

        KafkaOauthClients teamAOauthClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(teamAProducerName)
            .withConsumerName(teamAConsumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicName)
            .withMessageCount(testStorage.getMessageCount())
            .withConsumerGroup(consumerGroup)
            .withOauthClientId(TEAM_A_CLIENT)
            .withOauthClientSecret(TEAM_A_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        KubeResourceManager.get().createResourceWithWait(teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, teamAProducerName, testStorage.getMessageCount());
        KubeResourceManager.get().createResourceWithWait(teamAOauthClientJob.consumerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, teamAConsumerName, testStorage.getMessageCount());
    }

    /**
     * As a member of team A, I should be able to write to topics that starts with x- on any cluster and " +
     * and should also write and read to topics starting with 'a-'.
     */
    @ParallelTest
    @Order(2)
    void testTeamAWriteToTopic() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String teamAProducerName = TEAM_A_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String teamAConsumerName = TEAM_A_CONSUMER_NAME + "-" + testStorage.getClusterName();
        String consumerGroup = "a-consumer_group-" + testStorage.getClusterName();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());

        KafkaOauthClients teamAOauthClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(teamAProducerName)
            .withConsumerName(teamAConsumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withConsumerGroup(consumerGroup)
            .withOauthClientId(TEAM_A_CLIENT)
            .withOauthClientSecret(TEAM_A_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            // by default it's set to 1000, which makes the job longer to fail
            .withAdditionalConfig("retry.backoff.max.ms=100\n")
            .build();

        LOGGER.info("Sending {} messages to Broker with Topic name {}", testStorage.getMessageCount(), testStorage.getTopicName());
        LOGGER.info("Producer will not produce messages because authorization Topic will failed. Team A can write only to Topic starting with 'x-'");

        KubeResourceManager.get().createResourceWithWait(teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName));
        JobUtils.waitForJobFailure(Environment.TEST_SUITE_NAMESPACE, teamAProducerName, 30_000);
        JobUtils.deleteJobWithWait(Environment.TEST_SUITE_NAMESPACE, teamAProducerName);

        String topicXName = TOPIC_X + "-" + testStorage.getClusterName();
        LOGGER.info("Sending {} messages to Broker with Topic name {}", testStorage.getMessageCount(), topicXName);

        teamAOauthClientJob = new KafkaOauthClientsBuilder(teamAOauthClientJob)
            .withConsumerGroup(consumerGroup)
            .withTopicName(topicXName)
            .build();

        KubeResourceManager.get().createResourceWithWait(teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName));
        JobUtils.waitForJobFailure(Environment.TEST_SUITE_NAMESPACE, teamAProducerName, 30_000);
        JobUtils.deleteJobWithWait(Environment.TEST_SUITE_NAMESPACE, teamAProducerName);

        // Team A can not create topic starting with 'x-' only write to existing on
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, topicXName, oauthClusterName).build());
        KubeResourceManager.get().createResourceWithWait(teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, teamAProducerName, testStorage.getMessageCount());

        String topicAName = TOPIC_A + "-" + testStorage.getClusterName();

        LOGGER.info("Sending {} messages to Broker with Topic name {}", testStorage.getMessageCount(), topicAName);

        teamAOauthClientJob = new KafkaOauthClientsBuilder(teamAOauthClientJob)
            .withConsumerGroup(consumerGroup)
            .withTopicName(topicAName)
            .build();

        KubeResourceManager.get().createResourceWithWait(teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, teamAProducerName, testStorage.getMessageCount());
    }

    /**
     * As a member of team A, I should be able only read from consumer that starts with a_.
     */
    @ParallelTest
    @Order(3)
    void testTeamAReadFromTopic() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String teamAProducerName = TEAM_A_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String teamAConsumerName = TEAM_A_CONSUMER_NAME + "-" + testStorage.getClusterName();
        String topicAName = TOPIC_A + "-" + testStorage.getTopicName();
        String consumerGroup = "a-consumer_group-" + testStorage.getClusterName();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, topicAName, oauthClusterName).build());

        KafkaOauthClients teamAOauthClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(teamAProducerName)
            .withConsumerName(teamAConsumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicAName)
            .withMessageCount(testStorage.getMessageCount())
            .withConsumerGroup(consumerGroup)
            .withOauthClientId(TEAM_A_CLIENT)
            .withOauthClientSecret(TEAM_A_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        LOGGER.info("Sending {} messages to Broker with Topic name {}", testStorage.getMessageCount(), topicAName);
        KubeResourceManager.get().createResourceWithWait(teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, teamAProducerName, testStorage.getMessageCount());

        // team A client shouldn't be able to consume messages with wrong consumer group

        teamAOauthClientJob = new KafkaOauthClientsBuilder(teamAOauthClientJob)
            .withConsumerGroup("bad_consumer_group" + testStorage.getClusterName())
            .withTopicName(topicAName)
            .build();

        KubeResourceManager.get().createResourceWithWait(teamAOauthClientJob.consumerStrimziOauthTls(oauthClusterName));
        JobUtils.waitForJobFailure(Environment.TEST_SUITE_NAMESPACE, teamAConsumerName, 30_000);
        JobUtils.deleteJobWithWait(Environment.TEST_SUITE_NAMESPACE, teamAProducerName);

        // team A client should be able to consume messages with correct consumer group

        teamAOauthClientJob = new KafkaOauthClientsBuilder(teamAOauthClientJob)
            .withConsumerGroup("a-correct_consumer_group" + testStorage.getClusterName())
            .withTopicName(topicAName)
            .build();

        KubeResourceManager.get().createResourceWithWait(teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, teamAProducerName, testStorage.getMessageCount());
    }

    /**
     * As a member of team B, I should be able to write and read from topics that starts with b-.
     */
    @ParallelTest
    @Order(4)
    void testTeamBWriteToTopic() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String consumerGroup = "x-" + testStorage.getClusterName();
        String teamBProducerName = TEAM_B_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String teamBConsumerName = TEAM_B_CONSUMER_NAME + "-" + testStorage.getClusterName();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());

        KafkaOauthClients teamBOauthClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(teamBProducerName)
            .withConsumerName(teamBConsumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withConsumerGroup(consumerGroup)
            .withOauthClientId(TEAM_B_CLIENT)
            .withOauthClientSecret(TEAM_B_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            // by default it's set to 1000, which makes the job longer to fail
            .withAdditionalConfig("retry.backoff.max.ms=100\n")
            .build();

        LOGGER.info("Sending {} messages to Broker with Topic name {}", testStorage.getMessageCount(), testStorage.getTopicName());
        // Producer will not produce messages because authorization topic will failed. Team A can write only to topic starting with 'x-'
        KubeResourceManager.get().createResourceWithWait(teamBOauthClientJob.producerStrimziOauthTls(oauthClusterName));
        JobUtils.waitForJobFailure(Environment.TEST_SUITE_NAMESPACE, teamBProducerName, 30_000);
        JobUtils.deleteJobWithWait(Environment.TEST_SUITE_NAMESPACE, teamBProducerName);

        LOGGER.info("Sending {} messages to Broker with Topic name {}", testStorage.getMessageCount(), TOPIC_B);

        teamBOauthClientJob = new KafkaOauthClientsBuilder(teamBOauthClientJob)
            .withConsumerGroup("x-consumer_group_b-" + testStorage.getClusterName())
            .withTopicName(TOPIC_B)
            .build();

        KubeResourceManager.get().createResourceWithWait(teamBOauthClientJob.producerStrimziOauthTls(oauthClusterName));
        KubeResourceManager.get().createResourceWithWait(teamBOauthClientJob.consumerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientsSuccess(Environment.TEST_SUITE_NAMESPACE, teamBConsumerName, teamBProducerName, testStorage.getMessageCount());
    }

    /**
     * As a member of team A, I can write to topics starting with 'x-' and as a member of team B can read from topics starting with 'x-'.
     */
    @ParallelTest
    @Order(5)
    void testTeamAWriteToTopicStartingWithXAndTeamBReadFromTopicStartingWithX() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String teamAProducerName = TEAM_A_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String teamAConsumerName = TEAM_A_CONSUMER_NAME + "-" + testStorage.getClusterName();
        String teamBProducerName = TEAM_B_PRODUCER_NAME + "-" + testStorage.getClusterName();
        String teamBConsumerName = TEAM_B_CONSUMER_NAME + "-" + testStorage.getClusterName();
        // only write means that Team A can not create new topic 'x-.*'
        String topicXName = TOPIC_X + testStorage.getTopicName();
        String consumerGroup = "x-" + testStorage.getClusterName();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, topicXName, oauthClusterName).build());

        KafkaOauthClients teamAOauthClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(teamAProducerName)
            .withConsumerName(teamAConsumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicXName)
            .withMessageCount(testStorage.getMessageCount())
            .withConsumerGroup(consumerGroup)
            .withOauthClientId(TEAM_A_CLIENT)
            .withOauthClientSecret(TEAM_A_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        teamAOauthClientJob = new KafkaOauthClientsBuilder(teamAOauthClientJob)
            .withConsumerGroup("a-consumer_group" + testStorage.getClusterName())
            .withTopicName(topicXName)
            .build();

        KubeResourceManager.get().createResourceWithWait(teamAOauthClientJob.producerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, teamAProducerName, testStorage.getMessageCount());

        KafkaOauthClients teamBOauthClientJob = new KafkaOauthClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(teamBProducerName)
            .withConsumerName(teamBConsumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(oauthClusterName))
            .withTopicName(topicXName)
            .withMessageCount(testStorage.getMessageCount())
            .withConsumerGroup("x-consumer_group_b-" + testStorage.getClusterName())
            .withOauthClientId(TEAM_B_CLIENT)
            .withOauthClientSecret(TEAM_B_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        KubeResourceManager.get().createResourceWithWait(teamBOauthClientJob.consumerStrimziOauthTls(oauthClusterName));
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, teamBConsumerName, testStorage.getMessageCount());
    }

    @BeforeAll
    void setUp()  {
        super.setupCoAndKeycloak(Environment.TEST_SUITE_NAMESPACE);

        keycloakInstance.setRealm(TEST_REALM, true);

        String jaasConfig = JAAS_CONFIG_BUILDER.apply("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                Map.of(
                        "unsecuredLoginStringClaim_sub", "thePrincipalName",
                        "oauth.valid.issuer.uri", keycloakInstance.getValidIssuerUri(),
                        "oauth.jwks.expiry.seconds", Integer.toString(keycloakInstance.getJwksExpireSeconds()),
                        "oauth.jwks.refresh.seconds", Integer.toString(keycloakInstance.getJwksRefreshSeconds()),
                        "oauth.jwks.endpoint.uri", keycloakInstance.getJwksEndpointUri(),
                        "oauth.username.claim", keycloakInstance.getUserNameClaim(),
                        "oauth.ssl.endpoint.identification.algorithm", "",
                        "oauth.ssl.truststore.location", "/mnt/keycloak-certs/" + KeycloakInstance.KEYCLOAK_SECRET_CERT,
                        "oauth.ssl.truststore.type", "PEM"
                ));

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(Environment.TEST_SUITE_NAMESPACE, KafkaComponents.getBrokerPoolName(oauthClusterName), oauthClusterName, 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(Environment.TEST_SUITE_NAMESPACE, KafkaComponents.getControllerPoolName(oauthClusterName), oauthClusterName, 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, 3)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationCustomAuth()
                                    .withSasl(true)
                                    .withListenerConfig(Map.of(
                                            "sasl.enabled.mechanisms", "OAUTHBEARER",
                                            "oauthbearer.sasl.server.callback.handler.class", "io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler",
                                            "oauthbearer.sasl.jaas.config", jaasConfig
                                    ))
                                .endKafkaListenerAuthenticationCustomAuth()
                                .build())
                        .withNewKafkaAuthorizationCustom()
                            .withAuthorizerClass("io.strimzi.kafka.oauth.server.authorizer.KeycloakAuthorizer")
                            .withSupportsAdminApi(false)
                        .endKafkaAuthorizationCustom()
                        .addToConfig(Map.of(
                                "principal.builder.class", "io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder",
                                "strimzi.authorization.client.id", "kafka",
                                "strimzi.authorization.token.endpoint.uri", keycloakInstance.getOauthTokenEndpointUri(),
                                "strimzi.authorization.ssl.endpoint.identification.algorithm", "",
                                "strimzi.authorization.delegate.to.kafka.acl", "true",
                                "strimzi.authorization.ssl.truststore.location", "/mnt/keycloak-certs/" + KeycloakInstance.KEYCLOAK_SECRET_CERT,
                                "strimzi.authorization.ssl.truststore.type", "PEM"
                        ))
                        .withNewTemplate()
                            .withNewPod()
                                .withVolumes(new AdditionalVolumeBuilder().withName("keycloak-certs").withSecret(new SecretVolumeSourceBuilder().withSecretName(KeycloakInstance.KEYCLOAK_SECRET_NAME).build()).build())
                            .endPod()
                            .withNewKafkaContainer()
                                .withVolumeMounts(new VolumeMountBuilder().withName("keycloak-certs").withMountPath("/mnt/keycloak-certs").build())
                            .endKafkaContainer()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build());

        LOGGER.info("Setting producer and consumer properties");

        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, TEAM_A_CLIENT, oauthClusterName).build());
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, TEAM_B_CLIENT, oauthClusterName).build());
    }
}
