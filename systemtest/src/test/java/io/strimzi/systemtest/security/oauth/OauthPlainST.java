/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePort;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePortBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenerPlainBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.externalClients.OauthExternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.cli.annotations.Description;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;

import java.time.Duration;
import java.util.Map;

import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.HTTP_BRIDGE_DEFAULT_PORT;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(OAUTH)
@Tag(REGRESSION)
@Tag(NODEPORT_SUPPORTED)
@Tag(EXTERNAL_CLIENTS_USED)
public class OauthPlainST extends OauthAbstractST {

    private OauthExternalKafkaClient oauthExternalKafkaClient;

    @Description(
            "As an oauth producer, I should be able to produce messages to the kafka broker\n" +
            "As an oauth consumer, I should be able to consumer messages from the kafka broker.")
    @Test
    void testProducerConsumer() {
        oauthExternalKafkaClient.setClusterName(CLUSTER_NAME);

        oauthExternalKafkaClient.verifyProducedAndConsumedMessages(
            oauthExternalKafkaClient.sendMessagesPlain(),
            oauthExternalKafkaClient.receiveMessagesPlain()
        );
    }

    @Description("As an oauth KafkaConnect, I should be able to sink messages from kafka broker topic.")
    @Test
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testProducerConsumerConnect() {
        oauthExternalKafkaClient.verifyProducedAndConsumedMessages(
            oauthExternalKafkaClient.sendMessagesPlain(),
            oauthExternalKafkaClient.receiveMessagesPlain()
        );

        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .withNewSpec()
                    .withReplicas(1)
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .withNewKafkaClientAuthenticationOAuth()
                        .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                        .withClientId("kafka-connect")
                        .withNewClientSecret()
                            .withSecretName(CONNECT_OAUTH_SECRET)
                            .withKey(OAUTH_KEY)
                        .endClientSecret()
                    .endKafkaClientAuthenticationOAuth()
                    .withTls(null)
                .endSpec()
                .done();

        String kafkaConnectPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(kafkaConnectPodName);

        KafkaConnectorUtils.createFileSinkConnector(kafkaConnectPodName, TOPIC_NAME, Constants.DEFAULT_SINK_FILE_PATH, "http://localhost:8083");

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH);
    }

    @Description("As an oauth mirror maker, I should be able to replicate topic data between kafka clusters")
    @Test
    @Tag(MIRROR_MAKER)
    void testProducerConsumerMirrorMaker() {
        oauthExternalKafkaClient.verifyProducedAndConsumedMessages(
            oauthExternalKafkaClient.sendMessagesPlain(),
            oauthExternalKafkaClient.receiveMessagesPlain()
        );

        String targetKafkaCluster = CLUSTER_NAME + "-target";

        KafkaResource.kafkaEphemeral(targetKafkaCluster, 1, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewPlain()
                            .withNewKafkaListenerAuthenticationOAuth()
                                .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                .withUserNameClaim(keycloakInstance.getUserNameClaim())
                            .endKafkaListenerAuthenticationOAuth()
                        .endPlain()
                        .withNewKafkaListenerExternalNodePort()
                            .withNewKafkaListenerAuthenticationOAuth()
                                .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                .withUserNameClaim(keycloakInstance.getUserNameClaim())
                            .endKafkaListenerAuthenticationOAuth()
                            .withTls(false)
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        KafkaMirrorMakerResource.kafkaMirrorMaker(CLUSTER_NAME, CLUSTER_NAME, targetKafkaCluster,
                ClientUtils.generateRandomConsumerGroup(), 1, false)
                .editSpec()
                    .withNewConsumer()
                        .withBootstrapServers(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                        .withGroupId(ClientUtils.generateRandomConsumerGroup())
                        .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .withNewKafkaClientAuthenticationOAuth()
                            .withNewTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                            .withClientId("kafka-mirror-maker")
                            .withNewClientSecret()
                                .withSecretName(MIRROR_MAKER_OAUTH_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                        .endKafkaClientAuthenticationOAuth()
                        .withTls(null)
                    .endConsumer()
                    .withNewProducer()
                        .withBootstrapServers(KafkaResources.plainBootstrapAddress(targetKafkaCluster))
                        .withNewKafkaClientAuthenticationOAuth()
                            .withNewTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                            .withClientId("kafka-mirror-maker")
                            .withNewClientSecret()
                                .withSecretName(MIRROR_MAKER_OAUTH_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                        .endKafkaClientAuthenticationOAuth()
                        .addToConfig(ProducerConfig.ACKS_CONFIG, "all")
                        .withTls(null)
                    .endProducer()
                .endSpec()
                .done();

        TestUtils.waitFor("Waiting for Mirror Maker will copy messages from " + CLUSTER_NAME + " to " + targetKafkaCluster,
            Constants.GLOBAL_CLIENTS_POLL, Constants.TIMEOUT_FOR_MIRROR_MAKER_COPY_MESSAGES_BETWEEN_BROKERS,
            () -> {
                oauthExternalKafkaClient.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());
                return oauthExternalKafkaClient.receiveMessagesPlain() == MESSAGE_COUNT;
            });
    }

    @Test
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    void testProducerConsumerMirrorMaker2() {
        oauthExternalKafkaClient.verifyProducedAndConsumedMessages(
            oauthExternalKafkaClient.sendMessagesPlain(),
            oauthExternalKafkaClient.receiveMessagesPlain()
        );

        String kafkaSourceClusterName = CLUSTER_NAME;
        String kafkaTargetClusterName = CLUSTER_NAME + "-target";
        // mirror maker 2 adding prefix to mirrored topic for in this case mirrotopic will be : my-cluster.my-topic
        String kafkaTargetClusterTopicName = kafkaSourceClusterName + "." + TOPIC_NAME;
    
        KafkaResource.kafkaEphemeral(kafkaTargetClusterName, 3, 1)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewPlain()
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                    .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                    .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                    .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                    .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .endKafkaListenerAuthenticationOAuth()
                            .endPlain()
                            .withNewKafkaListenerExternalNodePort()
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                    .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                                    .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                                    .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                                    .withUserNameClaim(keycloakInstance.getUserNameClaim())
                                .endKafkaListenerAuthenticationOAuth()
                                .withTls(false)
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        // Deploy Mirror Maker 2.0 with oauth
        KafkaMirrorMaker2ClusterSpec sourceClusterWithOauth = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias(kafkaSourceClusterName)
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(kafkaSourceClusterName))
                .withNewKafkaClientAuthenticationOAuth()
                    .withNewTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId("kafka-mirror-maker-2")
                    .withNewClientSecret()
                        .withSecretName(MIRROR_MAKER_2_OAUTH_SECRET)
                        .withKey(OAUTH_KEY)
                    .endClientSecret()
                .endKafkaClientAuthenticationOAuth()
                .build();

        KafkaMirrorMaker2ClusterSpec targetClusterWithOauth = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias(kafkaTargetClusterName)
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(kafkaTargetClusterName))
                .withNewKafkaClientAuthenticationOAuth()
                    .withNewTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId("kafka-mirror-maker-2")
                    .withNewClientSecret()
                        .withSecretName(MIRROR_MAKER_2_OAUTH_SECRET)
                        .withKey(OAUTH_KEY)
                    .endClientSecret()
                .endKafkaClientAuthenticationOAuth()
                .build();
        
        KafkaMirrorMaker2Resource.kafkaMirrorMaker2(CLUSTER_NAME, kafkaTargetClusterName, kafkaSourceClusterName, 1, false)
                .editSpec()
                    .withClusters(sourceClusterWithOauth, targetClusterWithOauth)
                    .editFirstMirror()
                        .withNewSourceCluster(kafkaSourceClusterName)
                    .endMirror()
                .endSpec()
                .done();

        TestUtils.waitFor("Waiting for Mirror Maker 2 will copy messages from " + kafkaSourceClusterName + " to " + kafkaTargetClusterName,
            Duration.ofSeconds(30).toMillis(), Constants.TIMEOUT_FOR_MIRROR_MAKER_COPY_MESSAGES_BETWEEN_BROKERS,
            () -> {
                oauthExternalKafkaClient.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());
                return oauthExternalKafkaClient.receiveMessagesPlain() == MESSAGE_COUNT;
            });
    }

    @Description("As a oauth bridge, I should be able to send messages to bridge endpoint.")
    @Test
    @Tag(BRIDGE)
    void testProducerConsumerBridge() {
        oauthExternalKafkaClient.verifyProducedAndConsumedMessages(
            oauthExternalKafkaClient.sendMessagesPlain(),
            oauthExternalKafkaClient.receiveMessagesPlain()
        );

        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();

        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1)
                .editSpec()
                    .withNewKafkaClientAuthenticationOAuth()
                        .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                        .withClientId("kafka-bridge")
                        .withNewClientSecret()
                            .withSecretName(BRIDGE_OAUTH_SECRET)
                            .withKey(OAUTH_KEY)
                        .endClientSecret()
                    .endKafkaClientAuthenticationOAuth()
                .endSpec()
                .done();

        String producerName = "bridge-producer";

        KafkaClientsResource.producerStrimziBridge(producerName, KafkaBridgeResources.serviceName(CLUSTER_NAME), HTTP_BRIDGE_DEFAULT_PORT, TOPIC_NAME, MESSAGE_COUNT).done();
        ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);
    }

    @Test
    void testIntrospectionEndpointWithPlainCommunication() {
        LOGGER.info("Deploying kafka...");

        keycloakInstance.setIntrospectionEndpointUri("http://" + keycloakInstance.getHttpUri() + "/auth/realms/internal/protocol/openid-connect/token/introspect");
        String introspectionKafka = CLUSTER_NAME + "-intro";

        KafkaResource.kafkaEphemeral(introspectionKafka, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withNewKafkaListenerAuthenticationOAuth()
                                .withClientId(OAUTH_KAFKA_CLIENT_NAME)
                                .withNewClientSecret()
                                    .withSecretName(OAUTH_KAFKA_CLIENT_SECRET)
                                    .withKey(OAUTH_KEY)
                                .endClientSecret()
                                .withAccessTokenIsJwt(false)
                                .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                                .withIntrospectionEndpointUri(keycloakInstance.getIntrospectionEndpointUri())
                            .endKafkaListenerAuthenticationOAuth()
                            .withTls(false)
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        oauthExternalKafkaClient.setClusterName(introspectionKafka);

        oauthExternalKafkaClient.verifyProducedAndConsumedMessages(
            oauthExternalKafkaClient.sendMessagesPlain(),
            oauthExternalKafkaClient.receiveMessagesPlain()
        );    }

    @BeforeEach
    void setUpEach() {
        oauthExternalKafkaClient.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());
    }

    @BeforeAll
    void setUp() {
        keycloakInstance.setRealm("internal", false);

        LOGGER.info("Setting producer and consumer properties");

        oauthExternalKafkaClient = new OauthExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withOauthClientId(OAUTH_CLIENT_NAME)
            .withClientSecretName(OAUTH_CLIENT_SECRET)
            .withOauthTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
            .build();

        LOGGER.info("Oauth kafka client has following settings {}", oauthExternalKafkaClient.toString());

        String kafkaName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            // internal plain
            kafka.getSpec().getKafka().getListeners().setPlain(
                new KafkaListenerPlainBuilder()
                    .withNewKafkaListenerAuthenticationOAuth()
                        .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                        .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                        .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                        .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                        .withUserNameClaim(keycloakInstance.getUserNameClaim())
                    .endKafkaListenerAuthenticationOAuth()
                    .build());

            // external
            kafka.getSpec().getKafka().getListeners().setExternal(
                new KafkaListenerExternalNodePortBuilder()
                    .withNewKafkaListenerAuthenticationOAuth()
                        .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                        .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                        .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                        .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                        .withUserNameClaim(keycloakInstance.getUserNameClaim())
                    .endKafkaListenerAuthenticationOAuth()
                    .build());

            ((KafkaListenerExternalNodePort) kafka.getSpec().getKafka().getListeners().getExternal()).setTls(false);
        });

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
    }
}
