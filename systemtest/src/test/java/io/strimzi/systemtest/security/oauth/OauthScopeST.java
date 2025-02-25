/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.FIPSNotSupported;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.OAUTH;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(OAUTH)
@Tag(REGRESSION)
@FIPSNotSupported("Keycloak is not customized to run on FIPS env - https://github.com/strimzi/strimzi-kafka-operator/issues/8331")
public class OauthScopeST extends OauthAbstractST {
    
    private final String oauthClusterName = "oauth-cluster-scope-name";
    private final String scopeListener = "scopelist";
    private final String scopeListenerPort = "9098";
    private final String additionalOauthConfig =
        "sasl.mechanism = PLAIN\n" +
        "security.protocol = SASL_PLAINTEXT\n" +
        "sasl.jaas.config = org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka-client\" password=\"kafka-client-secret\" ;";

    @ParallelTest
    @Tag(CONNECT)
    void testScopeKafkaConnectSetIncorrectly() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // SCOPE TESTING
        resourceManager.createResourceWithoutWait(KafkaConnectTemplates.kafkaConnect(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), testStorage.getClusterName(), 1)
            .withNewSpec()
                .withReplicas(1)
                .withBootstrapServers(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + scopeListenerPort)
                .withConfig(connectorConfig)
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewKafkaClientAuthenticationOAuth()
                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId("kafka-client")
                    .withNewClientSecret()
                        .withSecretName(OAUTH_KAFKA_CLIENT_SECRET)
                        .withKey(OAUTH_KEY)
                    .endClientSecret()
                    // scope set in-correctly regarding to the scope-test realm
                    .withScope(null)
                .endKafkaClientAuthenticationOAuth()
                .withTls(null)
                .endSpec()
            .build());

        String kafkaConnectPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(Environment.TEST_SUITE_NAMESPACE, KafkaConnectResources.componentName(testStorage.getClusterName())).get(0).getMetadata().getName();

        // we except that "Token validation failed: Custom claim check failed because we specify scope='null'"
        StUtils.waitUntilLogFromPodContainsString(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName, KafkaConnectResources.componentName(testStorage.getClusterName()), "30s", "Token validation failed: Custom claim check failed");
    }

    @ParallelTest
    @Tag(CONNECT)
    void testScopeKafkaConnectSetCorrectly() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // SCOPE TESTING
        resourceManager.createResourceWithWait(KafkaConnectTemplates.kafkaConnect(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), testStorage.getClusterName(), 1)
            .withNewSpec()
                .withReplicas(1)
                .withBootstrapServers(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + scopeListenerPort)
                .withConfig(connectorConfig)
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewKafkaClientAuthenticationOAuth()
                    .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                    .withClientId("kafka-client")
                    .withNewClientSecret()
                        .withSecretName(OAUTH_KAFKA_CLIENT_SECRET)
                        .withKey(OAUTH_KEY)
                    .endClientSecret()
                    // scope set correctly regarding the scope-test realm
                    .withScope("test")
                .endKafkaClientAuthenticationOAuth()
                .withTls(null)
                .endSpec()
            .build());

        // Kafka connect passed the validation process (implicit the KafkaConnect is up)
        // explicitly verifying also logs
        String kafkaConnectPodName = kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, StrimziPodSetResource.getBrokerComponentName(oauthClusterName)).get(0).getMetadata().getName();

        String kafkaLog = kubeClient().logsInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, kafkaConnectPodName);
        assertThat("Kafka's log doesn't contain information about expiration of the access token",
            kafkaLog.contains("Access token expires at"), is(true));
        assertThat("Kafka's log doesn't contain information about evaluating path",
            kafkaLog.contains("Evaluating path: $[*][?]"), is(true));
        assertThat("Kafka's log doesn't contain information about evaluating path for scope",
            kafkaLog.contains("Evaluating path: @['scope']"), is(true));
        assertThat("Kafka's log doesn't contain information about user validation",
            kafkaLog.contains("User validated"), is(true));
        assertThat("Kafka's log doesn't contain information about setting validated token on callback",
            kafkaLog.contains("Set validated token on callback"), is(true));
    }

    @ParallelTest
    void testClientScopeKafkaSetCorrectly() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        final String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();

        KafkaClients oauthInternalClientChecksJob = new KafkaClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + scopeListenerPort)
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            // configures SASL/PLAIN to be used
            .withAdditionalConfig(additionalOauthConfig)
            .build();

        // clientScope is set to 'test' by default

        // verification phase the KafkaClient to authenticate.
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());

        resourceManager.createResourceWithWait(oauthInternalClientChecksJob.producerStrimzi());
        // client should succeed because we set to `clientScope=test` and also Kafka has `scope=test`
        ClientUtils.waitForClientSuccess(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());
    }

    @IsolatedTest("Modification of shared Kafka cluster")
    void testClientScopeKafkaSetIncorrectly() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String producerName = OAUTH_PRODUCER_NAME + "-" + testStorage.getClusterName();
        final String consumerName = OAUTH_CONSUMER_NAME + "-" + testStorage.getClusterName();
        final LabelSelector brokerSelector = KafkaResource.getLabelSelector(oauthClusterName, StrimziPodSetResource.getBrokerComponentName(oauthClusterName));

        KafkaClients oauthInternalClientChecksJob = new KafkaClientsBuilder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + scopeListenerPort)
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            // configures SASL/PLAIN to be used
            .withAdditionalConfig(additionalOauthConfig)
            .build();

        Map<String, String> brokerPods = PodUtils.podSnapshot(Environment.TEST_SUITE_NAMESPACE, brokerSelector);

        // re-configuring Kafka listener to have client scope assigned to null
        KafkaResource.replaceKafkaResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, kafka -> {
            List<GenericKafkaListener> scopeListeners = kafka.getSpec().getKafka().getListeners()
                .stream()
                .filter(listener -> listener.getName().equals(scopeListener))
                .toList();

            ((KafkaListenerAuthenticationOAuth) scopeListeners.get(0).getAuth()).setClientScope(null);
            kafka.getSpec().getKafka().getListeners().set(0, scopeListeners.get(0));
        });

        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(Environment.TEST_SUITE_NAMESPACE, brokerSelector, 3, brokerPods);

        // verification phase client should fail here because clientScope is set to 'null'
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), oauthClusterName).build());

        resourceManager.createResourceWithWait(oauthInternalClientChecksJob.producerStrimzi());
        // client should fail because the listener requires scope: 'test' in JWT token but was (the listener) temporarily
        // configured without clientScope resulting in a JWT token without the scope claim when using the clientId and
        // secret passed via SASL/PLAIN to obtain an access token in client's name.
        ClientUtils.waitForClientTimeout(Environment.TEST_SUITE_NAMESPACE, producerName, testStorage.getMessageCount());
        JobUtils.deleteJobWithWait(Environment.TEST_SUITE_NAMESPACE, producerName);

        // rollback previous configuration
        // re-configuring Kafka listener to have client scope assigned to 'test'
        KafkaResource.replaceKafkaResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, kafka -> {
            List<GenericKafkaListener> scopeListeners = kafka.getSpec().getKafka().getListeners()
                .stream()
                .filter(listener -> listener.getName().equals(scopeListener))
                .toList();

            ((KafkaListenerAuthenticationOAuth) scopeListeners.get(0).getAuth()).setClientScope("test");
            kafka.getSpec().getKafka().getListeners().set(0, scopeListeners.get(0));
        });

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(Environment.TEST_SUITE_NAMESPACE, brokerSelector, 3, brokerPods);
    }

    @BeforeAll
    void setUp() {
        super.setupCoAndKeycloak(Environment.TEST_SUITE_NAMESPACE);

        keycloakInstance.setRealm("scope-test", false);

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(Environment.TEST_SUITE_NAMESPACE, KafkaNodePoolResource.getBrokerPoolName(oauthClusterName), oauthClusterName, 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(Environment.TEST_SUITE_NAMESPACE, KafkaNodePoolResource.getControllerPoolName(oauthClusterName), oauthClusterName, 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, oauthClusterName, 3)
            .editSpec()
                .editKafka()
                .withListeners(
                    new GenericKafkaListenerBuilder()
                        .withName(scopeListener)
                        .withPort(Integer.parseInt(scopeListenerPort))
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(false)
                        .withNewKafkaListenerAuthenticationOAuth()
                            .withValidIssuerUri(keycloakInstance.getValidIssuerUri())
                            .withJwksExpirySeconds(keycloakInstance.getJwksExpireSeconds())
                            .withJwksRefreshSeconds(keycloakInstance.getJwksRefreshSeconds())
                            .withJwksEndpointUri(keycloakInstance.getJwksEndpointUri())
                            .withUserNameClaim(keycloakInstance.getUserNameClaim())
                            .withEnablePlain(true)
                            .withTokenEndpointUri(keycloakInstance.getOauthTokenEndpointUri())
                            .withCheckAudience(false)
                            .withCustomClaimCheck("@.scope =~ /.*test.*/")
                            .withClientScope("test")
                            .withClientId("kafka-component")
                        .endKafkaListenerAuthenticationOAuth()
                    .build())
                .endKafka()
            .endSpec()
            .build());
    }
}
