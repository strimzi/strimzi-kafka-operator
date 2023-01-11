/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.Level;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.rmi.UnexpectedException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(OAUTH)
@Tag(REGRESSION)
@IsolatedSuite
public class OauthScopeIsolatedST extends OauthAbstractST {
    
    private final String oauthClusterName = "oauth-cluster-scope-name";
    private final String scopeListener = "scopelist";
    private final String scopeListenerPort = "9098";
    private final String additionalOauthConfig =
        "sasl.mechanism = PLAIN\n" +
        "security.protocol = SASL_PLAINTEXT\n" +
        "sasl.jaas.config = org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka-client\" password=\"kafka-client-secret\" ;";

    @ParallelTest
    @Tag(CONNECT)
    void testScopeKafkaConnectSetIncorrectly(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        // SCOPE TESTING
        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnect(clusterName, clusterName, 1)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
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

        String kafkaConnectPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(clusterOperator.getDeploymentNamespace(), KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getName();

        // we except that "Token validation failed: Custom claim check failed because we specify scope='null'"
        StUtils.waitUntilLogFromPodContainsString(clusterOperator.getDeploymentNamespace(), kafkaConnectPodName, KafkaConnectResources.deploymentName(clusterName), "30s", "Token validation failed: Custom claim check failed");
    }

    @ParallelTest
    @Tag(CONNECT)
    void testScopeKafkaConnectSetCorrectly(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        // SCOPE TESTING
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(clusterName, clusterName, 1)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
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
                    // scope set correctly regarding to the scope-test realm
                    .withScope("test")
                .endKafkaClientAuthenticationOAuth()
                .withTls(null)
                .endSpec()
            .build());

        // Kafka connect passed the validation process (implicit the KafkaConnect is up)
        // explicitly verifying also logs
        String kafkaPodName = kubeClient().listPodsByPrefixInName(clusterOperator.getDeploymentNamespace(), KafkaResources.kafkaPodName(oauthClusterName, 0)).get(0).getMetadata().getName();

        String kafkaLog = KubeClusterResource.cmdKubeClient(clusterOperator.getDeploymentNamespace()).execInCurrentNamespace(Level.DEBUG, "logs", kafkaPodName, "--tail", "50").out();
        assertThat(kafkaLog, CoreMatchers.containsString("Access token expires at"));
        assertThat(kafkaLog, CoreMatchers.containsString("Evaluating path: $[*][?]"));
        assertThat(kafkaLog, CoreMatchers.containsString("Evaluating path: @['scope']"));
        assertThat(kafkaLog, CoreMatchers.containsString("User validated"));
        assertThat(kafkaLog, CoreMatchers.containsString("Set validated token on callback"));
    }

    @ParallelTest
    void testClientScopeKafkaSetCorrectly(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        final String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        KafkaClients oauthInternalClientChecksJob = new KafkaClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + scopeListenerPort)
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            // configures SASL/PLAIN to be used
            .withAdditionalConfig(additionalOauthConfig)
            .build();

        // clientScope is set to 'test' by default

        // verification phase the KafkaClient to authenticate.
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName, clusterOperator.getDeploymentNamespace()).build());

        resourceManager.createResource(extensionContext, oauthInternalClientChecksJob.producerStrimzi());
        // client should succeeded because we set to `clientScope=test` and also Kafka has `scope=test`
        ClientUtils.waitForClientSuccess(producerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);
    }

    @IsolatedTest("Modification of shared Kafka cluster")
    void testClientScopeKafkaSetIncorrectly(ExtensionContext extensionContext) throws UnexpectedException {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        final String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(oauthClusterName, KafkaResources.kafkaStatefulSetName(oauthClusterName));

        KafkaClients oauthInternalClientChecksJob = new KafkaClientsBuilder()
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(oauthClusterName) + ":" + scopeListenerPort)
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            // configures SASL/PLAIN to be used
            .withAdditionalConfig(additionalOauthConfig)
            .build();

        Map<String, String> kafkaPods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), kafkaSelector);

        // re-configuring Kafka listener to have client scope assigned to null
        KafkaResource.replaceKafkaResourceInSpecificNamespace(oauthClusterName, kafka -> {
            List<GenericKafkaListener> scopeListeners = kafka.getSpec().getKafka().getListeners()
                .stream()
                .filter(listener -> listener.getName().equals(scopeListener))
                .collect(Collectors.toList());

            ((KafkaListenerAuthenticationOAuth) scopeListeners.get(0).getAuth()).setClientScope(null);
            kafka.getSpec().getKafka().getListeners().set(0, scopeListeners.get(0));
        }, clusterOperator.getDeploymentNamespace());

        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), kafkaSelector, 3, kafkaPods);

        // verification phase client should fail here because clientScope is set to 'null'
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(oauthClusterName, topicName, clusterOperator.getDeploymentNamespace()).build());

        resourceManager.createResource(extensionContext, oauthInternalClientChecksJob.producerStrimzi());
        // client should fail because the listener requires scope: 'test' in JWT token but was (the listener) temporarily
        // configured without clientScope resulting in a JWT token without the scope claim when using the clientId and
        // secret passed via SASL/PLAIN to obtain an access token in client's name.
        ClientUtils.waitForClientTimeout(producerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(clusterOperator.getDeploymentNamespace(), producerName);

        // rollback previous configuration
        // re-configuring Kafka listener to have client scope assigned to 'test'
        KafkaResource.replaceKafkaResourceInSpecificNamespace(oauthClusterName, kafka -> {
            List<GenericKafkaListener> scopeListeners = kafka.getSpec().getKafka().getListeners()
                .stream()
                .filter(listener -> listener.getName().equals(scopeListener))
                .collect(Collectors.toList());

            ((KafkaListenerAuthenticationOAuth) scopeListeners.get(0).getAuth()).setClientScope("test");
            kafka.getSpec().getKafka().getListeners().set(0, scopeListeners.get(0));
        }, clusterOperator.getDeploymentNamespace());

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), kafkaSelector, 3, kafkaPods);
    }

    @BeforeAll
    void setUp(ExtensionContext extensionContext) {
        super.setupCoAndKeycloak(extensionContext, clusterOperator.getDeploymentNamespace());

        keycloakInstance.setRealm("scope-test", false);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(oauthClusterName, 3)
            .editMetadata()
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
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
