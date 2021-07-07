/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.oauth;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.KeycloakUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.rmi.UnexpectedException;
import java.util.List;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;

@Tag(OAUTH)
@Tag(REGRESSION)
public class OauthScopeST extends OauthAbstractST {

    public static final String NAMESPACE = "oauth2-cluster-scope-audience-test";

    private final String oauthClusterName = "oauth-cluster-scope-name";

    @ParallelNamespaceTest
    @Tag(CONNECT)
    void testClientSideScopeKafkaConnect(ExtensionContext extensionContext) throws UnexpectedException {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(NAMESPACE, extensionContext);
        final String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String producerName = OAUTH_PRODUCER_NAME + "-" + clusterName;
        final String consumerName = OAUTH_CONSUMER_NAME + "-" + clusterName;
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String audienceListener = "audielist";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
            .editKafka()
            .withListeners(
                new GenericKafkaListenerBuilder()
                    .withName(audienceListener)
                    .withPort(Integer.parseInt(audienceListenerPort))
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
                        .withCheckAudience(true)
                        .withCustomClaimCheck("@.scope == 'test'")
                        .withClientScope("test")
                        .withClientId("kafka-component")
                    .endKafkaListenerAuthenticationOAuth()
                    .build())
            .endKafka()
            .endSpec()
            .build());

        // SCOPE TESTING
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());
        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, clusterName, 1)
            .withNewSpec()
                .withReplicas(1)
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(clusterName))
                .withConfig(connectorConfig)
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
                    .withScope("test")
                .endKafkaClientAuthenticationOAuth()
                .withTls(null)
                .endSpec()
            .build());

        String kafkaConnectPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(namespaceName, clusterName).get(0).getMetadata().getName();
        // kafka connect should fail because we do not specify audience but only scope
        PodUtils.waitUntilPodIsInCrashLoopBackOff(namespaceName, kafkaConnectPodName);

        // re-configuring Kafka listener to have client scope assigned to null
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            List<GenericKafkaListener> audieListener = kafka.getSpec().getKafka().getListeners()
                .stream()
                .filter(listener -> listener.getName().equals(audienceListener))
                .collect(Collectors.toList());

            ((KafkaListenerAuthenticationOAuth) audieListener.get(0).getAuth()).setClientScope(null);
            kafka.getSpec().getKafka().getListeners().set(0, audieListener.get(0));
        }, namespaceName);

        StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 1, ResourceOperation.getTimeoutForResourceReadiness(Constants.STATEFUL_SET));
        // kafka connect should fail because we do not specify audience and also not scope
        PodUtils.waitUntilPodIsInCrashLoopBackOff(namespaceName, kafkaConnectPodName);
    }

    @BeforeAll
    void setUp(ExtensionContext extensionContext) {
        super.beforeAllMayOverride(extensionContext);
        // for namespace
        super.setupCoAndKeycloak(extensionContext, NAMESPACE);

        keycloakInstance.setRealm("scope-test", false);
    }

    @AfterAll
    void tearDown(ExtensionContext extensionContext) throws Exception {
        // delete keycloak before namespace
        KeycloakUtils.deleteKeycloak(NAMESPACE);
        // delete namespace etc.
        super.afterAllMayOverride(extensionContext);
    }
}
