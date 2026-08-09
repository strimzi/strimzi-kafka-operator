/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityEncryptionType;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.acl.StrimziAclOperation;
import io.strimzi.kafka.config.model.Scope;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.kafkaclients.ClientsAuthentication;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumer;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumerBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.DYNAMIC_CONFIGURATION;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.ROLLING_UPDATE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test suite for verifying configurable security of the Kafka cluster's internal communication."),
    labels = {
        @Label(value = TestDocsLabels.SECURITY),
        @Label(value = TestDocsLabels.KAFKA)
    }
)
class ClusterSecurityST extends AbstractST {
    private static final int BROKER_REPLICAS = 3;
    private static final int CONTROLLER_REPLICAS = 3;
    private static final String INTERNAL_CLUSTER_SECURITY_ANNOTATION = "strimzi.io/internal-cluster-security";

    @Tag(CRUISE_CONTROL)
    @Tag(DYNAMIC_CONFIGURATION)
    @Tag(ROLLING_UPDATE)
    @TestDoc(
        description = @Desc("Test a Kafka cluster that uses different combinations of authentication and encryption for internal communication."),
        steps = {
            @Step(value = "Deploy Kafka with specified encryption and authentication (including Entity Operator and Cruise Control).", expected = "The cluster is ready with the requested security configuration."),
            @Step(value = "Create a TLS user with ACLs and send and consume messages over mTLS.", expected = "The messages are authorized, sent, and consumed successfully."),
            @Step(value = "Change dynamic Kafka configuration.", expected = "The configuration is applied without rolling the Kafka pods."),
            @Step(value = "Change read-only Kafka configuration.", expected = "The Kafka controllers and brokers roll and remain functional."),
            @Step(value = "Run a Kafka rebalance.", expected = "Cruise Control completes the rebalance.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY),
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.CRUISE_CONTROL)
        }
    )
    @ParameterizedTest(name = "Encryption: {0}; Authentication: {1}")
    @MethodSource("securityConfigurationCombos")
    void testClusterSecurityConfiguration(ClusterSecurityEncryptionType encryption, ClusterSecurityAuthenticationType authentication) {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(),
                        testStorage.getClusterName(), BROKER_REPLICAS).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(),
                        testStorage.getClusterName(), CONTROLLER_REPLICAS).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafkaWithCruiseControlTunedForFastModelGeneration(testStorage.getNamespaceName(), testStorage.getClusterName(), BROKER_REPLICAS)
                        .editMetadata()
                            .addToAnnotations(INTERNAL_CLUSTER_SECURITY_ANNOTATION, clusterSecurityAnnotation(encryption, authentication))
                        .endMetadata()
                        .editSpec()
                            .editKafka()
                                .withNewKafkaAuthorizationSimple()
                                .endKafkaAuthorizationSimple()
                                .withListeners(
                                    new GenericKafkaListenerBuilder()
                                        .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                                        .withPort(9092)
                                        .withType(KafkaListenerType.INTERNAL)
                                        .withTls(false)
                                        .build(),
                                    new GenericKafkaListenerBuilder()
                                        .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                                        .withPort(9093)
                                        .withType(KafkaListenerType.INTERNAL)
                                        .withTls(true)
                                        .withNewKafkaListenerAuthenticationTlsAuth()
                                        .endKafkaListenerAuthenticationTlsAuth()
                                        .build())
                            .endKafka()
                        .endSpec()
                        .build(),
                ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );
        KubeResourceManager.get().createResourceWithWait(
                authorizedTlsUser(testStorage),
                KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 3, 3, 2).build()
        );

        // Assert security status
        assertClusterSecurityStatus(testStorage, encryption, authentication);

        // Test message producing and consuming
        KafkaProducerConsumer clients = kafkaClients(testStorage);
        sendAndReceiveMessages(testStorage, clients, testStorage.getMessageCount());

        // Check dynamic configuration update
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        KafkaUtils.updateConfigurationWithStabilityWait(testStorage.getNamespaceName(), testStorage.getClusterName(),
                "log.message.timestamp.type", "LogAppendTime");
        assertThat(KafkaUtils.verifyCrDynamicConfiguration(testStorage.getNamespaceName(), testStorage.getClusterName(),
                "log.message.timestamp.type", "LogAppendTime"), is(true));
        assertThat(KafkaUtils.verifyPodDynamicConfiguration(testStorage.getNamespaceName(), testStorage.getClusterName(),
                PodUtils.getPodNameByPrefix(testStorage.getNamespaceName(), testStorage.getScraperName()), Scope.CLUSTER_WIDE.toString(),
                "log.message.timestamp.type", "LogAppendTime"), is(true));
        assertThat("Controller pods rolled after a dynamic configuration update",
                PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector()), is(controllerPods));
        assertThat("Broker pods rolled after a dynamic configuration update",
                PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector()), is(brokerPods));

        // Check configuration updates through rolling updates
        KafkaUtils.updateSpecificConfiguration(testStorage.getNamespaceName(), testStorage.getClusterName(), "auto.create.topics.enable", false);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(),
                CONTROLLER_REPLICAS, controllerPods);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(),
                BROKER_REPLICAS, brokerPods);

        // Test message consumption
        consumeMessages(testStorage, clients, testStorage.getMessageCount());

        // Test CRuise Control
        KubeResourceManager.get().createResourceWithWait(
                KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName()).build()
        );
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(),
                KafkaRebalanceState.ProposalReady);
        KafkaRebalanceUtils.doRebalancingProcess(testStorage.getNamespaceName(), testStorage.getClusterName());
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test migrating internal cluster security from the default TLS and mTLS configuration to no security and back."),
        steps = {
            @Step(value = "Deploy a persistent Kafka cluster with authorization and send and consume messages using a TLS user with ACLs.", expected = "The cluster is ready and the messages are authorized and available."),
            @Step(value = "Pause reconciliation, stop all cluster workloads, remove status.clusterSecurity, disable TLS and mTLS, and unpause.", expected = "The cluster restarts without internal encryption or authentication."),
            @Step(value = "Consume the existing messages and send and consume new messages.", expected = "Both the existing and new messages are available."),
            @Step(value = "Repeat the stopped migration and remove the internal security annotation.", expected = "The cluster restarts with the default TLS and mTLS configuration."),
            @Step(value = "Consume all existing messages and send and consume more messages.", expected = "All existing and new messages are available after the round-trip migration.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY),
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testClusterSecurityMigration() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // Deploy the initial cluster
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(),
                testStorage.getClusterName(), BROKER_REPLICAS).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(),
                testStorage.getClusterName(), CONTROLLER_REPLICAS).build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), BROKER_REPLICAS)
                    .editSpec()
                        .editKafka()
                            .withNewKafkaAuthorizationSimple()
                            .endKafkaAuthorizationSimple()
                            .withListeners(
                                new GenericKafkaListenerBuilder()
                                    .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .withNewKafkaListenerAuthenticationTlsAuth()
                                    .endKafkaListenerAuthenticationTlsAuth()
                                    .build())
                        .endKafka()
                    .endSpec()
                    .build()
        );
        KubeResourceManager.get().createResourceWithWait(
            authorizedTlsUser(testStorage),
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 3, 3, 2).build()
        );

        assertClusterSecurityStatus(testStorage, ClusterSecurityEncryptionType.STRIMZI_TLS, ClusterSecurityAuthenticationType.STRIMZI_MTLS);

        KafkaProducerConsumer clients = kafkaClients(testStorage);
        sendAndReceiveMessages(testStorage, clients, testStorage.getMessageCount());

        // Migrate to different security settings
        migrateClusterSecurity(testStorage, clusterSecurityAnnotation(ClusterSecurityEncryptionType.NONE, ClusterSecurityAuthenticationType.NONE));
        assertClusterSecurityStatus(testStorage, ClusterSecurityEncryptionType.NONE, ClusterSecurityAuthenticationType.NONE);
        consumeMessages(testStorage, clients, testStorage.getMessageCount());
        sendMessages(testStorage, clients, testStorage.getMessageCount());
        consumeMessages(testStorage, clients, 2 * testStorage.getMessageCount());

        // Migrate back to the original to close the round trip
        migrateClusterSecurity(testStorage, clusterSecurityAnnotation(ClusterSecurityEncryptionType.STRIMZI_TLS, ClusterSecurityAuthenticationType.STRIMZI_MTLS));
        assertClusterSecurityStatus(testStorage, ClusterSecurityEncryptionType.STRIMZI_TLS, ClusterSecurityAuthenticationType.STRIMZI_MTLS);
        consumeMessages(testStorage, clients, 2 * testStorage.getMessageCount());
        sendMessages(testStorage, clients, testStorage.getMessageCount());
        consumeMessages(testStorage, clients, 3 * testStorage.getMessageCount());
    }

    private void migrateClusterSecurity(TestStorage testStorage, String clusterSecurity) {
        String namespaceName = testStorage.getNamespaceName();
        String clusterName = testStorage.getClusterName();

        KafkaUtils.annotateKafka(namespaceName, clusterName,
            Map.of(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"));
        KafkaUtils.waitForKafkaStatus(namespaceName, clusterName, CustomResourceStatus.ReconciliationPaused);

        String selector = Labels.STRIMZI_CLUSTER_LABEL + "=" + clusterName + "," + Labels.STRIMZI_KIND_LABEL + "=" + Kafka.RESOURCE_KIND;
        KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).exec(
            "delete", "strimzipodsets,deployments", "-l", selector, "--cascade=foreground");

        LabelSelector clusterPodSelector = new LabelSelectorBuilder()
            .addToMatchLabels(Labels.STRIMZI_CLUSTER_LABEL, clusterName)
            .addToMatchLabels(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND)
            .build();
        PodUtils.waitForPodsReady(namespaceName, clusterPodSelector, 0, true);

        CrdClients.kafkaClient()
            .inNamespace(namespaceName)
            .withName(clusterName)
            .subresource("status")
            .patch(PatchContext.of(PatchType.JSON_MERGE), "{\"status\":{\"clusterSecurity\":null}}");

        if (clusterSecurity == null) {
            KafkaUtils.removeAnnotation(namespaceName, clusterName, INTERNAL_CLUSTER_SECURITY_ANNOTATION);
        } else {
            KafkaUtils.annotateKafka(namespaceName, clusterName, Map.of(INTERNAL_CLUSTER_SECURITY_ANNOTATION, clusterSecurity));
        }
        KafkaUtils.annotateKafka(namespaceName, clusterName,
            Map.of(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "false"));

        PodUtils.waitForPodsReady(namespaceName, testStorage.getControllerSelector(), CONTROLLER_REPLICAS, true);
        PodUtils.waitForPodsReady(namespaceName, testStorage.getBrokerSelector(), BROKER_REPLICAS, true);
        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);
    }

    private KafkaProducerConsumer kafkaClients(TestStorage testStorage) {
        return new KafkaProducerConsumerBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount())
            .withAuthentication(ClientsAuthentication.configureTls(testStorage.getClusterName(), testStorage.getUsername()))
            .build();
    }

    private KafkaUser authorizedTlsUser(TestStorage testStorage) {
        return KafkaUserTemplates.tlsUser(testStorage)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withName(testStorage.getTopicName())
                        .endAclRuleTopicResource()
                        .withOperations(StrimziAclOperation.READ, StrimziAclOperation.WRITE,
                            StrimziAclOperation.DESCRIBE, StrimziAclOperation.CREATE)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleGroupResource()
                            .withName("*")
                        .endAclRuleGroupResource()
                        .withOperations(StrimziAclOperation.READ)
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build();
    }

    private void sendAndReceiveMessages(TestStorage testStorage, KafkaProducerConsumer clients, int messageCount) {
        clients.setMessageCount(messageCount);
        clients.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());
        KubeResourceManager.get().createResourceWithWait(clients.getProducer().getJob(), clients.getConsumer().getJob());
        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), messageCount);
    }

    private void sendMessages(TestStorage testStorage, KafkaProducerConsumer clients, int messageCount) {
        clients.setMessageCount(messageCount);
        KubeResourceManager.get().createResourceWithWait(clients.getProducer().getJob());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getProducerName(), messageCount);
    }

    private void consumeMessages(TestStorage testStorage, KafkaProducerConsumer clients, int messageCount) {
        clients.setMessageCount(messageCount);
        clients.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());
        KubeResourceManager.get().createResourceWithWait(clients.getConsumer().getJob());
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), messageCount);
    }


    /**
     * Generates the combinations of encryption and authentication types for testing.
     *
     * @return  A stream of arguments containing encryption and authentication type combinations.
     */
    private Stream<Arguments> securityConfigurationCombos() {
        return Stream.of(
                Arguments.of(ClusterSecurityEncryptionType.STRIMZI_TLS, ClusterSecurityAuthenticationType.NONE),
                Arguments.of(ClusterSecurityEncryptionType.NONE, ClusterSecurityAuthenticationType.NONE)
        );
    }

    /**
     * Generates the Cluster Security annotation for the given encryption and authentication types
     *
     * @param encryption        Encryption type
     * @param authentication    Authentication type
     *
     * @return  Cluster Security annotation as a JSON string
     */
    private String clusterSecurityAnnotation(ClusterSecurityEncryptionType encryption, ClusterSecurityAuthenticationType authentication) {
        return "{\"encryption\":{\"type\":\"" + encryption.toValue() + "\"},\"authentication\":{\"type\":\"" + authentication.toValue() + "\"}}";
    }

    /**
     * Checks that the cluster security status corresponds to the desired encryption and authentication types.
     *
     * @param testStorage       Test storage containing cluster information
     * @param encryption        Expected encryption type
     * @param authentication    Expected authentication type
     */
    private void assertClusterSecurityStatus(TestStorage testStorage, ClusterSecurityEncryptionType encryption, ClusterSecurityAuthenticationType authentication) {
        assertThat(CrdClients.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName())
                .get().getStatus().getClusterSecurity(),
            is(Map.of(
                "encryption", Map.of("type", encryption.toValue()),
                "authentication", Map.of("type", authentication.toValue())
            )));
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();
    }
}
