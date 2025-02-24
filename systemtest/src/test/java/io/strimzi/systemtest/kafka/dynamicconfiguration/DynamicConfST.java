/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka.dynamicconfiguration;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpec;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.TestTags.DYNAMIC_CONFIGURATION;
import static io.strimzi.systemtest.TestTags.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestTags.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.ROLLING_UPDATE;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
@Tag(DYNAMIC_CONFIGURATION)
@SuiteDoc(
    description = @Desc("Responsible for verifying that changes in dynamic Kafka configuration do not trigger a rolling update."),
    beforeTestSteps = {
        @Step(value = "Deploy the cluster operator.", expected = "Cluster operator is installed successfully.")
    }
)
public class DynamicConfST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(DynamicConfST.class);
    private static final int KAFKA_REPLICAS = 3;

    private Map<String, Object> kafkaConfig;

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @TestDoc(
        description = @Desc("Test for verifying dynamic configuration changes in a Kafka cluster with multiple clusters in one namespace."),
        steps = {
            @Step(value = "Deep copy shared Kafka configuration.", expected = "Configuration map is duplicated with deep copy."),
            @Step(value = "Create resources with wait.", expected = "Resources are created and ready."),
            @Step(value = "Create scraper pod.", expected = "Scraper pod is created."),
            @Step(value = "Retrieve and verify Kafka configurations from ConfigMaps.", expected = "Configurations meet expected values."),
            @Step(value = "Retrieve Kafka broker configuration via CLI.", expected = "Dynamic configurations are retrieved."),
            @Step(value = "Update Kafka configuration for unclean leader election.", expected = "Configuration is updated and verified for dynamic property."),
            @Step(value = "Verify updated Kafka configurations.", expected = "Updated configurations are persistent and correct.")
        },
        labels = {
            @Label(value = TestDocsLabels.DYNAMIC_CONFIGURATION),
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testSimpleDynamicConfiguration() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        Map<String, Object> deepCopyOfSharedKafkaConfig = kafkaConfig.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), KAFKA_REPLICAS).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), KAFKA_REPLICAS)
            .editSpec()
                .editKafka()
                    .withConfig(deepCopyOfSharedKafkaConfig)
                .endKafka()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, testStorage.getScraperName()).build()
        );

        String scraperPodName = kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, testStorage.getScraperName()).get(0).getMetadata().getName();
        String brokerPodName = KubeClusterResource.kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        int podNum = KafkaResource.getPodNumFromPodName(testStorage.getBrokerComponentName(), brokerPodName);

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName())) {
            String kafkaConfiguration = kubeClient().getConfigMap(Environment.TEST_SUITE_NAMESPACE, cmName).getData().get("server.config");
            assertThat(kafkaConfiguration, containsString("offsets.topic.replication.factor=1"));
            assertThat(kafkaConfiguration, containsString("transaction.state.log.replication.factor=1"));
        }

        String kafkaConfigurationFromPod = KafkaCmdClient.describeKafkaBrokerUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum);
        assertThat(kafkaConfigurationFromPod, containsString("Dynamic configs for broker 0 are:\n"));

        deepCopyOfSharedKafkaConfig.put("unclean.leader.election.enable", true);

        updateAndVerifyDynConf(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), deepCopyOfSharedKafkaConfig);

        // Wait until the configuration is properly set and returned by Kafka Admin API
        StUtils.waitUntilSupplierIsSatisfied("unclean.leader.election.enable=true is available in Broker config", () ->
            KafkaCmdClient.describeKafkaBrokerUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum).contains("unclean.leader.election.enable=" + true));

        LOGGER.info("Verifying values after update");

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName())) {
            String kafkaConfiguration = kubeClient().getConfigMap(Environment.TEST_SUITE_NAMESPACE, cmName).getData().get("server.config");
            assertThat(kafkaConfiguration, containsString("offsets.topic.replication.factor=1"));
            assertThat(kafkaConfiguration, containsString("transaction.state.log.replication.factor=1"));
            assertThat(kafkaConfiguration, containsString("unclean.leader.election.enable=true"));
        }
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(ROLLING_UPDATE)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @TestDoc(
        description = @Desc("Ensures that updating to an external listener causes a rolling restart of the Kafka brokers."),
        steps = {
            @Step(value = "Create Kafka cluster with internal and external listeners.", expected = "Kafka cluster is created with the specified listeners."),
            @Step(value = "Verify initial configurations are correctly set in the broker.", expected = "Initial broker configurations are verified."),
            @Step(value = "Update Kafka cluster to change listener types.", expected = "Change in listener types triggers rolling update."),
            @Step(value = "Verify the rolling restart is successful.", expected = "All broker nodes successfully rolled and Kafka configuration updated.")
        },
        labels = {
            @Label(value = TestDocsLabels.DYNAMIC_CONFIGURATION),
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testUpdateToExternalListenerCausesRollingRestart() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        Map<String, Object> deepCopyOfShardKafkaConfig = kafkaConfig.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), KAFKA_REPLICAS).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), KAFKA_REPLICAS)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
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
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(false)
                                .build())
                    .withConfig(deepCopyOfShardKafkaConfig)
                .endKafka()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, testStorage.getScraperName()).build()
        );

        String scraperPodName = kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, testStorage.getScraperName()).get(0).getMetadata().getName();
        String brokerPodName = KubeClusterResource.kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        int podNum = KafkaResource.getPodNumFromPodName(testStorage.getBrokerComponentName(), brokerPodName);

        String kafkaConfigurationFromPod = KafkaCmdClient.describeKafkaBrokerUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum);

        assertThat(kafkaConfigurationFromPod, containsString("Dynamic configs for broker " + podNum + " are:\n"));

        deepCopyOfShardKafkaConfig.put("unclean.leader.election.enable", true);

        updateAndVerifyDynConf(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), deepCopyOfShardKafkaConfig);

        // Wait until the configuration is properly set and returned by Kafka Admin API
        StUtils.waitUntilSupplierIsSatisfied("unclean.leader.election.enable=true is available in Broker config", () ->
            KafkaCmdClient.describeKafkaBrokerUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum).contains("unclean.leader.election.enable=" + true));

        // Edit listeners - this should cause RU (because of new crts)
        Map<String, String> brokerPods = PodUtils.podSnapshot(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector());
        LOGGER.info("Updating listeners of Kafka cluster");

        KafkaResource.replaceKafkaResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), k -> {
            k.getSpec().getKafka().setListeners(Arrays.asList(
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
                    .build(),
                new GenericKafkaListenerBuilder()
                    .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                    .withPort(9094)
                    .withType(KafkaListenerType.NODEPORT)
                    .withTls(true)
                    .build()
            ));
        });

        RollingUpdateUtils.waitTillComponentHasRolled(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector(), KAFKA_REPLICAS, brokerPods);
        assertThat(RollingUpdateUtils.componentHasRolled(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector(), brokerPods), is(true));

        kafkaConfigurationFromPod = KafkaCmdClient.describeKafkaBrokerUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum);
        assertThat(kafkaConfigurationFromPod, containsString("Dynamic configs for broker " + podNum + " are:\n"));

        deepCopyOfShardKafkaConfig.put("compression.type", "snappy");

        updateAndVerifyDynConf(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), deepCopyOfShardKafkaConfig);

        // Wait until the configuration is properly set and returned by Kafka Admin API
        StUtils.waitUntilSupplierIsSatisfied("compression.type=snappy is set in Kafka", () ->
            KafkaCmdClient.describeKafkaBrokerUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum).contains("compression.type=snappy"));

        kafkaConfigurationFromPod = KafkaCmdClient.describeKafkaBrokerUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum);
        assertThat(kafkaConfigurationFromPod, containsString("Dynamic configs for broker " + podNum + " are:\n"));

        deepCopyOfShardKafkaConfig.put("unclean.leader.election.enable", true);

        updateAndVerifyDynConf(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), deepCopyOfShardKafkaConfig);

        // Wait until the configuration is properly set and returned by Kafka Admin API
        StUtils.waitUntilSupplierIsSatisfied("unclean.leader.election.enable=true is available in Broker config", () ->
                KafkaCmdClient.describeKafkaBrokerUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum).contains("unclean.leader.election.enable=" + true));

        // Remove external listeners (node port) - this should cause RU (we need to update advertised.listeners)
        // Other external listeners cases are rolling because of crts
        brokerPods = PodUtils.podSnapshot(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector());
        LOGGER.info("Updating listeners of Kafka cluster");

        KafkaResource.replaceKafkaResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), k -> {
            k.getSpec().getKafka().setListeners(Arrays.asList(
                new GenericKafkaListenerBuilder()
                    .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                    .withPort(9092)
                    .withType(KafkaListenerType.INTERNAL)
                    .withTls(false)
                    .build(),
                new GenericKafkaListenerBuilder()
                    .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                    .withPort(9094)
                    .withType(KafkaListenerType.NODEPORT)
                    .withTls(true)
                    .build()
            ));
        });

        RollingUpdateUtils.waitTillComponentHasRolled(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector(), KAFKA_REPLICAS, brokerPods);
        assertThat(RollingUpdateUtils.componentHasRolled(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector(), brokerPods), is(true));

        kafkaConfigurationFromPod = KafkaCmdClient.describeKafkaBrokerUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum);
        assertThat(kafkaConfigurationFromPod, containsString("Dynamic configs for broker " + podNum + " are:\n"));

        deepCopyOfShardKafkaConfig.put("unclean.leader.election.enable", false);

        updateAndVerifyDynConf(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), deepCopyOfShardKafkaConfig);

        // Wait until the configuration is properly set and returned by Kafka Admin API
        StUtils.waitUntilSupplierIsSatisfied("unclean.leader.election.enable=false is set in Kafka", () ->
            KafkaCmdClient.describeKafkaBrokerUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum).contains("unclean.leader.election.enable=" + false));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(ROLLING_UPDATE)
    @TestDoc(
        description = @Desc("Test validating that updating Kafka cluster listeners to use external clients causes a rolling restart."),
        steps = {
            @Step(value = "Setup initial Kafka cluster and resources.", expected = "Kafka cluster and resources are successfully created."),
            @Step(value = "Create external Kafka clients and verify message production/consumption on plain listener.", expected = "Messages are successfully produced and consumed using plain listener."),
            @Step(value = "Attempt to produce/consume messages using TLS listener before update.", expected = "Exception is thrown because the listener is plain."),
            @Step(value = "Update Kafka cluster to use external TLS listener.", expected = "Kafka cluster is updated and rolling restart occurs."),
            @Step(value = "Verify message production/consumption using TLS listener after update.", expected = "Messages are successfully produced and consumed using TLS listener."),
            @Step(value = "Attempt to produce/consume messages using plain listener after TLS update.", expected = "Exception is thrown because the listener is TLS."),
            @Step(value = "Revert Kafka cluster listener to plain.", expected = "Kafka cluster listener is reverted and rolling restart occurs."),
            @Step(value = "Verify message production/consumption on plain listener after reverting.", expected = "Messages are successfully produced and consumed using plain listener."),
            @Step(value = "Attempt to produce/consume messages using TLS listener after reverting.", expected = "Exception is thrown because the listener is plain.")
        },
        labels = {
            @Label(value = TestDocsLabels.DYNAMIC_CONFIGURATION),
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testUpdateToExternalListenerCausesRollingRestartUsingExternalClients() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        Map<String, Object> deepCopyOfShardKafkaConfig = kafkaConfig.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), KAFKA_REPLICAS).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), KAFKA_REPLICAS)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(false)
                        .build())
                    .withConfig(deepCopyOfShardKafkaConfig)
                .endKafka()
            .endSpec()
            .build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), testStorage.getClusterName()).build());
        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaUsername(), testStorage.getClusterName()).build());

        ExternalKafkaClient externalKafkaClientTls = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withClusterName(testStorage.getClusterName())
            .withMessageCount(testStorage.getMessageCount())
            .withKafkaUsername(testStorage.getKafkaUsername())
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        ExternalKafkaClient externalKafkaClientPlain = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withClusterName(testStorage.getClusterName())
            .withMessageCount(testStorage.getMessageCount())
            .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClientPlain.verifyProducedAndConsumedMessages(
            externalKafkaClientPlain.sendMessagesPlain(),
            externalKafkaClientPlain.receiveMessagesPlain()
        );

        assertThrows(Exception.class, () -> {
            externalKafkaClientTls.sendMessagesTls();
            externalKafkaClientTls.receiveMessagesTls();
            LOGGER.error("Producer & Consumer did not send and receive messages because external listener is set to plain communication");
        });

        LOGGER.info("Updating listeners of Kafka cluster");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), k -> {
            k.getSpec().getKafka().setListeners(Arrays.asList(
                new GenericKafkaListenerBuilder()
                    .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                    .withPort(9093)
                    .withType(KafkaListenerType.INTERNAL)
                    .withTls(true)
                    .build(),
                new GenericKafkaListenerBuilder()
                    .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                    .withPort(9094)
                    .withType(KafkaListenerType.NODEPORT)
                    .withTls(true)
                    .withNewKafkaListenerAuthenticationTlsAuth()
                    .endKafkaListenerAuthenticationTlsAuth()
                    .build()
            ));
        });

        // TODO: remove it ?
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector(), KAFKA_REPLICAS, brokerPods);

        externalKafkaClientTls.verifyProducedAndConsumedMessages(
            externalKafkaClientTls.sendMessagesTls() + testStorage.getMessageCount(),
            externalKafkaClientTls.receiveMessagesTls()
        );

        assertThrows(Exception.class, () -> {
            externalKafkaClientPlain.sendMessagesPlain();
            externalKafkaClientPlain.receiveMessagesPlain();
            LOGGER.error("Producer & Consumer did not send and receive messages because external listener is set to tls communication");
        });

        LOGGER.info("Updating listeners of Kafka cluster");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), k -> {
            k.getSpec().getKafka().setListeners(Collections.singletonList(
                new GenericKafkaListenerBuilder()
                    .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                    .withPort(9094)
                    .withType(KafkaListenerType.NODEPORT)
                    .withTls(false)
                    .build()
            ));
        });

        RollingUpdateUtils.waitTillComponentHasRolled(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector(), KAFKA_REPLICAS, brokerPods);

        assertThrows(Exception.class, () -> {
            externalKafkaClientTls.sendMessagesTls();
            externalKafkaClientTls.receiveMessagesTls();
            LOGGER.error("Producer & Consumer did not send and receive messages because external listener is set to plain communication");
        });

        externalKafkaClientPlain.verifyProducedAndConsumedMessages(
            externalKafkaClientPlain.sendMessagesPlain() + testStorage.getMessageCount(),
            externalKafkaClientPlain.receiveMessagesPlain()
        );
    }

    /**
     * UpdateAndVerifyDynConf, change the kafka configuration and verify that no rolling update were triggered
     * @param namespaceName name of the namespace
     * @param kafkaConfig specific kafka configuration, which will be changed
     */
    private void updateAndVerifyDynConf(final String namespaceName, String clusterName, Map<String, Object> kafkaConfig) {
        LabelSelector brokerSelector = KafkaResource.getLabelSelector(clusterName, StrimziPodSetResource.getBrokerComponentName(clusterName));
        Map<String, String> brokerPods = PodUtils.podSnapshot(namespaceName, brokerSelector);

        LOGGER.info("Updating configuration of Kafka cluster");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(namespaceName, clusterName, k -> {
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            kafkaClusterSpec.setConfig(kafkaConfig);
        });

        PodUtils.verifyThatRunningPodsAreStable(namespaceName, StrimziPodSetResource.getBrokerComponentName(clusterName));
        assertThat(RollingUpdateUtils.componentHasRolled(namespaceName, brokerSelector, brokerPods), is(false));
    }

    @BeforeEach
    void setupEach() {
        kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "1");
        kafkaConfig.put("transaction.state.log.replication.factor", "1");
    }

    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();
    }
}
