/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.metrics.MetricsCollector;
import io.strimzi.systemtest.resources.ComponentType;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NETWORKPOLICIES_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(NETWORKPOLICIES_SUPPORTED)
@Tag(REGRESSION)
public class NetworkPoliciesST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(NetworkPoliciesST.class);

    @IsolatedTest("Specific Cluster Operator for test case")
    @Tag(INTERNAL_CLIENTS_USED)
    void testNetworkPoliciesWithPlainListener(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        final String topic0 = "topic-example-0";
        final String topic1 = "topic-example-1";

        final String deniedProducerName = "denied-" + testStorage.getProducerName();
        final String deniedConsumerName = "denied-" + testStorage.getConsumerName();

        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .createInstallation()
            .runInstallation();

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .withNetworkPolicyPeers(
                                new NetworkPolicyPeerBuilder()
                                    .withNewPodSelector()
                                        .addToMatchLabels("app", testStorage.getProducerName())
                                    .endPodSelector()
                                    .build(),
                                new NetworkPolicyPeerBuilder()
                                    .withNewPodSelector()
                                        .addToMatchLabels("app", testStorage.getConsumerName())
                                    .endPodSelector()
                                    .build()
                            )
                            .build())
                .endKafka()
                .withNewKafkaExporter()
                .endKafkaExporter()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), topic0, testStorage.getNamespaceName()).build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), topic1, testStorage.getNamespaceName()).build()
        );

        final String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();
        NetworkPolicyResource.allowNetworkPolicySettingsForKafkaExporter(extensionContext, testStorage.getClusterName(), testStorage.getNamespaceName());

        LOGGER.info("Verifying that producer/consumer: {}/{} are able to exchange messages", testStorage.getProducerName(), testStorage.getConsumerName());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(topic0)
            .withUsername(testStorage.getUsername())
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerScramShaPlainStrimzi(), kafkaClients.consumerScramShaPlainStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withProducerName(deniedProducerName)
            .withConsumerName(deniedConsumerName)
            .withTopicName(topic1)
            .build();

        LOGGER.info("Verifying that producer/consumer: {}/{} are not able to exchange messages", deniedProducerName, deniedConsumerName);
        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsTimeout(testStorage);

        LOGGER.info("Check metrics exported by KafkaExporter");

        MetricsCollector metricsCollector = new MetricsCollector.Builder()
            .withScraperPodName(scraperPodName)
            .withComponentName(testStorage.getClusterName())
            .withComponentType(ComponentType.KafkaExporter)
            .build();

        metricsCollector.collectMetricsFromPods();
        assertThat("KafkaExporter metrics should be non-empty", metricsCollector.getCollectedData().size() > 0);
        for (Map.Entry<String, String> entry : metricsCollector.getCollectedData().entrySet()) {
            assertThat("Value from collected metric should be non-empty", !entry.getValue().isEmpty());
            assertThat("Metrics doesn't contain specific values", entry.getValue().contains("kafka_consumergroup_current_offset"));
            assertThat("Metrics doesn't contain specific values", entry.getValue().contains("kafka_topic_partitions{topic=\"" + topic0 + "\"} 1"));
            assertThat("Metrics doesn't contain specific values", entry.getValue().contains("kafka_topic_partitions{topic=\"" + topic1 + "\"} 1"));
        }
    }

    @IsolatedTest("Specific Cluster Operator for test case")
    @Tag(INTERNAL_CLIENTS_USED)
    void testNetworkPoliciesWithTlsListener(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        final String topic0 = "topic-example-0";
        final String topic1 = "topic-example-1";

        final String deniedProducerName = "denied-" + testStorage.getProducerName();
        final String deniedConsumerName = "denied-" + testStorage.getConsumerName();

        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .createInstallation()
            .runInstallation();

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .withNetworkPolicyPeers(
                                new NetworkPolicyPeerBuilder()
                                    .withNewPodSelector()
                                        .addToMatchLabels("app", testStorage.getProducerName())
                                    .endPodSelector()
                                    .build(),
                                new NetworkPolicyPeerBuilder()
                                    .withNewPodSelector()
                                        .addToMatchLabels("app", testStorage.getConsumerName())
                                    .endPodSelector()
                                    .build()
                            )
                            .build())
                .endKafka()
            .endSpec()
            .build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), topic0, testStorage.getNamespaceName()).build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), topic1, testStorage.getNamespaceName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage).build()
        );

        LOGGER.info("Verifying that producer/consumer: {}/{} are able to exchange messages", testStorage.getProducerName(), testStorage.getConsumerName());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(topic0)
            .withUsername(testStorage.getUsername())
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerScramShaTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerScramShaTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withProducerName(deniedProducerName)
            .withConsumerName(deniedConsumerName)
            .withTopicName(topic1)
            .build();

        LOGGER.info("Verifying that producer/consumer: {}/{} are not able to exchange messages", deniedProducerName, deniedConsumerName);
        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsTimeout(testStorage);
    }

    @IsolatedTest("Specific Cluster Operator for test case")
    void testNPWhenOperatorIsInSameNamespaceAsOperand(ExtensionContext extensionContext) {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .createInstallation()
            .runInstallation();

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        checkNetworkPoliciesInNamespace(clusterName, Constants.TEST_SUITE_NAMESPACE);
        changeKafkaConfigurationAndCheckObservedGeneration(clusterName, Constants.TEST_SUITE_NAMESPACE);
    }

    @IsolatedTest("Specific Cluster Operator for test case")
    void testNPWhenOperatorIsInDifferentNamespaceThanOperand(ExtensionContext extensionContext) {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String secondNamespace = "second-" + Constants.TEST_SUITE_NAMESPACE;

        Map<String, String> labels = new HashMap<>();
        labels.put("my-label", "my-value");

        EnvVar operatorLabelsEnv = new EnvVarBuilder()
                .withName("STRIMZI_OPERATOR_NAMESPACE_LABELS")
                .withValue(labels.toString().replaceAll("\\{|}", ""))
                .build();

        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .withBindingsNamespaces(Arrays.asList(Constants.TEST_SUITE_NAMESPACE, secondNamespace))
            .withExtraEnvVars(Collections.singletonList(operatorLabelsEnv))
            .createInstallation()
            .runInstallation();

        Namespace actualNamespace = kubeClient().getClient().namespaces().withName(Constants.TEST_SUITE_NAMESPACE).get();
        kubeClient().getClient().namespaces().withName(Constants.TEST_SUITE_NAMESPACE).edit(ns -> new NamespaceBuilder(actualNamespace)
            .editOrNewMetadata()
                .addToLabels(labels)
            .endMetadata()
            .build());

        cluster.setNamespace(secondNamespace);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaWithMetrics(clusterName, secondNamespace, 3, 3)
            .editMetadata()
                .addToLabels(labels)
            .endMetadata()
            .build());

        checkNetworkPoliciesInNamespace(clusterName, secondNamespace);

        changeKafkaConfigurationAndCheckObservedGeneration(clusterName, secondNamespace);
    }

    @IsolatedTest("Specific Cluster Operator for test case")
    @Tag(CRUISE_CONTROL)
    void testNPGenerationEnvironmentVariable(ExtensionContext extensionContext) {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        EnvVar networkPolicyGenerationEnv = new EnvVarBuilder()
            .withName("STRIMZI_NETWORK_POLICY_GENERATION")
            .withValue("false")
            .build();

        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .withExtraEnvVars(Collections.singletonList(networkPolicyGenerationEnv))
            .createInstallation()
            .runInstallation();

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3)
            .build());

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnect(clusterName, Constants.TEST_SUITE_NAMESPACE, 1)
                .build());

        List<NetworkPolicy> networkPolicyList = kubeClient().getClient().network().networkPolicies().list().getItems().stream()
            .filter(item -> item.getMetadata().getLabels() != null && item.getMetadata().getLabels().containsKey("strimzi.io/name"))
            .collect(Collectors.toList());

        assertThat("List of NetworkPolicies generated by Strimzi is not empty.", networkPolicyList, is(Collections.EMPTY_LIST));
    }

    void checkNetworkPoliciesInNamespace(String clusterName, String namespace) {
        List<NetworkPolicy> networkPolicyList = new ArrayList<>(kubeClient().getClient().network().networkPolicies().inNamespace(namespace).list().getItems());

        assertNotNull(networkPolicyList.stream().filter(networkPolicy ->  networkPolicy.getMetadata().getName().contains(KafkaResources.kafkaNetworkPolicyName(clusterName))).findFirst());
        assertNotNull(networkPolicyList.stream().filter(networkPolicy ->  networkPolicy.getMetadata().getName().contains(KafkaResources.zookeeperNetworkPolicyName(clusterName))).findFirst());
        assertNotNull(networkPolicyList.stream().filter(networkPolicy ->  networkPolicy.getMetadata().getName().contains(KafkaResources.entityOperatorDeploymentName(clusterName))).findFirst());

        // if KE is enabled
        if (KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getSpec().getKafkaExporter() != null) {
            assertNotNull(networkPolicyList.stream().filter(networkPolicy ->  networkPolicy.getMetadata().getName().contains(KafkaExporterResources.deploymentName(clusterName))).findFirst());
        }
    }

    void changeKafkaConfigurationAndCheckObservedGeneration(String clusterName, String namespace) {
        long observedGen = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getStatus().getObservedGeneration();
        KafkaUtils.updateConfigurationWithStabilityWait(namespace, clusterName, "log.message.timestamp.type", "LogAppendTime");

        assertThat(KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getStatus().getObservedGeneration(), is(not(observedGen)));
    }
}
