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
import io.skodjob.testframe.MetricsCollector;
import io.skodjob.testframe.metrics.Gauge;
import io.skodjob.testframe.metrics.Metric;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.SkipDefaultNetworkPolicyCreation;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.metrics.KafkaExporterMetricsComponent;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.specific.MetricsUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.NETWORKPOLICIES_SUPPORTED;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(NETWORKPOLICIES_SUPPORTED)
@Tag(REGRESSION)
public class NetworkPoliciesST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(NetworkPoliciesST.class);

    /**
     * @description This test case verifies network policies if Cluster Operator is in same namespace as Operands.
     *
     * @steps
     *  1. - Deploy Kafka with plain and tls listeners, having set network policies for respective clients
     *     - Kafka is deployed
     *  2. - Deploy KafkaTopics, metric scraper Pod and all Kafka clients
     *     - All resources are deployed
     *  3. - Produce and Consume messages with clients using respective listeners
     *     - Clients Fails or Succeed in connecting to the Kafka based on specified network policies
     *  4. - Scrape exported metrics regarding present Kafka Topics
     *     - Metrics are present and have expected values
     *  5. - Change log configuration in Kafka config
     *     - Observed generation is higher than before the config change and Rolling Update did not take place.
     *
     * @usecase
     *  - network-policy
     *  - listeners
     *  - metrics
     */
    @IsolatedTest("Specific Cluster Operator for test case")
    @SuppressWarnings("checkstyle:MethodLength")
    void testNetworkPoliciesOnListenersWhenOperatorIsInSameNamespaceAsOperands() {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final String topicNameAccessedTls = testStorage.getTopicName() + "-accessed-tls";
        final String topicNameAccessedPlain = testStorage.getTopicName() + "-accessed-plain";
        final String topicNameDeniedTls = testStorage.getTopicName() + "-denied-tls";
        final String topicNameDeniedPlain = testStorage.getTopicName() + "-denied-plain";

        final String producerNameAccessedTls = testStorage.getProducerName() + "-accessed-tls";
        final String producerNameAccessedPlain = testStorage.getProducerName() + "-accessed-plain";
        final String consumerNameAccessedTls = testStorage.getConsumerName() + "-accessed-tls";
        final String consumerNameAccessedPlain = testStorage.getConsumerName() + "-accessed-plain";

        final String producerNameDeniedTls = testStorage.getProducerName() + "-denied-tls";
        final String producerNameDeniedPlain = testStorage.getProducerName() + "-denied-plain";
        final String consumerNameDeniedTls = testStorage.getConsumerName() + "-denied-tls";
        final String consumerNameDeniedPlain = testStorage.getConsumerName() + "-denied-plain";

        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withNamespaceName(testStorage.getNamespaceName())
                .build()
            )
            .install();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editSpec()
                .editKafka()
                    .withListeners(
                        new GenericKafkaListenerBuilder()
                            .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .withNetworkPolicyPeers(
                                new NetworkPolicyPeerBuilder()
                                    .withNewPodSelector()
                                        .addToMatchLabels("app", producerNameAccessedPlain)
                                    .endPodSelector()
                                    .build(),
                                new NetworkPolicyPeerBuilder()
                                    .withNewPodSelector()
                                        .addToMatchLabels("app", consumerNameAccessedPlain)
                                    .endPodSelector()
                                    .build()
                            )
                            .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .withNetworkPolicyPeers(
                                new NetworkPolicyPeerBuilder()
                                    .withNewPodSelector()
                                    .addToMatchLabels("app", producerNameAccessedTls)
                                    .endPodSelector()
                                    .build(),
                                new NetworkPolicyPeerBuilder()
                                    .withNewPodSelector()
                                    .addToMatchLabels("app", consumerNameAccessedTls)
                                    .endPodSelector()
                                    .build()
                            )
                            .build()
                    )
                .endKafka()
                .withNewKafkaExporter()
                .endKafkaExporter()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), topicNameAccessedPlain, testStorage.getClusterName()).build(),
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), topicNameAccessedTls, testStorage.getClusterName()).build(),
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), topicNameDeniedPlain, testStorage.getClusterName()).build(),
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), topicNameDeniedTls, testStorage.getClusterName()).build()
        );

        LOGGER.info("Initialize producers and consumers with access to the Kafka using plain and tls listeners");
        final KafkaClients kafkaClientsWithAccessPlain = ClientUtils.getInstantScramShaOverPlainClientBuilder(testStorage)
            .withProducerName(producerNameAccessedPlain)
            .withConsumerName(consumerNameAccessedPlain)
            .withTopicName(topicNameAccessedPlain)
            .build();
        final KafkaClients kafkaClientsWithAccessTls = ClientUtils.getInstantScramShaOverTlsClientBuilder(testStorage)
            .withProducerName(producerNameAccessedTls)
            .withConsumerName(consumerNameAccessedTls)
            .withTopicName(topicNameAccessedTls)
            .build();

        LOGGER.info("Initialize producers and consumers without access (denied) to the Kafka using plain and tls listeners");
        final KafkaClients kafkaClientsWithoutAccessPlain = ClientUtils.getInstantScramShaOverPlainClientBuilder(testStorage)
            .withProducerName(producerNameDeniedPlain)
            .withConsumerName(consumerNameDeniedPlain)
            .withTopicName(topicNameDeniedPlain)
            .build();
        final KafkaClients kafkaClientsWithoutAccessTls = ClientUtils.getInstantScramShaOverTlsClientBuilder(testStorage)
            .withProducerName(producerNameDeniedTls)
            .withConsumerName(consumerNameDeniedTls)
            .withTopicName(topicNameDeniedTls)
            .build();

        LOGGER.info("Deploy all initialized clients");
        KubeResourceManager.get().createResourceWithWait(
            kafkaClientsWithAccessPlain.producerScramShaPlainStrimzi(),
            kafkaClientsWithAccessPlain.consumerScramShaPlainStrimzi(),
            kafkaClientsWithAccessTls.producerScramShaTlsStrimzi(testStorage.getClusterName()),
            kafkaClientsWithAccessTls.consumerScramShaTlsStrimzi(testStorage.getClusterName()),
            kafkaClientsWithoutAccessPlain.producerScramShaPlainStrimzi(),
            kafkaClientsWithoutAccessPlain.consumerScramShaPlainStrimzi(),
            kafkaClientsWithoutAccessTls.producerScramShaTlsStrimzi(testStorage.getClusterName()),
            kafkaClientsWithoutAccessTls.consumerScramShaTlsStrimzi(testStorage.getClusterName())
        );

        LOGGER.info("Verifying that clients: {}, {}, {}, {} are all allowed to communicate", producerNameAccessedPlain, consumerNameAccessedPlain, producerNameAccessedTls, consumerNameAccessedTls);
        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), consumerNameAccessedPlain, producerNameAccessedPlain, testStorage.getMessageCount());
        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), consumerNameAccessedTls, producerNameAccessedTls, testStorage.getMessageCount());

        LOGGER.info("Verifying that clients: {}, {}, {}, {} are denied to communicate", producerNameDeniedPlain, consumerNameDeniedPlain, producerNameDeniedTls, consumerNameDeniedTls);
        ClientUtils.waitForClientsTimeout(testStorage.getNamespaceName(), consumerNameDeniedPlain, producerNameDeniedPlain, testStorage.getMessageCount());
        ClientUtils.waitForClientsTimeout(testStorage.getNamespaceName(), consumerNameDeniedTls, producerNameDeniedTls, testStorage.getMessageCount());

        LOGGER.info("Check metrics exported by KafkaExporter");

        final String scraperPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();
        MetricsCollector metricsCollector = new MetricsCollector.Builder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withScraperPodName(scraperPodName)
            .withComponent(KafkaExporterMetricsComponent.create(testStorage.getNamespaceName(), testStorage.getClusterName()))
            .build();

        metricsCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
        assertThat("KafkaExporter metrics should be non-empty", metricsCollector.getCollectedData().size() > 0);
        for (Map.Entry<String, List<Metric>> entry : metricsCollector.getCollectedData().entrySet()) {
            assertThat("Value from collected metric should be non-empty", !entry.getValue().isEmpty());
            MetricsUtils.assertContainsMetric(entry.getValue(), "kafka_consumergroup_current_offset");

            List<Metric> topicPartitionsMetrics = entry.getValue().stream().filter(metrics -> metrics.getName().contains("kafka_topic_partitions")).toList();

            Gauge topicAccessedTlsMetric = (Gauge) topicPartitionsMetrics.stream().filter(metric -> metric.getLabels().get("topic").equals(topicNameAccessedTls)).findFirst().get();
            Gauge topicAccessedPlainMetric = (Gauge) topicPartitionsMetrics.stream().filter(metric -> metric.getLabels().get("topic").equals(topicNameAccessedPlain)).findFirst().get();

            assertThat(topicAccessedTlsMetric.getValue(), is(1.0));
            assertThat(topicAccessedPlainMetric.getValue(), is(1.0));
        }

        checkNetworkPoliciesInNamespace(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
        changeKafkaConfigurationAndCheckObservedGeneration(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
    }

    @IsolatedTest("Specific Cluster Operator for test case")
    void testNPWhenOperatorIsInDifferentNamespaceThanOperand() {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String secondNamespace = "second-" + Environment.TEST_SUITE_NAMESPACE;

        Map<String, String> labels = new HashMap<>();
        labels.put("my-label", "my-value");

        EnvVar operatorLabelsEnv = new EnvVarBuilder()
                .withName("STRIMZI_OPERATOR_NAMESPACE_LABELS")
                .withValue(labels.toString().replaceAll("\\{|}", ""))
                .build();

        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
                .withNamespacesToWatch(TestConstants.WATCH_ALL_NAMESPACES)
                .withExtraEnvVars(operatorLabelsEnv)
                .build()
            )
            .install();

        Namespace actualNamespace = KubeResourceManager.get().kubeClient().getClient().namespaces().withName(Environment.TEST_SUITE_NAMESPACE).get();
        KubeResourceManager.get().kubeClient().getClient().namespaces().withName(Environment.TEST_SUITE_NAMESPACE).edit(ns -> new NamespaceBuilder(actualNamespace)
            .editOrNewMetadata()
                .addToLabels(labels)
            .endMetadata()
            .build());

        KubeResourceManager.get().createResourceWithWait(
            new NamespaceBuilder()
                .withNewMetadata()
                    .withName(secondNamespace)
                .endMetadata()
                .build()
        );
        cluster.setNamespace(secondNamespace);

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(secondNamespace, testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(secondNamespace, testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafkaMetricsConfigMap(secondNamespace, testStorage.getClusterName()),
            KafkaTemplates.kafkaWithMetrics(secondNamespace, testStorage.getClusterName(), 3)
            .editMetadata()
                .addToLabels(labels)
            .endMetadata()
            .build()
        );

        checkNetworkPoliciesInNamespace(secondNamespace, testStorage.getClusterName());

        changeKafkaConfigurationAndCheckObservedGeneration(secondNamespace, testStorage.getClusterName());
    }

    @IsolatedTest("Specific Cluster Operator for test case")
    @Tag(CRUISE_CONTROL)
    @SkipDefaultNetworkPolicyCreation("NetworkPolicy generation from CO is disabled in this test, resulting in problems with connection" +
        " in case of that DENY ALL global NetworkPolicy is used")
    void testNPGenerationEnvironmentVariable() {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        EnvVar networkPolicyGenerationEnv = new EnvVarBuilder()
            .withName("STRIMZI_NETWORK_POLICY_GENERATION")
            .withValue("false")
            .build();

        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
                .withExtraEnvVars(networkPolicyGenerationEnv)
                .build()
            )
            .install();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), 3)
            .build());

        KubeResourceManager.get().createResourceWithWait(KafkaConnectTemplates.kafkaConnect(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), 1)
                .build());

        List<NetworkPolicy> networkPolicyList = KubeResourceManager.get().kubeClient().getClient().network().networkPolicies().list().getItems().stream()
            .filter(item -> item.getMetadata().getLabels() != null && item.getMetadata().getLabels().containsKey("strimzi.io/name"))
            .collect(Collectors.toList());

        assertThat("List of NetworkPolicies generated by Strimzi is not empty.", networkPolicyList, is(Collections.EMPTY_LIST));
    }

    void checkNetworkPoliciesInNamespace(String namespaceName, String clusterName) {
        List<NetworkPolicy> networkPolicyList = new ArrayList<>(KubeResourceManager.get().kubeClient().getClient().network().networkPolicies().inNamespace(namespaceName).list().getItems());

        assertNotNull(networkPolicyList.stream().filter(networkPolicy ->  networkPolicy.getMetadata().getName().contains(KafkaResources.kafkaNetworkPolicyName(clusterName))).findFirst());
        assertNotNull(networkPolicyList.stream().filter(networkPolicy ->  networkPolicy.getMetadata().getName().contains(KafkaResources.entityOperatorDeploymentName(clusterName))).findFirst());

        // if KE is enabled
        if (CrdClients.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getSpec().getKafkaExporter() != null) {
            assertNotNull(networkPolicyList.stream().filter(networkPolicy ->  networkPolicy.getMetadata().getName().contains(KafkaExporterResources.componentName(clusterName))).findFirst());
        }
    }

    void changeKafkaConfigurationAndCheckObservedGeneration(String namespaceName, String clusterName) {
        long observedGen = CrdClients.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getObservedGeneration();
        KafkaUtils.updateConfigurationWithStabilityWait(namespaceName, clusterName, "log.message.timestamp.type", "LogAppendTime");

        assertThat(CrdClients.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getObservedGeneration(), is(not(observedGen)));
    }
}
