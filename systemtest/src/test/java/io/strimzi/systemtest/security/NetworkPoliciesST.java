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
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.kubernetes.ClusterRoleBindingResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.kubernetes.ClusterRoleBindingTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.specific.MetricsUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NETWORKPOLICIES_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(NETWORKPOLICIES_SUPPORTED)
@Tag(REGRESSION)
public class NetworkPoliciesST extends AbstractST {
    public static final String NAMESPACE = "np-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(NetworkPoliciesST.class);

    @IsolatedTest("Specific cluster operator for test case")
    @Tag(INTERNAL_CLIENTS_USED)
    void testNetworkPoliciesWithPlainListener(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        installClusterOperator(extensionContext, NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT);

        String allowedKafkaClientsName = clusterName + "-" + Constants.KAFKA_CLIENTS + "-allow";
        String deniedKafkaClientsName = clusterName + "-" + Constants.KAFKA_CLIENTS + "-deny";
        Map<String, String> matchLabelForPlain = new HashMap<>();
        matchLabelForPlain.put("app", allowedKafkaClientsName);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .withNetworkPolicyPeers(
                                new NetworkPolicyPeerBuilder()
                                    .withNewPodSelector()
                                    .withMatchLabels(matchLabelForPlain)
                                    .endPodSelector()
                                    .build())
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
                .withNewKafkaExporter()
                .endKafkaExporter()
            .endSpec()
            .build());

        NetworkPolicyResource.allowNetworkPolicySettingsForKafkaExporter(extensionContext, clusterName);

        String topic0 = "topic-example-0";
        String topic1 = "topic-example-1";

        String userName = "user-example";
        KafkaUser kafkaUser = KafkaUserTemplates.scramShaUser(clusterName, userName).build();

        resourceManager.createResource(extensionContext, kafkaUser);
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topic0).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topic1).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, allowedKafkaClientsName, kafkaUser).build());

        String allowedKafkaClientsPodName = kubeClient().listPodsByPrefixInName(allowedKafkaClientsName).get(0).getMetadata().getName();

        LOGGER.info("Verifying that {} pod is able to exchange messages", allowedKafkaClientsPodName);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(allowedKafkaClientsPodName)
            .withTopicName(topic0)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, deniedKafkaClientsName, kafkaUser).build());

        String deniedKafkaClientsPodName = kubeClient().listPodsByPrefixInName(deniedKafkaClientsName).get(0).getMetadata().getName();

        InternalKafkaClient newInternalKafkaClient = internalKafkaClient.toBuilder()
            .withUsingPodName(deniedKafkaClientsPodName)
            .withTopicName(topic1)
            .build();

        LOGGER.info("Verifying that {} pod is not able to exchange messages", deniedKafkaClientsPodName);
        assertThrows(AssertionError.class, () ->  {
            newInternalKafkaClient.checkProducedAndConsumedMessages(
                newInternalKafkaClient.sendMessagesPlain(),
                newInternalKafkaClient.receiveMessagesPlain()
            );
        });

        LOGGER.info("Check metrics exported by Kafka Exporter");
        Map<String, String> kafkaExporterMetricsData = MetricsUtils.collectKafkaExporterPodsMetrics(allowedKafkaClientsPodName, clusterName);
        assertThat("Kafka Exporter metrics should be non-empty", kafkaExporterMetricsData.size() > 0);
        for (Map.Entry<String, String> entry : kafkaExporterMetricsData.entrySet()) {
            assertThat("Value from collected metric should be non-empty", !entry.getValue().isEmpty());
            assertThat("Metrics doesn't contain specific values", entry.getValue().contains("kafka_consumergroup_current_offset"));
            assertThat("Metrics doesn't contain specific values", entry.getValue().contains("kafka_topic_partitions{topic=\"" + topic0 + "\"} 1"));
            assertThat("Metrics doesn't contain specific values", entry.getValue().contains("kafka_topic_partitions{topic=\"" + topic1 + "\"} 1"));
        }
    }

    @IsolatedTest("Specific cluster operator for test case")
    @Tag(INTERNAL_CLIENTS_USED)
    void testNetworkPoliciesWithTlsListener(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        installClusterOperator(extensionContext, NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT);

        String allowedKafkaClientsName = clusterName + "-" + Constants.KAFKA_CLIENTS + "-allow";
        String deniedKafkaClientsName = clusterName + "-" + Constants.KAFKA_CLIENTS + "-deny";
        Map<String, String> matchLabelsForTls = new HashMap<>();
        matchLabelsForTls.put("app", allowedKafkaClientsName);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .withNetworkPolicyPeers(
                                new NetworkPolicyPeerBuilder()
                                    .withNewPodSelector()
                                    .withMatchLabels(matchLabelsForTls)
                                    .endPodSelector()
                                    .build())
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        String topic0 = "topic-example-0";
        String topic1 = "topic-example-1";

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topic0).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topic1).build());

        String userName = "user-example";
        KafkaUser kafkaUser = KafkaUserTemplates.scramShaUser(clusterName, userName).build();

        resourceManager.createResource(extensionContext, kafkaUser);
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, allowedKafkaClientsName, kafkaUser).build());

        String allowedKafkaClientsPodName = kubeClient().listPodsByPrefixInName(allowedKafkaClientsName).get(0).getMetadata().getName();

        LOGGER.info("Verifying that {} pod is able to exchange messages", allowedKafkaClientsPodName);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(allowedKafkaClientsPodName)
            .withTopicName(topic0)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, deniedKafkaClientsName, kafkaUser).build());

        String deniedKafkaClientsPodName = kubeClient().listPodsByPrefixInName(deniedKafkaClientsName).get(0).getMetadata().getName();

        InternalKafkaClient newInternalKafkaClient = internalKafkaClient.toBuilder()
            .withUsingPodName(deniedKafkaClientsPodName)
            .withTopicName(topic1)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        LOGGER.info("Verifying that {} pod is not able to exchange messages", deniedKafkaClientsPodName);

        assertThrows(AssertionError.class, () -> {
            newInternalKafkaClient.checkProducedAndConsumedMessages(
                newInternalKafkaClient.sendMessagesTls(),
                newInternalKafkaClient.receiveMessagesTls()
            );
        });
    }

    @IsolatedTest("Specific cluster operator for test case")
    void testNPWhenOperatorIsInSameNamespaceAsOperand(ExtensionContext extensionContext) {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        installClusterOperator(extensionContext, NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        checkNetworkPoliciesInNamespace(clusterName, NAMESPACE);
        changeKafkaConfigurationAndCheckObservedGeneration(clusterName, NAMESPACE);
    }

    @IsolatedTest("Specific cluster operator for test case")
    void testNPWhenOperatorIsInDifferentNamespaceThanOperand(ExtensionContext extensionContext) {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String secondNamespace = "second-" + NAMESPACE;

        Map<String, String> labels = new HashMap<>();
        labels.put("my-label", "my-value");

        EnvVar operatorLabelsEnv = new EnvVarBuilder()
            .withName("STRIMZI_OPERATOR_NAMESPACE_LABELS")
            .withValue(labels.toString().replaceAll("\\{|}", ""))
            .build();

        cluster.createNamespace(secondNamespace);

        prepareEnvForOperator(extensionContext, NAMESPACE, Arrays.asList(NAMESPACE, secondNamespace));

        // Apply rolebindings in CO namespace
        applyBindings(extensionContext, NAMESPACE);

        // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
        List<ClusterRoleBinding> clusterRoleBindingList = ClusterRoleBindingTemplates.clusterRoleBindingsForAllNamespaces(NAMESPACE);
        clusterRoleBindingList.forEach(clusterRoleBinding ->
            ClusterRoleBindingResource.clusterRoleBinding(extensionContext, clusterRoleBinding));
        // 060-Deployment
        resourceManager.createResource(extensionContext, BundleResource.clusterOperator(NAMESPACE, "*", Constants.CO_OPERATION_TIMEOUT_DEFAULT)
            .editOrNewSpec()
                .editOrNewTemplate()
                    .editOrNewSpec()
                        .editContainer(0)
                            .addToEnv(operatorLabelsEnv)
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build());

        Namespace actualNamespace = kubeClient().getClient().namespaces().withName(NAMESPACE).get();
        kubeClient().getClient().namespaces().withName(NAMESPACE).edit(ns -> new NamespaceBuilder(actualNamespace)
            .editOrNewMetadata()
                .addToLabels(labels)
            .endMetadata()
            .build());

        cluster.setNamespace(secondNamespace);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editMetadata()
                .addToLabels(labels)
            .endMetadata()
            .build());

        checkNetworkPoliciesInNamespace(clusterName, secondNamespace);

        changeKafkaConfigurationAndCheckObservedGeneration(clusterName, secondNamespace);
    }

    void checkNetworkPoliciesInNamespace(String clusterName, String namespace) {
        List<NetworkPolicy> networkPolicyList = new ArrayList<>(kubeClient().getClient().network().networkPolicies().inNamespace(namespace).list().getItems());

        String networkPolicyPrefix = clusterName + "-network-policy";

        assertNotNull(networkPolicyList.stream().filter(networkPolicy ->  networkPolicy.getMetadata().getName().contains(networkPolicyPrefix + "-kafka")).findFirst());
        assertNotNull(networkPolicyList.stream().filter(networkPolicy ->  networkPolicy.getMetadata().getName().contains(networkPolicyPrefix + "-zookeeper")).findFirst());
    }

    void changeKafkaConfigurationAndCheckObservedGeneration(String clusterName, String namespace) {
        long observedGen = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getStatus().getObservedGeneration();
        KafkaUtils.updateConfigurationWithStabilityWait(clusterName, "log.message.timestamp.type", "LogAppendTime");

        assertThat(KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getStatus().getObservedGeneration(), is(not(observedGen)));
    }
}
