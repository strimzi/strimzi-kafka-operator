/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.specific.MetricsUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testNetworkPoliciesWithPlainListener() {
        installClusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT);

        String allowedKafkaClientsName = CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS + "-allow";
        String deniedKafkaClientsName = CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS + "-deny";
        Map<String, String> matchLabelForPlain = new HashMap<>();
        matchLabelForPlain.put("app", allowedKafkaClientsName);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1)
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
            .done();

        String topic0 = "topic-example-0";
        String topic1 = "topic-example-1";

        String userName = "user-example";
        KafkaUser kafkaUser = KafkaUserResource.scramShaUser(CLUSTER_NAME, userName).done();

        KafkaTopicResource.topic(CLUSTER_NAME, topic0).done();
        KafkaTopicResource.topic(CLUSTER_NAME, topic1).done();

        KafkaClientsResource.deployKafkaClients(false, allowedKafkaClientsName, kafkaUser).done();

        String allowedKafkaClientsPodName = kubeClient().listPodsByPrefixInName(allowedKafkaClientsName).get(0).getMetadata().getName();

        LOGGER.info("Verifying that {} pod is able to exchange messages", allowedKafkaClientsPodName);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(allowedKafkaClientsPodName)
            .withTopicName(topic0)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        KafkaClientsResource.deployKafkaClients(false, deniedKafkaClientsName, kafkaUser).done();

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
        Map<String, String> kafkaExporterMetricsData = MetricsUtils.collectKafkaExporterPodsMetrics(CLUSTER_NAME);
        assertThat("Kafka Exporter metrics should be non-empty", kafkaExporterMetricsData.size() > 0);
        for (Map.Entry<String, String> entry : kafkaExporterMetricsData.entrySet()) {
            assertThat("Value from collected metric should be non-empty", !entry.getValue().isEmpty());
            assertThat("Metrics doesn't contain specific values", entry.getValue().contains("kafka_consumergroup_current_offset"));
            assertThat("Metrics doesn't contain specific values", entry.getValue().contains("kafka_topic_partitions{topic=\"" + topic0 + "\"} 1"));
            assertThat("Metrics doesn't contain specific values", entry.getValue().contains("kafka_topic_partitions{topic=\"" + topic1 + "\"} 1"));
        }
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testNetworkPoliciesWithTlsListener() {
        installClusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT);

        String allowedKafkaClientsName = CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS + "-allow";
        String deniedKafkaClientsName = CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS + "-deny";
        Map<String, String> matchLabelsForTls = new HashMap<>();
        matchLabelsForTls.put("app", allowedKafkaClientsName);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1)
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
            .done();

        String topic0 = "topic-example-0";
        String topic1 = "topic-example-1";
        KafkaTopicResource.topic(CLUSTER_NAME, topic0).done();
        KafkaTopicResource.topic(CLUSTER_NAME, topic1).done();

        String userName = "user-example";
        KafkaUser kafkaUser = KafkaUserResource.scramShaUser(CLUSTER_NAME, userName).done();

        KafkaClientsResource.deployKafkaClients(true, allowedKafkaClientsName, kafkaUser).done();

        String allowedKafkaClientsPodName = kubeClient().listPodsByPrefixInName(allowedKafkaClientsName).get(0).getMetadata().getName();

        LOGGER.info("Verifying that {} pod is able to exchange messages", allowedKafkaClientsPodName);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(allowedKafkaClientsPodName)
            .withTopicName(topic0)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        KafkaClientsResource.deployKafkaClients(true, deniedKafkaClientsName, kafkaUser).done();

        String deniedKafkaClientsPodName = kubeClient().listPodsByPrefixInName(deniedKafkaClientsName).get(0).getMetadata().getName();

        InternalKafkaClient newInternalKafkaClient = internalKafkaClient.toBuilder()
            .withUsingPodName(deniedKafkaClientsPodName)
            .withTopicName(topic1)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        LOGGER.info("Verifying that {} pod is  not able to exchange messages", deniedKafkaClientsPodName);

        assertThrows(AssertionError.class, () -> {
            newInternalKafkaClient.checkProducedAndConsumedMessages(
                newInternalKafkaClient.sendMessagesTls(),
                newInternalKafkaClient.receiveMessagesTls()
            );
        });
    }

    @Test
    void testNPWhenOperatorIsInSameNamespaceAsOperand() {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        installClusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        checkNetworkPoliciesInNamespace(NAMESPACE);

        changeKafkaConfigurationAndCheckObservedGeneration(NAMESPACE);
    }

    @Test
    void testNPWhenOperatorIsInDifferentNamespaceThanOperand() {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        String secondNamespace = "second-" + NAMESPACE;

        Map<String, String> labels = new HashMap<>();
        labels.put("my-label", "my-value");

        EnvVar operatorLabelsEnv = new EnvVarBuilder()
            .withName("STRIMZI_OPERATOR_NAMESPACE_LABELS")
            .withValue(labels.toString().replaceAll("\\{|}", ""))
            .build();

        cluster.createNamespace(secondNamespace);

        prepareEnvForOperator(NAMESPACE, Arrays.asList(NAMESPACE, secondNamespace));

        // Apply role bindings in CO namespace
        applyRoleBindings(NAMESPACE);

        // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
        List<ClusterRoleBinding> clusterRoleBindingList = KubernetesResource.clusterRoleBindingsForAllNamespaces(NAMESPACE);
        clusterRoleBindingList.forEach(clusterRoleBinding ->
            KubernetesResource.clusterRoleBinding(clusterRoleBinding, NAMESPACE));
        // 060-Deployment
        BundleResource.clusterOperator("*", Constants.CO_OPERATION_TIMEOUT_DEFAULT)
            .editOrNewSpec()
                .editOrNewTemplate()
                    .editOrNewSpec()
                        .editContainer(0)
                            .addToEnv(operatorLabelsEnv)
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .done();

        kubeClient().getClient().namespaces().withName(NAMESPACE).edit()
            .editMetadata()
                .addToLabels(labels)
            .endMetadata()
            .done();

        cluster.setNamespace(secondNamespace);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
            .editMetadata()
                .addToLabels(labels)
            .endMetadata()
            .done();

        checkNetworkPoliciesInNamespace(secondNamespace);

        changeKafkaConfigurationAndCheckObservedGeneration(secondNamespace);
    }

    void checkNetworkPoliciesInNamespace(String namespace) {
        List<NetworkPolicy> networkPolicyList = new ArrayList<>(kubeClient().getClient().network().networkPolicies().inNamespace(namespace).list().getItems());

        String networkPolicyPrefix = CLUSTER_NAME + "-network-policy";

        assertNotNull(networkPolicyList.stream().filter(networkPolicy ->  networkPolicy.getMetadata().getName().contains(networkPolicyPrefix + "-kafka")).findFirst());
        assertNotNull(networkPolicyList.stream().filter(networkPolicy ->  networkPolicy.getMetadata().getName().contains(networkPolicyPrefix + "-zookeeper")).findFirst());
    }

    void changeKafkaConfigurationAndCheckObservedGeneration(String namespace) {
        long observedGen = KafkaResource.kafkaClient().inNamespace(namespace).withName(CLUSTER_NAME).get().getStatus().getObservedGeneration();
        KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, "log.message.timestamp.type", "LogAppendTime");

        assertThat(KafkaResource.kafkaClient().inNamespace(namespace).withName(CLUSTER_NAME).get().getStatus().getObservedGeneration(), is(not(observedGen)));
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
    }
}
