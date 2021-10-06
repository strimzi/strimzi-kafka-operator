/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka.listeners;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.ListenerAddress;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.BeforeAllOnce;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.security.CertAndKeyFiles;
import io.strimzi.systemtest.security.SystemTestCertAndKey;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.vertx.core.json.JsonArray;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.STATEFUL_SET;
import static io.strimzi.systemtest.security.SystemTestCertManager.exportToPemFiles;
import static io.strimzi.systemtest.security.SystemTestCertManager.generateIntermediateCaCertAndKey;
import static io.strimzi.systemtest.security.SystemTestCertManager.generateEndEntityCertAndKey;
import static io.strimzi.systemtest.security.SystemTestCertManager.generateRootCaCertAndKey;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.getKafkaSecretCertificates;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.getKafkaStatusCertificates;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag(REGRESSION)
@IsolatedSuite
public class ListenersST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(ListenersST.class);

    private static final CertAndKeyFiles ROOT_CA_CERT_AND_KEY_1;
    private static final CertAndKeyFiles STRIMZI_CERT_AND_KEY_1;
    private static final CertAndKeyFiles CHAIN_CERT_AND_KEY_1;

    private static final CertAndKeyFiles ROOT_CA_CERT_AND_KEY_2;
    private static final CertAndKeyFiles STRIMZI_CERT_AND_KEY_2;
    private static final CertAndKeyFiles CHAIN_CERT_AND_KEY_2;

    static {
        SystemTestCertAndKey root1 = generateRootCaCertAndKey();
        SystemTestCertAndKey intermediate1 = generateIntermediateCaCertAndKey(root1);
        SystemTestCertAndKey strimzi1 = generateEndEntityCertAndKey(intermediate1);
        ROOT_CA_CERT_AND_KEY_1 = exportToPemFiles(root1);
        STRIMZI_CERT_AND_KEY_1 = exportToPemFiles(strimzi1);
        CHAIN_CERT_AND_KEY_1 = exportToPemFiles(strimzi1, intermediate1, root1);

        SystemTestCertAndKey root2 = generateRootCaCertAndKey();
        SystemTestCertAndKey intermediate2 = generateIntermediateCaCertAndKey(root2);
        SystemTestCertAndKey strimzi2 = generateEndEntityCertAndKey(intermediate2);
        ROOT_CA_CERT_AND_KEY_2 = exportToPemFiles(root2);
        STRIMZI_CERT_AND_KEY_2 = exportToPemFiles(strimzi2);
        CHAIN_CERT_AND_KEY_2 = exportToPemFiles(strimzi2, intermediate2, root2);
    }

    private final String customCertChain1 = "custom-certificate-chain-1";
    private final String customCertServer1 = "custom-certificate-server-1";
    private final String customCertServer2 = "custom-certificate-server-2";
    private final String customRootCA1 = "custom-certificate-root-1";
    private final String customListenerName = "randname";

    private String userName = KafkaUserUtils.generateRandomNameOfKafkaUser();

    /**
     * Test sending messages over plain transport, without auth
     */
    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesPlainAnonymous(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, clusterName + "-" + Constants.KAFKA_CLIENTS).build());

        final String defaultKafkaClientsPodName =
            kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Checking produced and consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        Service kafkaService = kubeClient(namespaceName).getService(namespaceName, KafkaResources.bootstrapServiceName(clusterName));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(StUtils.expectedServiceDiscoveryInfo("none", "none", false, true), is(serviceDiscoveryArray));
    }

    /**
     * Test sending messages over tls transport using mutual tls auth
     */
    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesTlsAuthenticated(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String kafkaUser = mapWithTestUsers.get(extensionContext.getDisplayName());

        // Use a Kafka with plain listener disabled
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(
                        new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withTls(false)
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                        .build()
                    )
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        KafkaUser user = KafkaUserTemplates.tlsUser(clusterName, kafkaUser).build();
        resourceManager.createResource(extensionContext, user);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, user).build());

        final String kafkaClientsPodName =
            kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(kafkaUser)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        // Check brokers availability
        LOGGER.info("Checking produced and consumed messages to pod:{}", kafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        Service kafkaService = kubeClient(namespaceName).getService(namespaceName, KafkaResources.bootstrapServiceName(clusterName));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(StUtils.expectedServiceDiscoveryInfo("none", Constants.TLS_LISTENER_DEFAULT_NAME, false, true), is(serviceDiscoveryArray));
    }

    /**
     * Test sending messages over plain transport using scram sha auth
     */
    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesPlainScramSha(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String kafkaUsername = mapWithTestUsers.get(extensionContext.getDisplayName());

        // Use a Kafka with plain listener disabled
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(customListenerName)
                            .withPort(9095)
                            .withTls(false)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        KafkaUser kafkaUser = KafkaUserTemplates.scramShaUser(clusterName, kafkaUsername).build();

        resourceManager.createResource(extensionContext, kafkaUser);

        String brokerPodLog = kubeClient(namespaceName).logsInSpecificNamespace(namespaceName, clusterName + "-kafka-0", "kafka");
        Pattern p = Pattern.compile("^.*" + Pattern.quote(kafkaUsername) + ".*$", Pattern.MULTILINE);
        Matcher m = p.matcher(brokerPodLog);
        boolean found = false;
        while (m.find()) {
            found = true;
            LOGGER.info("Broker pod log line about user {}: {}", kafkaUsername, m.group());
        }
        if (!found) {
            LOGGER.warn("No broker pod log lines about user {}", kafkaUsername);
            LOGGER.info("Broker pod log:\n----\n{}\n----\n", brokerPodLog);
        }

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespaceName, false, clusterName + "-" + Constants.KAFKA_CLIENTS, kafkaUser).build());

        final String kafkaClientsPodName =
            kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(kafkaUsername)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(customListenerName)
            .build();

        // Check brokers availability
        LOGGER.info("Checking produced and consumed messages to pod:{}", kafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        Service kafkaService = kubeClient(namespaceName).getService(namespaceName, KafkaResources.bootstrapServiceName(clusterName));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(serviceDiscoveryArray, is(StUtils.expectedServiceDiscoveryInfo(9095, "kafka", "scram-sha-512", false)));
    }

    /**
     * Test sending messages over tls transport using scram sha auth
     */
    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesTlsScramSha(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String kafkaUsername = mapWithTestUsers.get(extensionContext.getDisplayName());
        final int passwordLength = 25;

        // Use a Kafka with plain listener disabled
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9096)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                        .build())
                .endKafka()
                .editEntityOperator()
                    .editOrNewTemplate()
                        .editOrNewUserOperatorContainer()
                            .addToEnv(
                                new ContainerEnvVarBuilder()
                                    .withName("STRIMZI_SCRAM_SHA_PASSWORD_LENGTH")
                                    .withValue(String.valueOf(passwordLength))
                                    .build())
                        .endUserOperatorContainer()
                    .endTemplate()
                .endEntityOperator()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        KafkaUser kafkaUser = KafkaUserTemplates.scramShaUser(clusterName, kafkaUsername).build();

        resourceManager.createResource(extensionContext, kafkaUser);
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespaceName, true, clusterName + "-" + Constants.KAFKA_CLIENTS, kafkaUser).build());

        final String kafkaClientsPodName =
            kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(kafkaUsername)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        // Check brokers availability
        LOGGER.info("Checking produced and consumed messages to pod:{}", kafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        LOGGER.info("Checking if generated password has {} characters", passwordLength);
        String password = kubeClient().namespace(namespaceName).getSecret(kafkaUsername).getData().get("password");
        String decodedPassword = new String(Base64.getDecoder().decode(password));

        assertEquals(decodedPassword.length(), passwordLength);

        Service kafkaService = kubeClient(namespaceName).getService(namespaceName, KafkaResources.bootstrapServiceName(clusterName));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(serviceDiscoveryArray, is(StUtils.expectedServiceDiscoveryInfo(9096, "kafka", "scram-sha-512", true)));
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testNodePort(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final Map<String, String> label = singletonMap("my-label", "value");
        final Map<String, String> anno = singletonMap("my-annotation", "value");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9097)
                            .withTls(true)
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.NODEPORT)
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9098)
                            .withTls(false)
                        .build())
                    .withConfig(singletonMap("default.replication.factor", 3))
                    .editOrNewTemplate()
                        .withNewClusterRoleBinding()
                            .withNewMetadata()
                                .withAnnotations(anno)
                                .withLabels(label)
                            .endMetadata()
                        .endClusterRoleBinding()
                    .endTemplate()
                .endKafka()
            .endSpec()
            .build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesPlain(),
            externalKafkaClient.receiveMessagesPlain()
        );

        // Check that Kafka status has correct addresses in NodePort external listener part
        for (ListenerStatus listenerStatus : KafkaResource.getKafkaStatus(clusterName, namespaceName).getListeners()) {
            if (listenerStatus.getType().equals(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)) {
                List<String> listStatusAddresses = listenerStatus.getAddresses().stream().map(ListenerAddress::getHost).collect(Collectors.toList());
                listStatusAddresses.sort(Comparator.comparing(String::toString));
                List<Integer> listStatusPorts = listenerStatus.getAddresses().stream().map(ListenerAddress::getPort).collect(Collectors.toList());
                Integer nodePort = kubeClient(namespaceName).getService(namespaceName, KafkaResources.externalBootstrapServiceName(clusterName)).getSpec().getPorts().get(0).getNodePort();

                List<String> nodeIps = kubeClient(namespaceName).listPods(kubeClient(namespaceName).getStatefulSet(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName)).getMetadata().getLabels())
                        .stream().map(pods -> pods.getStatus().getHostIP()).distinct().collect(Collectors.toList());
                nodeIps.sort(Comparator.comparing(String::toString));

                assertThat(listStatusAddresses, is(nodeIps));
                for (Integer port : listStatusPorts) {
                    assertThat(port, is(nodePort));
                }
            }
        }

        // check the ClusterRoleBinding annotations and labels in Kafka cluster
        Map<String, String> actualLabel = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getSpec().getKafka().getTemplate().getClusterRoleBinding().getMetadata().getLabels();
        Map<String, String> actualAnno = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getSpec().getKafka().getTemplate().getClusterRoleBinding().getMetadata().getAnnotations();

        assertThat(actualLabel, is(label));
        assertThat(actualAnno, is(anno));
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testOverrideNodePortConfiguration(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        final int brokerNodePort = 32000;
        final int brokerId = 0;

        final int clusterBootstrapNodePort = 32100;
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9099)
                            .withTls(true)
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.NODEPORT)
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9100)
                            .withTls(false)
                            .withNewConfiguration()
                                .withNewBootstrap()
                                    .withNodePort(clusterBootstrapNodePort)
                                .endBootstrap()
                                .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                        .withBroker(brokerId)
                                        .withNodePort(brokerNodePort)
                                        .build())
                            .endConfiguration()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        LOGGER.info("Checking nodePort to {} for bootstrap service {}", clusterBootstrapNodePort,
                KafkaResources.externalBootstrapServiceName(clusterName));
        assertThat(kubeClient(namespaceName).getService(namespaceName, KafkaResources.externalBootstrapServiceName(clusterName))
                .getSpec().getPorts().get(0).getNodePort(), is(clusterBootstrapNodePort));
        String firstExternalService = clusterName + "-kafka-" + Constants.EXTERNAL_LISTENER_DEFAULT_NAME + "-" + 0;
        LOGGER.info("Checking nodePort to {} for kafka-broker service {}", brokerNodePort, firstExternalService);
        assertThat(kubeClient(namespaceName).getService(namespaceName, firstExternalService)
                .getSpec().getPorts().get(0).getNodePort(), is(brokerNodePort));

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesPlain(),
            externalKafkaClient.receiveMessagesPlain()
        );
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testNodePortTls(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9101)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                        .build())
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, userName).build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );
    }

    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testLoadBalancer(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9102)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(false)
                            .withNewConfiguration()
                                .withFinalizers(LB_FINALIZERS)
                            .endConfiguration()
                        .build())
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .build());

        ServiceUtils.waitUntilAddressIsReachable(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getListeners().get(0).getAddresses().get(0).getHost());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesPlain(),
            externalKafkaClient.receiveMessagesPlain()
        );
    }

    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testLoadBalancerTls(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9103)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                            .withNewConfiguration()
                                .withFinalizers(LB_FINALIZERS)
                            .endConfiguration()
                        .build())
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, userName).build());

        ServiceUtils.waitUntilAddressIsReachable(KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getListeners().get(0).getAddresses().get(0).getHost());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );
    }

//    ##########################################
//    #### Custom Certificates in Listeners ####
//    ##########################################

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomSoloCertificatesForNodePort(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9104)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(customListenerName)
                            .withPort(9105)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(customListenerName)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName("consumer-group-certs-1")
            .withUsingPodName(kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient.setMessageCount(2 * MESSAGE_COUNT);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(2 * MESSAGE_COUNT));
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomChainCertificatesForNodePort(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String clusterCustomCertChain1 = clusterName + "-" + customCertChain1;
        final String clusterCustomRootCA1 = clusterName + "-" + customRootCA1;

        SecretUtils.createCustomSecret(clusterCustomCertChain1, clusterName, namespaceName, CHAIN_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomRootCA1, clusterName, namespaceName, ROOT_CA_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(customListenerName)
                            .withPort(9106)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertChain1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertChain1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(clusterCustomRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, customListenerName, null, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName("consumer-group-certs-2")
            .withListenerName(customListenerName)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient.setMessageCount(MESSAGE_COUNT * 2);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(2 * MESSAGE_COUNT));
    }

    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomSoloCertificatesForLoadBalancer(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9107)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9108)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                                .withFinalizers(LB_FINALIZERS)
                            .endConfiguration()
                        .build())
                .endKafka()
            .endSpec()
            .build());


        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName("consumer-group-certs-3")
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient.setMessageCount(MESSAGE_COUNT * 2);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(2 * MESSAGE_COUNT));
    }

    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomChainCertificatesForLoadBalancer(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String clusterCustomCertChain1 = clusterName + "-" + customCertChain1;
        final String clusterCustomRootCA1 = clusterName + "-" + customRootCA1;

        SecretUtils.createCustomSecret(clusterCustomCertChain1, clusterName, namespaceName, CHAIN_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomRootCA1, clusterName, namespaceName, ROOT_CA_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9109)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertChain1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9110)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertChain1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                                .withFinalizers(LB_FINALIZERS)
                            .endConfiguration()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        KafkaTopicUtils.waitForKafkaTopicCreationByNamePrefix(namespaceName, topicName);

        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName)
            .editMetadata()
                .withNamespace(namespaceName)
            .endMetadata()
            .build();
        resourceManager.createResource(extensionContext, aliceUser);

        KafkaUserUtils.waitForKafkaUserCreation(namespaceName, userName);

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(clusterCustomRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName("consumer-group-certs-4")
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient.setMessageCount(MESSAGE_COUNT * 2);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(2 * MESSAGE_COUNT));
    }

    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @OpenShiftOnly
    void testCustomSoloCertificatesForRoute(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9111)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9112)
                            .withType(KafkaListenerType.ROUTE)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName("consumer-group-certs-4")
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient.setMessageCount(MESSAGE_COUNT * 2);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(2 * MESSAGE_COUNT));
    }

    @ParallelNamespaceTest
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @OpenShiftOnly
    void testCustomChainCertificatesForRoute(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String clusterCustomCertChain1 = clusterName + "-" + customCertChain1;
        final String clusterCustomRootCA1 = clusterName + "-" + customRootCA1;

        SecretUtils.createCustomSecret(clusterCustomCertChain1, clusterName, namespaceName, CHAIN_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomRootCA1, clusterName, namespaceName, ROOT_CA_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9112)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertChain1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9113)
                            .withType(KafkaListenerType.ROUTE)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertChain1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(clusterCustomRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName("consumer-group-certs-6")
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient.setMessageCount(MESSAGE_COUNT * 2);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(2 * MESSAGE_COUNT));
    }


    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testCustomCertLoadBalancerAndTlsRollingUpdate(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;
        final String clusterCustomCertServer2 = clusterName + "-" + customCertServer2;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_2);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9113)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9114)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(true)
                            .withNewConfiguration()
                                .withFinalizers(LB_FINALIZERS)
                            .endConfiguration()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        String externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        String externalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterName + "-cluster-ca-cert", "ca.crt");

        String internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, namespaceName, clusterName);

        LOGGER.info("Check if KafkaStatus certificates from external listeners are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        //External secret cert is same as internal in this case
        assertThat(externalSecretCerts, is(internalCerts));

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withCertificateAuthorityCertificateName(null)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9113)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer2)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                            .build(),
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9114)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                                .withFinalizers(LB_FINALIZERS)
                            .endConfiguration()
                            .build()
            ));
        }, namespaceName);

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, ResourceOperation.getTimeoutForResourceReadiness(STATEFUL_SET));

        KafkaUtils.waitForKafkaStatusUpdate(namespaceName, clusterName);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        String internalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName("consumer-group-certs-81")
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient.setMessageCount(MESSAGE_COUNT * 3);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(3 * MESSAGE_COUNT));

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_2);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_1);

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(clusterName), 3);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        internalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        sent = externalKafkaClient.sendMessagesTls() + MESSAGE_COUNT;

        externalKafkaClient.setMessageCount(2 * MESSAGE_COUNT);

        externalKafkaClient.verifyProducedAndConsumedMessages(
            sent,
            externalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName("consumer-group-certs-71")
            .withMessageCount(MESSAGE_COUNT)
            .build();

        sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient.setMessageCount(MESSAGE_COUNT * 5);

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(5 * MESSAGE_COUNT));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9113)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer2)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                            .build(),
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9114)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withNewConfiguration()
                                .withFinalizers(LB_FINALIZERS)
                            .endConfiguration()
                            .withTls(true)
                            .build()
            ));
        }, namespaceName);

        StatefulSetUtils.waitTillSsHasRolled(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, ResourceOperation.getTimeoutForResourceReadiness(STATEFUL_SET));

        KafkaUtils.waitForKafkaStatusUpdate(namespaceName, clusterName);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterName + "-cluster-ca-cert", "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        internalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(null)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        sent = externalKafkaClient.sendMessagesTls() + MESSAGE_COUNT;

        externalKafkaClient.setMessageCount(2 * MESSAGE_COUNT);

        externalKafkaClient.verifyProducedAndConsumedMessages(
            sent,
            externalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName("consumer-group-certs-83")
            .withMessageCount(6 * MESSAGE_COUNT)
            .build();

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(6 * MESSAGE_COUNT));
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testCustomCertNodePortAndTlsRollingUpdate(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;
        final String clusterCustomCertServer2 = clusterName + "-" + customCertServer2;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_2);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9115)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9116)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                        .build())
                .endKafka()
            .endSpec()
            .build());


        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        String externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        String externalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterName + "-cluster-ca-cert", "ca.crt");

        String internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, namespaceName, clusterName);

        LOGGER.info("Check if KafkaStatus certificates from external listeners are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        //External secret cert is same as internal in this case
        assertThat(externalSecretCerts, is(internalCerts));

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9115)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer2)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                            .build(),
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9116)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                            .build()
            ));
        }, namespaceName);

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, ResourceOperation.getTimeoutForResourceReadiness(STATEFUL_SET));

        KafkaUtils.waitForKafkaStatusUpdate(namespaceName, clusterName);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        String internalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        int expectedMessageCountForNewGroup = MESSAGE_COUNT * 3;

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName("consumer-group-certs-71")
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        int expectedMessageCountForExternalClient = sent;
        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient.setMessageCount(expectedMessageCountForNewGroup);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(expectedMessageCountForNewGroup));

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_2);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_1);

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, ResourceOperation.getTimeoutForResourceReadiness(STATEFUL_SET));

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        internalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        sent = externalKafkaClient.sendMessagesTls();
        expectedMessageCountForExternalClient += sent;
        expectedMessageCountForNewGroup += sent;

        externalKafkaClient.verifyProducedAndConsumedMessages(
            expectedMessageCountForExternalClient,
            externalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName("consumer-group-certs-72")
            .withMessageCount(MESSAGE_COUNT)
            .build();

        sent = internalKafkaClient.sendMessagesTls();
        expectedMessageCountForNewGroup += sent;

        expectedMessageCountForExternalClient = sent;

        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient.setMessageCount(expectedMessageCountForNewGroup);
        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(expectedMessageCountForNewGroup));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9115)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer2)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                            .build(),
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9116)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .build()
            ));
        }, namespaceName);

        StatefulSetUtils.waitTillSsHasRolled(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, ResourceOperation.getTimeoutForResourceReadiness(STATEFUL_SET));

        KafkaUtils.waitForKafkaStatusUpdate(namespaceName, clusterName);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterName + "-cluster-ca-cert", "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        internalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(null)
            .build();

        sent = externalKafkaClient.sendMessagesTls();

        expectedMessageCountForExternalClient += sent;
        expectedMessageCountForNewGroup += sent;

        externalKafkaClient.verifyProducedAndConsumedMessages(
            expectedMessageCountForExternalClient,
            externalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName("consumer-group-certs-73")
            .withMessageCount(expectedMessageCountForNewGroup)
            .build();

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(expectedMessageCountForNewGroup));
    }

    @ParallelNamespaceTest
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @OpenShiftOnly
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testCustomCertRouteAndTlsRollingUpdate(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;
        final String clusterCustomCertServer2 = clusterName + "-" + customCertServer2;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_2);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9117)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9118)
                            .withType(KafkaListenerType.ROUTE)
                            .withTls(true)
                        .build())
                .endKafka()
            .endSpec()
            .build());

        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        String externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        String externalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterName + "-cluster-ca-cert", "ca.crt");

        String internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, namespaceName, clusterName);

        LOGGER.info("Check if KafkaStatus certificates from external listeners are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        //External secret cert is same as internal in this case
        assertThat(externalSecretCerts, is(internalCerts));

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withCertificateAuthorityCertificateName(null)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9117)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer2)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                            .build(),
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9118)
                            .withType(KafkaListenerType.ROUTE)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                            .build()
            ));
        }, namespaceName);

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, ResourceOperation.getTimeoutForResourceReadiness(STATEFUL_SET));

        KafkaUtils.waitForKafkaStatusUpdate(namespaceName, clusterName);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        String internalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(null)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls() + MESSAGE_COUNT,
            externalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName("consumer-group-certs-91")
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient.setMessageCount(MESSAGE_COUNT * 3);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(3 * MESSAGE_COUNT));

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_2);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_1);

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, ResourceOperation.getTimeoutForResourceReadiness(STATEFUL_SET));

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        internalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls() + MESSAGE_COUNT,
            externalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName("consumer-group-certs-92")
            .withMessageCount(MESSAGE_COUNT)
            .build();

        sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withMessageCount(5 * MESSAGE_COUNT)
            .build();

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(5 * MESSAGE_COUNT));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9117)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer2)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                            .build(),
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9118)
                            .withType(KafkaListenerType.ROUTE)
                            .withTls(true)
                            .build()
            ));
        }, namespaceName);

        StatefulSetUtils.waitTillSsHasRolled(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName,  KafkaResources.kafkaStatefulSetName(clusterName), 3, ResourceOperation.getTimeoutForResourceReadiness(STATEFUL_SET));

        KafkaUtils.waitForKafkaStatusUpdate(namespaceName, clusterName);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(namespaceName, clusterName + "-cluster-ca-cert", "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, namespaceName, clusterName);
        internalSecretCerts = getKafkaSecretCertificates(namespaceName,  clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(null)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        sent = externalKafkaClient.sendMessagesTls() + (5 * MESSAGE_COUNT);

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withMessageCount(6 * MESSAGE_COUNT)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            sent,
            externalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withMessageCount(6 * MESSAGE_COUNT)
            .withConsumerGroupName("consumer-group-certs-93")
            .build();

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(6 * MESSAGE_COUNT));
    }

    @ParallelNamespaceTest
    void testNonExistingCustomCertificate(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String nonExistingCertName = "non-existing-certificate";

        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9119)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(nonExistingCertName)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName, KafkaResources.zookeeperStatefulSetName(clusterName), 1, ResourceOperation.getTimeoutForResourceReadiness(STATEFUL_SET));

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, namespaceName, ".*Secret " + nonExistingCertName + " with custom TLS certificate does not exist.*");

        KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).delete();
    }

    @ParallelNamespaceTest
    void testCertificateWithNonExistingDataCrt(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String nonExistingCertName = "non-existing-crt";
        final String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9120)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate(nonExistingCertName)
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName, KafkaResources.zookeeperStatefulSetName(clusterName), 1, ResourceOperation.getTimeoutForResourceReadiness(STATEFUL_SET));

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, namespaceName,
                ".*Secret " + clusterCustomCertServer1 + " does not contain certificate under the key " + nonExistingCertName + ".*");

        KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).delete();
    }

    @ParallelNamespaceTest
    void testCertificateWithNonExistingDataKey(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String nonExistingCertKey = "non-existing-key";
        final String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, namespaceName, STRIMZI_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9121)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(clusterCustomCertServer1)
                                    .withKey(nonExistingCertKey)
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName, KafkaResources.zookeeperStatefulSetName(clusterName), 1, ResourceOperation.getTimeoutForResourceReadiness(STATEFUL_SET));

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, namespaceName,
                ".*Secret " + clusterCustomCertServer1 + " does not contain custom certificate private key under the key " + nonExistingCertKey + ".*");

        KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).delete();
    }

    @ParallelNamespaceTest
    void testMessagesTlsScramShaWithPredefinedPassword(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        final String firstUnencodedPassword = "completely_secret_password";
        final String secondUnencodedPassword = "completely_different_secret_password";

        final String firstEncodedPassword = Base64.getEncoder().encodeToString(firstUnencodedPassword.getBytes(StandardCharsets.UTF_8));
        final String secondEncodedPassword = Base64.getEncoder().encodeToString(secondUnencodedPassword.getBytes(StandardCharsets.UTF_8));

        final String secretName = clusterName + "-secret";

        Secret password = new SecretBuilder()
            .withNewMetadata()
                .withName(secretName)
            .endMetadata()
            .addToData("password", firstEncodedPassword)
            .build();

        kubeClient().namespace(namespaceName).createSecret(password);
        assertThat("Password in secret is not correct", kubeClient().namespace(namespaceName).getSecret(secretName).getData().get("password"), is(firstEncodedPassword));

        KafkaUser kafkaUser = KafkaUserTemplates.scramShaUser(clusterName, userName)
            .editSpec()
                .withNewKafkaUserScramSha512ClientAuthentication()
                    .withNewPassword()
                        .withNewValueFrom()
                            .withNewSecretKeyRef("password", secretName, false)
                        .endValueFrom()
                    .endPassword()
                .endKafkaUserScramSha512ClientAuthentication()
            .endSpec()
            .build();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withType(KafkaListenerType.INTERNAL)
                        .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9096)
                        .withTls(true)
                        .withNewKafkaListenerAuthenticationScramSha512Auth()
                        .endKafkaListenerAuthenticationScramSha512Auth()
                        .build())
                .endKafka()
            .endSpec()
            .build(),
            kafkaUser,
            KafkaTopicTemplates.topic(clusterName, topicName).build()
        );

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespaceName, true, kafkaClientsName, kafkaUser).build());

        String kafkaClientsPodName =
            kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, kafkaClientsName).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        int sentMessages = internalKafkaClient.sendMessagesTls();
        internalKafkaClient.checkProducedAndConsumedMessages(
            sentMessages,
            internalKafkaClient.receiveMessagesTls()
        );

        LOGGER.info("Changing password in secret: {}, we should be able to send/receive messages", secretName);

        password = new SecretBuilder(password)
            .addToData("password", secondEncodedPassword)
            .build();

        kubeClient().namespace(namespaceName).createSecret(password);
        SecretUtils.waitForUserPasswordChange(namespaceName, userName, secondEncodedPassword);

        LOGGER.info("We need to recreate Kafka Clients deployment, so the correct password from secret will be taken");
        resourceManager.deleteResource(kubeClient().getDeployment(kafkaClientsName));
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespaceName, true, kafkaClientsName, kafkaUser).build());

        LOGGER.info("Receiving messages with new password");

        kafkaClientsPodName =
            kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, kafkaClientsName).get(0).getMetadata().getName();

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withUsingPodName(kafkaClientsPodName)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            sentMessages,
            internalKafkaClient.receiveMessagesTls()
        );
    }

    @ParallelNamespaceTest
    void dtestAdvertisedHostNamesAppearsInBrokerCerts(ExtensionContext extensionContext) throws CertificateException {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);

        final String advertHostInternal0 = "kafka-test.internal.0.net";
        final String advertHostInternal1 = "kafka-test.internal.1.net";
        final String advertHostInternal2 = "kafka-test.internal.2.net";

        List<String> advertHostInternalList = asList(advertHostInternal0, advertHostInternal1, advertHostInternal2);

        final int advertPortInternalListener = 9999;

        final String advertHostExternal0 = "kafka-test.external.0.net";
        final String advertHostExternal1 = "kafka-test.external.1.net";
        final String advertHostExternal2 = "kafka-test.external.2.net";

        List<String> advertHostExternalList = asList(advertHostExternal0, advertHostExternal1, advertHostExternal2);

        final int advertPortExternalListener = 9888;

        GenericKafkaListenerConfigurationBroker brokerInternal0 =
            new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withAdvertisedHost(advertHostInternal0)
                .withAdvertisedPort(advertPortInternalListener)
                .build();

        GenericKafkaListenerConfigurationBroker brokerInternal1 =
            new GenericKafkaListenerConfigurationBrokerBuilder(brokerInternal0)
                .withBroker(1)
                .withAdvertisedHost(advertHostInternal1)
                .build();

        GenericKafkaListenerConfigurationBroker brokerInternal2 =
            new GenericKafkaListenerConfigurationBrokerBuilder(brokerInternal0)
                .withBroker(2)
                .withAdvertisedHost(advertHostInternal2)
                .build();

        GenericKafkaListenerConfigurationBroker brokerExternal0 =
            new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withAdvertisedHost(advertHostExternal0)
                .withAdvertisedPort(advertPortExternalListener)
                .build();

        GenericKafkaListenerConfigurationBroker brokerExternal1 =
            new GenericKafkaListenerConfigurationBrokerBuilder(brokerExternal0)
                .withBroker(1)
                .withAdvertisedHost(advertHostExternal1)
                .build();

        GenericKafkaListenerConfigurationBroker brokerExternal2 =
            new GenericKafkaListenerConfigurationBrokerBuilder(brokerExternal0)
                .withBroker(2)
                .withAdvertisedHost(advertHostExternal2)
                .build();

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
                .editSpec()
                    .editKafka()
                        .withListeners(asList(
                            new GenericKafkaListenerBuilder()
                                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9098)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withBrokers(asList(brokerInternal0, brokerInternal1, brokerInternal2))
                                .endConfiguration()
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9099)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withBrokers(asList(brokerExternal0, brokerExternal1, brokerExternal2))
                                .endConfiguration()
                                .build()
                        ))
                    .endKafka()
                .endSpec()
                .build());

        Map<String, String> secretData = kubeClient().getSecret(namespaceName, KafkaResources.brokersServiceName(clusterName)).getData();
        List<String> kafkaPods = kubeClient().listPodNamesInSpecificNamespace(namespaceName, Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND)
            .stream().filter(podName -> podName.contains("kafka")).collect(Collectors.toList());

        int index = 0;
        for (String kafkaBroker : kafkaPods) {
            String cert = secretData.get(kafkaBroker + ".crt");

            LOGGER.info("Encoding {}.crt", kafkaBroker);

            ByteArrayInputStream publicCert = new ByteArrayInputStream(Base64.getDecoder().decode(cert.getBytes()));
            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            Certificate certificate = certificateFactory.generateCertificate(publicCert);

            assertThat(certificate.toString(), containsString(advertHostInternalList.get(index)));
            assertThat(certificate.toString(), containsString(advertHostExternalList.get(index++)));
        }
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        final String namespaceToWatch = Environment.isNamespaceRbacScope() ? INFRA_NAMESPACE : Constants.WATCH_ALL_NAMESPACES;

        install.unInstall();
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
            .withNamespace(INFRA_NAMESPACE)
            .withWatchingNamespaces(namespaceToWatch)
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_SHORT)
            .createInstallation()
            .runInstallation();
    }

    @AfterEach
    void afterEach(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        kubeClient(namespaceName).getClient().persistentVolumeClaims().inNamespace(namespaceName).delete();
    }
}
