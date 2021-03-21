/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka.listeners;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.NodePortListenerBrokerOverride;
import io.strimzi.api.kafka.model.listener.arraylistener.ArrayOrObjectKafkaListeners;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.ListenerAddress;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.security.SystemTestCertManager.exportToPemFiles;
import static io.strimzi.systemtest.security.SystemTestCertManager.generateIntermediateCaCertAndKey;
import static io.strimzi.systemtest.security.SystemTestCertManager.generateEndEntityCertAndKey;
import static io.strimzi.systemtest.security.SystemTestCertManager.generateRootCaCertAndKey;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.getKafkaSecretCertificates;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.getKafkaStatusCertificates;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
public class ListenersST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(ListenersST.class);

    public static final String NAMESPACE = "listeners";

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

    private String customCertChain1 = "custom-certificate-chain-1";
    private String customCertServer1 = "custom-certificate-server-1";
    private String customCertServer2 = "custom-certificate-server-2";
    private String customRootCA1 = "custom-certificate-root-1";
    private String customListenerName = "randname";

    private String userName = KafkaUserUtils.generateRandomNameOfKafkaUser();

    /**
     * Test sending messages over plain transport, without auth
     */
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesPlainAnonymous(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String clientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, clusterName + "-" + Constants.KAFKA_CLIENTS).build());

        final String defaultKafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Checking produced and consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        Service kafkaService = kubeClient().getService(KafkaResources.bootstrapServiceName(clusterName));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(StUtils.expectedServiceDiscoveryInfo("none", "none", false, true), is(serviceDiscoveryArray));
    }

    /**
     * Test sending messages over tls transport using mutual tls auth
     */
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesTlsAuthenticated(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String kafkaUser = mapWithTestUsers.get(extensionContext.getDisplayName());

        // Use a Kafka with plain listener disabled
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withTls(false)
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        KafkaUser user = KafkaUserTemplates.tlsUser(clusterName, kafkaUser).build();
        resourceManager.createResource(extensionContext, user);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, user).build());

        final String kafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
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

        Service kafkaService = kubeClient().getService(KafkaResources.bootstrapServiceName(clusterName));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(StUtils.expectedServiceDiscoveryInfo("none", Constants.TLS_LISTENER_DEFAULT_NAME, false, true), is(serviceDiscoveryArray));
    }

    /**
     * Test sending messages over plain transport using scram sha auth
     */
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesPlainScramSha(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String kafkaUsername = mapWithTestUsers.get(extensionContext.getDisplayName());

        // Use a Kafka with plain listener disabled
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(customListenerName)
                            .withPort(9095)
                            .withTls(false)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        KafkaUser kafkaUser = KafkaUserTemplates.scramShaUser(clusterName, kafkaUsername).build();

        resourceManager.createResource(extensionContext, kafkaUser);

        String brokerPodLog = kubeClient().logs(clusterName + "-kafka-0", "kafka");
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

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, clusterName + "-" + Constants.KAFKA_CLIENTS, kafkaUser).build());

        final String kafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
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

        Service kafkaService = kubeClient().getService(KafkaResources.bootstrapServiceName(clusterName));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(serviceDiscoveryArray, is(StUtils.expectedServiceDiscoveryInfo(9095, "kafka", "scram-sha-512", false)));
    }

    /**
     * Test sending messages over tls transport using scram sha auth
     */
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(ACCEPTANCE)
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesTlsScramSha(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String kafkaUsername = mapWithTestUsers.get(extensionContext.getDisplayName());

        // Use a Kafka with plain listener disabled
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9096)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        KafkaUser kafkaUser = KafkaUserTemplates.scramShaUser(clusterName, kafkaUsername).build();

        resourceManager.createResource(extensionContext, kafkaUser);
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, kafkaUser).build());

        final String kafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
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

        Service kafkaService = kubeClient().getService(KafkaResources.bootstrapServiceName(clusterName));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(serviceDiscoveryArray, is(StUtils.expectedServiceDiscoveryInfo(9096, "kafka", "scram-sha-512", true)));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testNodePort(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        Map<String, String> label = Collections.singletonMap("my-label", "value");
        Map<String, String> anno = Collections.singletonMap("my-annotation", "value");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9097)
                            .withTls(true)
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
                            .withType(KafkaListenerType.NODEPORT)
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9098)
                            .withTls(false)
                        .endGenericKafkaListener()
                    .endListeners()
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

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesPlain(),
            basicExternalKafkaClient.receiveMessagesPlain()
        );

        // Check that Kafka status has correct addresses in NodePort external listener part
        for (ListenerStatus listenerStatus : KafkaResource.getKafkaStatus(clusterName, NAMESPACE).getListeners()) {
            if (listenerStatus.getType().equals(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)) {
                List<String> listStatusAddresses = listenerStatus.getAddresses().stream().map(ListenerAddress::getHost).collect(Collectors.toList());
                listStatusAddresses.sort(Comparator.comparing(String::toString));
                List<Integer> listStatusPorts = listenerStatus.getAddresses().stream().map(ListenerAddress::getPort).collect(Collectors.toList());
                Integer nodePort = kubeClient().getService(KafkaResources.externalBootstrapServiceName(clusterName)).getSpec().getPorts().get(0).getNodePort();

                List<String> nodeIps = kubeClient().listPods(kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(clusterName)).getMetadata().getLabels())
                        .stream().map(pods -> pods.getStatus().getHostIP()).distinct().collect(Collectors.toList());
                nodeIps.sort(Comparator.comparing(String::toString));

                assertThat(listStatusAddresses, is(nodeIps));
                for (Integer port : listStatusPorts) {
                    assertThat(port, is(nodePort));
                }
            }
        }

        // check the ClusterRoleBinding annotations and labels in Kafka cluster
        Map<String, String> actualLabel = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).get().getSpec().getKafka().getTemplate().getClusterRoleBinding().getMetadata().getLabels();
        Map<String, String> actualAnno = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).get().getSpec().getKafka().getTemplate().getClusterRoleBinding().getMetadata().getAnnotations();

        assertThat(actualLabel, is(label));
        assertThat(actualAnno, is(anno));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testOverrideNodePortConfiguration(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        int brokerNodePort = 32000;
        int brokerId = 0;

        NodePortListenerBrokerOverride nodePortListenerBrokerOverride = new NodePortListenerBrokerOverride();
        nodePortListenerBrokerOverride.setBroker(brokerId);
        nodePortListenerBrokerOverride.setNodePort(brokerNodePort);
        LOGGER.info("Setting nodePort to {} for broker {}", nodePortListenerBrokerOverride.getNodePort(),
                nodePortListenerBrokerOverride.getBroker());

        int clusterBootstrapNodePort = 32100;
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9099)
                            .withTls(true)
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        LOGGER.info("Checking nodePort to {} for bootstrap service {}", clusterBootstrapNodePort,
                KafkaResources.externalBootstrapServiceName(clusterName));
        assertThat(kubeClient().getService(KafkaResources.externalBootstrapServiceName(clusterName))
                .getSpec().getPorts().get(0).getNodePort(), is(clusterBootstrapNodePort));
        String firstExternalService = clusterName + "-kafka-" + Constants.EXTERNAL_LISTENER_DEFAULT_NAME + "-" + 0;
        LOGGER.info("Checking nodePort to {} for kafka-broker service {}", brokerNodePort, firstExternalService);
        assertThat(kubeClient().getService(firstExternalService)
                .getSpec().getPorts().get(0).getNodePort(), is(brokerNodePort));

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesPlain(),
            basicExternalKafkaClient.receiveMessagesPlain()
        );
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testNodePortTls(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9101)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                        .endGenericKafkaListener()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, userName).build());

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testLoadBalancer(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9102)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(false)
                        .endGenericKafkaListener()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .build());

        ServiceUtils.waitUntilAddressIsReachable(KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus().getListeners().get(0).getAddresses().get(0).getHost());

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesPlain(),
            basicExternalKafkaClient.receiveMessagesPlain()
        );
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(ACCEPTANCE)
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testLoadBalancerTls(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9103)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                        .endGenericKafkaListener()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, userName).build());

        ServiceUtils.waitUntilAddressIsReachable(KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus().getListeners().get(0).getAddresses().get(0).getHost());

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );
    }

//    ##########################################
//    #### Custom Certificates in Listeners ####
//    ##########################################

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomSoloCertificatesForNodePort(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(customListenerName)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName("consumer-group-certs-1")
            .withUsingPodName(kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient.setMessageCount(2 * MESSAGE_COUNT);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(2 * MESSAGE_COUNT));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomChainCertificatesForNodePort(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String clusterCustomCertChain1 = clusterName + "-" + customCertChain1;
        String clusterCustomRootCA1 = clusterName + "-" + customRootCA1;

        SecretUtils.createCustomSecret(clusterCustomCertChain1, clusterName, NAMESPACE, CHAIN_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomRootCA1, clusterName, NAMESPACE, ROOT_CA_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(clusterCustomRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, customListenerName, null, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
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

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomSoloCertificatesForLoadBalancer(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
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
                            .endConfiguration()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());


        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
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

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomChainCertificatesForLoadBalancer(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String clusterCustomCertChain1 = clusterName + "-" + customCertChain1;
        String clusterCustomRootCA1 = clusterName + "-" + customRootCA1;

        SecretUtils.createCustomSecret(clusterCustomCertChain1, clusterName, NAMESPACE, CHAIN_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomRootCA1, clusterName, NAMESPACE, ROOT_CA_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
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
                            .endConfiguration()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        KafkaTopicUtils.waitForKafkaTopicCreationByNamePrefix(topicName);

        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        KafkaUserUtils.waitForKafkaUserCreation(userName);

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(clusterCustomRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
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

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(ACCEPTANCE)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @OpenShiftOnly
    void testCustomSoloCertificatesForRoute(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
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

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @OpenShiftOnly
    void testCustomChainCertificatesForRoute(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String clusterCustomCertChain1 = clusterName + "-" + customCertChain1;
        String clusterCustomRootCA1 = clusterName + "-" + customRootCA1;

        SecretUtils.createCustomSecret(clusterCustomCertChain1, clusterName, NAMESPACE, CHAIN_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomRootCA1, clusterName, NAMESPACE, ROOT_CA_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(clusterCustomRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
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


    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testCustomCertLoadBalancerAndTlsRollingUpdate(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;
        String clusterCustomCertServer2 = clusterName + "-" + customCertServer2;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_2);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9113)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9114)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(true)
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        String externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        String externalSecretCerts = getKafkaSecretCertificates(clusterName + "-cluster-ca-cert", "ca.crt");

        String internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);

        LOGGER.info("Check if KafkaStatus certificates from external listeners are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        //External secret cert is same as internal in this case
        assertThat(externalSecretCerts, is(internalCerts));

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withCertificateAuthorityCertificateName(null)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName));

        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            kafka.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(asList(
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
                            .endConfiguration()
                            .build()
            )));
        });

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(clusterName), 3);

        KafkaUtils.waitForKafkaStatusUpdate(clusterName);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        String internalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        basicExternalKafkaClient = basicExternalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
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

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_2);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_1);

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(clusterName), 3);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        internalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
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

        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            kafka.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(asList(
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
                            .build()
            )));
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(clusterName), 3);

        KafkaUtils.waitForKafkaStatusUpdate(clusterName);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(clusterName + "-cluster-ca-cert", "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        internalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .withCertificateAuthorityCertificateName(null)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName("consumer-group-certs-83")
            .withMessageCount(6 * MESSAGE_COUNT)
            .build();

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(6 * MESSAGE_COUNT));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testCustomCertNodePortAndTlsRollingUpdate(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;
        String clusterCustomCertServer2 = clusterName + "-" + customCertServer2;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_2);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9115)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9116)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());


        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        String externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        String externalSecretCerts = getKafkaSecretCertificates(clusterName + "-cluster-ca-cert", "ca.crt");

        String internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);

        LOGGER.info("Check if KafkaStatus certificates from external listeners are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        //External secret cert is same as internal in this case
        assertThat(externalSecretCerts, is(internalCerts));

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName));

        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            kafka.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(asList(
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
            )));
        });

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(clusterName), 3);

        KafkaUtils.waitForKafkaStatusUpdate(clusterName);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        String internalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        basicExternalKafkaClient = basicExternalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName("consumer-group-certs-71")
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient.setMessageCount(MESSAGE_COUNT * 3);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(3 * MESSAGE_COUNT));

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_2);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_1);

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(clusterName), 3);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        internalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName("consumer-group-certs-72")
            .withMessageCount(MESSAGE_COUNT * 5)
            .build();

        sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(5 * MESSAGE_COUNT));
        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(5 * MESSAGE_COUNT));

        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            kafka.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(asList(
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
            )));
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(clusterName), 3);

        KafkaUtils.waitForKafkaStatusUpdate(clusterName);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(clusterName + "-cluster-ca-cert", "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        internalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        basicExternalKafkaClient = basicExternalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(null)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName("consumer-group-certs-73")
            .build();

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(5 * MESSAGE_COUNT));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @OpenShiftOnly
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testCustomCertRouteAndTlsRollingUpdate(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;
        String clusterCustomCertServer2 = clusterName + "-" + customCertServer2;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_2);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9117)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9118)
                            .withType(KafkaListenerType.ROUTE)
                            .withTls(true)
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, aliceUser);

        String externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        String externalSecretCerts = getKafkaSecretCertificates(clusterName + "-cluster-ca-cert", "ca.crt");

        String internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);

        LOGGER.info("Check if KafkaStatus certificates from external listeners are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        //External secret cert is same as internal in this case
        assertThat(externalSecretCerts, is(internalCerts));

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withCertificateAuthorityCertificateName(null)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName));

        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            kafka.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(asList(
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
            )));
        });

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(clusterName), 3);

        KafkaUtils.waitForKafkaStatusUpdate(clusterName);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        String internalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        basicExternalKafkaClient = basicExternalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(null)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
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

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_2);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_1);

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(clusterName), 3);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        internalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName("consumer-group-certs-92")
            .withMessageCount(MESSAGE_COUNT * 5)
            .build();

        sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(5 * MESSAGE_COUNT));
        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(5 * MESSAGE_COUNT));

        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            kafka.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(asList(
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
            )));
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(clusterName), 3);

        KafkaUtils.waitForKafkaStatusUpdate(clusterName);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        externalSecretCerts = getKafkaSecretCertificates(clusterName + "-cluster-ca-cert", "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, clusterName);
        internalSecretCerts = getKafkaSecretCertificates(clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        basicExternalKafkaClient = basicExternalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(null)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withMessageCount(5 * MESSAGE_COUNT)
            .withConsumerGroupName("consumer-group-certs-93")
            .build();

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(5 * MESSAGE_COUNT));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testNonExistingCustomCertificate(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String nonExistingCertName = "non-existing-certificate";

        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(clusterName), 1);

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, NAMESPACE, ".*Secret " + nonExistingCertName + " with custom TLS certificate does not exist.*");

        KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).delete();
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCertificateWithNonExistingDataCrt(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String nonExistingCertName = "non-existing-crt";
        String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(clusterName), 1);

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, NAMESPACE,
                ".*Secret " + clusterCustomCertServer1 + " does not contain certificate under the key " + nonExistingCertName + ".*");

        KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).delete();
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCertificateWithNonExistingDataKey(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String nonExistingCertKey = "non-existing-key";
        String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, clusterName, NAMESPACE, STRIMZI_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
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
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(clusterName), 1);

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, NAMESPACE,
                ".*Secret " + clusterCustomCertServer1 + " does not contain custom certificate private key under the key " + nonExistingCertKey + ".*");

        KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).delete();
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE);
    }

    @AfterEach
    void afterEach(ExtensionContext extensionContext) throws Exception {
        kubeClient().getClient().persistentVolumeClaims().inNamespace(NAMESPACE).delete();
    }
}
