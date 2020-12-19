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
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
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

    private String customCertChain1 = "custom-certificate-chain-1";
    private String customCertChain2 = "custom-certificate-chain-2";
    private String customCertServer1 = "custom-certificate-server-1";
    private String customCertServer2 = "custom-certificate-server-2";
    private String customRootCA1 = "custom-certificate-root-1";
    private String customRootCA2 = "custom-certificate-root-2";
    private String customListenerName = "randname";

    private String userName = KafkaUserUtils.generateRandomNameOfKafkaUser();

    /**
     * Test sending messages over plain transport, without auth
     */
    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesPlainAnonymous() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();
        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        final String defaultKafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Checking produced and consumed messages to pod:{}", defaultKafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        Service kafkaService = kubeClient().getService(KafkaResources.bootstrapServiceName(CLUSTER_NAME));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(StUtils.expectedServiceDiscoveryInfo("none", "none", false, true), is(serviceDiscoveryArray));
    }

    /**
     * Test sending messages over tls transport using mutual tls auth
     */
    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesTlsAuthenticated() {
        String kafkaUser = KafkaUserUtils.generateRandomNameOfKafkaUser();
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        // Use a Kafka with plain listener disabled
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
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
                .endSpec().done();
        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, kafkaUser).done();

        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        final String kafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName("my-cluster" + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

        Service kafkaService = kubeClient().getService(KafkaResources.bootstrapServiceName(CLUSTER_NAME));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(StUtils.expectedServiceDiscoveryInfo("none", Constants.TLS_LISTENER_DEFAULT_NAME, false, true), is(serviceDiscoveryArray));
    }

    /**
     * Test sending messages over plain transport using scram sha auth
     */
    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesPlainScramSha() {
        String kafkaUsername = KafkaUserUtils.generateRandomNameOfKafkaUser();
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        // Use a Kafka with plain listener disabled
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
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
                .endSpec().done();

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        KafkaUser kafkaUser = KafkaUserResource.scramShaUser(CLUSTER_NAME, kafkaUsername).done();

        String brokerPodLog = kubeClient().logs(CLUSTER_NAME + "-kafka-0", "kafka");
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

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, kafkaUser).done();

        final String kafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName("my-cluster" + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

        Service kafkaService = kubeClient().getService(KafkaResources.bootstrapServiceName(CLUSTER_NAME));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(serviceDiscoveryArray, is(StUtils.expectedServiceDiscoveryInfo(9095, "kafka", "scram-sha-512", false)));
    }

    /**
     * Test sending messages over tls transport using scram sha auth
     */
    @Test
    @Tag(ACCEPTANCE)
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesTlsScramSha() {
        String kafkaUsername = KafkaUserUtils.generateRandomNameOfKafkaUser();
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        // Use a Kafka with plain listener disabled
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
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
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        KafkaUser kafkaUser = KafkaUserResource.scramShaUser(CLUSTER_NAME, kafkaUsername).done();

        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, kafkaUser).done();

        final String kafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName("my-cluster" + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

        Service kafkaService = kubeClient().getService(KafkaResources.bootstrapServiceName(CLUSTER_NAME));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(serviceDiscoveryArray, is(StUtils.expectedServiceDiscoveryInfo(9096, "kafka", "scram-sha-512", true)));
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testNodePort() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
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
                .endKafka()
            .endSpec()
            .done();

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesPlain(),
            basicExternalKafkaClient.receiveMessagesPlain()
        );

        // Check that Kafka status has correct addresses in NodePort external listener part
        for (ListenerStatus listenerStatus : KafkaResource.getKafkaStatus(CLUSTER_NAME, NAMESPACE).getListeners()) {
            if (listenerStatus.getType().equals(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)) {
                List<String> listStatusAddresses = listenerStatus.getAddresses().stream().map(ListenerAddress::getHost).collect(Collectors.toList());
                listStatusAddresses.sort(Comparator.comparing(String::toString));
                List<Integer> listStatusPorts = listenerStatus.getAddresses().stream().map(ListenerAddress::getPort).collect(Collectors.toList());
                Integer nodePort = kubeClient().getService(KafkaResources.externalBootstrapServiceName(CLUSTER_NAME)).getSpec().getPorts().get(0).getNodePort();

                List<String> nodeIps = kubeClient().listPods(kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).getMetadata().getLabels())
                        .stream().map(pods -> pods.getStatus().getHostIP()).distinct().collect(Collectors.toList());
                nodeIps.sort(Comparator.comparing(String::toString));

                assertThat(listStatusAddresses, is(nodeIps));
                for (Integer port : listStatusPorts) {
                    assertThat(port, is(nodePort));
                }
            }
        }
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testOverrideNodePortConfiguration() {
        int brokerNodePort = 32000;
        int brokerId = 0;

        NodePortListenerBrokerOverride nodePortListenerBrokerOverride = new NodePortListenerBrokerOverride();
        nodePortListenerBrokerOverride.setBroker(brokerId);
        nodePortListenerBrokerOverride.setNodePort(brokerNodePort);
        LOGGER.info("Setting nodePort to {} for broker {}", nodePortListenerBrokerOverride.getNodePort(),
                nodePortListenerBrokerOverride.getBroker());

        int clusterBootstrapNodePort = 32100;
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
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
                .done();

        LOGGER.info("Checking nodePort to {} for bootstrap service {}", clusterBootstrapNodePort,
                KafkaResources.externalBootstrapServiceName(CLUSTER_NAME));
        assertThat(kubeClient().getService(KafkaResources.externalBootstrapServiceName(CLUSTER_NAME))
                .getSpec().getPorts().get(0).getNodePort(), is(clusterBootstrapNodePort));
        String firstExternalService = CLUSTER_NAME + "-kafka-" + Constants.EXTERNAL_LISTENER_DEFAULT_NAME + "-" + 0;
        LOGGER.info("Checking nodePort to {} for kafka-broker service {}", brokerNodePort, firstExternalService);
        assertThat(kubeClient().getService(firstExternalService)
                .getSpec().getPorts().get(0).getNodePort(), is(brokerNodePort));


        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesPlain(),
            basicExternalKafkaClient.receiveMessagesPlain()
        );
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testNodePortTls() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
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
            .done();

        KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME).done();

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testLoadBalancer() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
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
            .done();

        ServiceUtils.waitUntilAddressIsReachable(KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getListeners().get(0).getAddresses().get(0).getHost());

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesPlain(),
            basicExternalKafkaClient.receiveMessagesPlain()
        );
    }

    @Test
    @Tag(ACCEPTANCE)
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testLoadBalancerTls() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
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
            .done();

        KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME).done();

        ServiceUtils.waitUntilAddressIsReachable(KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getListeners().get(0).getAddresses().get(0).getHost());

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

    @Test
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomSoloCertificatesForNodePort() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
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
                                    .withSecretName(customCertServer1)
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
                                    .withSecretName(customCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec().done();

        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(customCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(customListenerName)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName("consumer-group-certs-1")
            .withUsingPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        internalKafkaClient.setMessageCount(2 * MESSAGE_COUNT);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(2 * MESSAGE_COUNT));
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomChainCertificatesForNodePort() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
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
                                    .withSecretName(customCertChain1)
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
                                    .withSecretName(customCertChain1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec().done();

        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(customRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, customListenerName, null, aliceUser).done();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomSoloCertificatesForLoadBalancer() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
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
                                    .withSecretName(customCertServer1)
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
                                    .withSecretName(customCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec().done();

        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(customCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomChainCertificatesForLoadBalancer() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
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
                                    .withSecretName(customCertChain1)
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
                                    .withSecretName(customCertChain1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec().done();

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaTopicUtils.waitForKafkaTopicCreationByNamePrefix(topicName);

        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();
        KafkaUserUtils.waitForKafkaUserCreation(userName);

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(customRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

    @Test
    @Tag(ACCEPTANCE)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @OpenShiftOnly
    void testCustomSoloCertificatesForRoute() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
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
                                    .withSecretName(customCertServer1)
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
                                    .withSecretName(customCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec().done();

        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(customCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

    @Test
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @OpenShiftOnly
    void testCustomChainCertificatesForRoute() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
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
                                    .withSecretName(customCertChain1)
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
                                    .withSecretName(customCertChain1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec().done();

        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withCertificateAuthorityCertificateName(customRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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


    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testCustomCertLoadBalancerAndTlsRollingUpdate() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3)
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
            .endSpec().done();

        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        String externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        String externalSecretCerts = getKafkaSecretCertificates(CLUSTER_NAME + "-cluster-ca-cert", "ca.crt");

        String internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);

        LOGGER.info("Check if KafkaStatus certificates from external listeners are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        //External secret cert is same as internal in this case
        assertThat(externalSecretCerts, is(internalCerts));

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9113)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(customCertServer2)
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
                                    .withSecretName(customCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                            .build()
            )));
        });

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        KafkaUtils.waitForKafkaStatusUpdate(CLUSTER_NAME);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        externalSecretCerts = getKafkaSecretCertificates(customCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        String internalSecretCerts = getKafkaSecretCertificates(customCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        basicExternalKafkaClient = basicExternalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(customCertServer1)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

        SecretUtils.createCustomSecret(customCertServer1, CLUSTER_NAME, NAMESPACE,
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver2/strimzi/strimzi.pem")).getFile(),
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver2/strimzi/strimzi.key")).getFile());

        SecretUtils.createCustomSecret(customCertServer2, CLUSTER_NAME, NAMESPACE,
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver1/strimzi/strimzi.pem")).getFile(),
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver1/strimzi/strimzi.key")).getFile());

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        externalSecretCerts = getKafkaSecretCertificates(customCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        internalSecretCerts = getKafkaSecretCertificates(customCertServer2, "ca.crt");

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

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9113)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(customCertServer2)
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

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        KafkaUtils.waitForKafkaStatusUpdate(CLUSTER_NAME);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        externalSecretCerts = getKafkaSecretCertificates(CLUSTER_NAME + "-cluster-ca-cert", "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        internalSecretCerts = getKafkaSecretCertificates(customCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

    @Test
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testCustomCertNodePortAndTlsRollingUpdate() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3)
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
            .done();

        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        String externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        String externalSecretCerts = getKafkaSecretCertificates(CLUSTER_NAME + "-cluster-ca-cert", "ca.crt");

        String internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);

        LOGGER.info("Check if KafkaStatus certificates from external listeners are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        //External secret cert is same as internal in this case
        assertThat(externalSecretCerts, is(internalCerts));

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9115)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(customCertServer2)
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
                                    .withSecretName(customCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                            .build()
            )));
        });

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        KafkaUtils.waitForKafkaStatusUpdate(CLUSTER_NAME);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        externalSecretCerts = getKafkaSecretCertificates(customCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        String internalSecretCerts = getKafkaSecretCertificates(customCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        basicExternalKafkaClient = basicExternalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(customCertServer1)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesTls(),
            basicExternalKafkaClient.receiveMessagesTls()
        );

        // Deploy client pod with custom certificates and collect messages from internal TLS listener
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

        SecretUtils.createCustomSecret(customCertServer1, CLUSTER_NAME, NAMESPACE,
            Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver2/strimzi/strimzi.pem")).getFile(),
            Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver2/strimzi/strimzi.key")).getFile());

        SecretUtils.createCustomSecret(customCertServer2, CLUSTER_NAME, NAMESPACE,
            Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver1/strimzi/strimzi.pem")).getFile(),
            Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver1/strimzi/strimzi.key")).getFile());

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        externalSecretCerts = getKafkaSecretCertificates(customCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        internalSecretCerts = getKafkaSecretCertificates(customCertServer2, "ca.crt");

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

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9115)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(customCertServer2)
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

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        KafkaUtils.waitForKafkaStatusUpdate(CLUSTER_NAME);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        externalSecretCerts = getKafkaSecretCertificates(CLUSTER_NAME + "-cluster-ca-cert", "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        internalSecretCerts = getKafkaSecretCertificates(customCertServer2, "ca.crt");

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

    @Test
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @OpenShiftOnly
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testCustomCertRouteAndTlsRollingUpdate() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3)
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
            .endSpec().done();

        KafkaUser aliceUser = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        String externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        String externalSecretCerts = getKafkaSecretCertificates(CLUSTER_NAME + "-cluster-ca-cert", "ca.crt");

        String internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);

        LOGGER.info("Check if KafkaStatus certificates from external listeners are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        //External secret cert is same as internal in this case
        assertThat(externalSecretCerts, is(internalCerts));

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9117)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(customCertServer2)
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
                                    .withSecretName(customCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                            .build()
            )));
        });

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        KafkaUtils.waitForKafkaStatusUpdate(CLUSTER_NAME);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        externalSecretCerts = getKafkaSecretCertificates(customCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        String internalSecretCerts = getKafkaSecretCertificates(customCertServer2, "ca.crt");

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
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, false, aliceUser).done();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName())
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
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

        SecretUtils.createCustomSecret(customCertServer1, CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/ver2/strimzi/strimzi.pem").getFile(),
                getClass().getClassLoader().getResource("custom-certs/ver2/strimzi/strimzi.key").getFile());

        SecretUtils.createCustomSecret(customCertServer2, CLUSTER_NAME, NAMESPACE,
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver1/strimzi/strimzi.pem")).getFile(),
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver1/strimzi/strimzi.key")).getFile());

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        externalSecretCerts = getKafkaSecretCertificates(customCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        internalSecretCerts = getKafkaSecretCertificates(customCertServer2, "ca.crt");

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

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9117)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewConfiguration()
                                .withNewBrokerCertChainAndKey()
                                    .withSecretName(customCertServer2)
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

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        KafkaUtils.waitForKafkaStatusUpdate(CLUSTER_NAME);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        externalSecretCerts = getKafkaSecretCertificates(CLUSTER_NAME + "-cluster-ca-cert", "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, NAMESPACE, CLUSTER_NAME);
        internalSecretCerts = getKafkaSecretCertificates(customCertServer2, "ca.crt");

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

    @Test
    void testNonExistingCustomCertificate() {
        String nonExistingCertName = "non-existing-certificate";
        String clusterName = "broken-cluster";

        KafkaResource.kafkaWithoutWait(KafkaResource.defaultKafka(clusterName, 1, 1)
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
            .endSpec().build());

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(clusterName), 1);

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, NAMESPACE, ".*Secret " + nonExistingCertName + " with custom TLS certificate does not exist.*");

        KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).delete();
    }

    @Test
    void testCertificateWithNonExistingDataCrt() {
        String nonExistingCertName = "non-existing-crt";
        String clusterName = "broken-cluster";

        KafkaResource.kafkaWithoutWait(KafkaResource.defaultKafka(clusterName, 1, 1)
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
                                    .withSecretName(customCertServer1)
                                    .withKey("ca.key")
                                    .withCertificate(nonExistingCertName)
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec().build());

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(clusterName), 1);

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, NAMESPACE,
                ".*Secret " + customCertServer1 + " does not contain certificate under the key " + nonExistingCertName + ".*");

        KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).delete();
    }

    @Test
    void testCertificateWithNonExistingDataKey() {
        String nonExistingCertKey = "non-existing-key";
        String clusterName = "broken-cluster";

        KafkaResource.kafkaWithoutWait(KafkaResource.defaultKafka(clusterName, 1, 1)
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
                                    .withSecretName(customCertServer1)
                                    .withKey(nonExistingCertKey)
                                    .withCertificate("ca.crt")
                                .endBrokerCertChainAndKey()
                            .endConfiguration()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec().build());

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(clusterName), 1);

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, NAMESPACE,
                ".*Secret " + customCertServer1 + " does not contain custom certificate private key under the key " + nonExistingCertKey + ".*");

        KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).delete();
    }

    @BeforeEach
    void setupCertificates() {
        SecretUtils.deleteSecretWithWait(customCertChain1, NAMESPACE);
        SecretUtils.deleteSecretWithWait(customCertChain2, NAMESPACE);
        SecretUtils.deleteSecretWithWait(customCertServer1, NAMESPACE);
        SecretUtils.deleteSecretWithWait(customCertServer2, NAMESPACE);
        SecretUtils.deleteSecretWithWait(customRootCA1, NAMESPACE);
        SecretUtils.deleteSecretWithWait(customRootCA2, NAMESPACE);

        SecretUtils.createCustomSecret(customCertChain1, CLUSTER_NAME, NAMESPACE,
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver1/chain/strimzi-bundle.crt")).getFile(),
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver1/chain/strimzi-key.pem")).getFile());

        SecretUtils.createCustomSecret(customCertChain2, CLUSTER_NAME, NAMESPACE,
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver2/chain/strimzi-bundle.crt")).getFile(),
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver2/chain/strimzi-key.pem")).getFile());

        SecretUtils.createCustomSecret(customCertServer1, CLUSTER_NAME, NAMESPACE,
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver1/strimzi/strimzi.pem")).getFile(),
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver1/strimzi/strimzi.key")).getFile());

        SecretUtils.createCustomSecret(customCertServer2, CLUSTER_NAME, NAMESPACE,
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver2/strimzi/strimzi.pem")).getFile(),
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver2/strimzi/strimzi.key")).getFile());

        SecretUtils.createCustomSecret(customRootCA1, CLUSTER_NAME, NAMESPACE,
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver1/root/ca.pem")).getFile(),
                Objects.requireNonNull(getClass().getClassLoader().getResource("custom-certs/ver1/root/ca.key")).getFile());

        SecretUtils.createCustomSecret(customRootCA2, CLUSTER_NAME, NAMESPACE,
                getClass().getClassLoader().getResource("custom-certs/ver2/root/ca.pem").getFile(),
                getClass().getClassLoader().getResource("custom-certs/ver2/root/ca.key").getFile());
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);
    }

    @Override
    protected void tearDownEnvironmentAfterEach() throws Exception {
        super.tearDownEnvironmentAfterEach();
        kubeClient().getClient().persistentVolumeClaims().inNamespace(NAMESPACE).delete();
    }
}
