/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka.listeners;

import io.fabric8.kubernetes.api.model.LabelSelector;
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
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.security.CertAndKeyFiles;
import io.strimzi.systemtest.security.SystemTestCertAndKey;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.vertx.core.json.JsonArray;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.x509.GeneralName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Base64;
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
import static io.strimzi.systemtest.Constants.SANITY;
import static io.strimzi.systemtest.security.SystemTestCertManager.exportToPemFiles;
import static io.strimzi.systemtest.security.SystemTestCertManager.generateIntermediateCaCertAndKey;
import static io.strimzi.systemtest.security.SystemTestCertManager.generateEndEntityCertAndKey;
import static io.strimzi.systemtest.security.SystemTestCertManager.generateRootCaCertAndKey;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.getKafkaSecretCertificates;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.getKafkaStatusCertificates;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag(REGRESSION)
@ParallelSuite
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

    /**
     * Test sending messages over plain transport, without auth
     */
    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendMessagesPlainAnonymous(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        Service kafkaService = kubeClient(testStorage.getNamespaceName()).getService(testStorage.getNamespaceName(), KafkaResources.bootstrapServiceName(testStorage.getClusterName()));
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
        final TestStorage testStorage = new TestStorage(extensionContext);

        // Use a Kafka with plain listener disabled
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
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

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withUserName(testStorage.getUserName())
            .withTopicName(testStorage.getTopicName())
            .build();

        resourceManager.createResource(extensionContext,
            kafkaClients.producerTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        Service kafkaService = kubeClient(testStorage.getNamespaceName()).getService(testStorage.getNamespaceName(), KafkaResources.bootstrapServiceName(testStorage.getClusterName()));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(StUtils.expectedServiceDiscoveryInfo("none", Constants.TLS_LISTENER_DEFAULT_NAME, false, true), is(serviceDiscoveryArray));
    }

    /**
     * Test sending messages over plain transport using scram sha auth
     */
    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @KRaftNotSupported("Scram-sha is not supported by KRaft mode and is used in this test case")
    void testSendMessagesPlainScramSha(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        // Use a Kafka with plain listener disabled
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
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

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage).build()
        );

        String brokerPodLog = kubeClient(testStorage.getNamespaceName()).logsInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName() + "-kafka-0", "kafka");
        Pattern p = Pattern.compile("^.*" + Pattern.quote(testStorage.getUserName()) + ".*$", Pattern.MULTILINE);
        Matcher m = p.matcher(brokerPodLog);
        boolean found = false;
        while (m.find()) {
            found = true;
            LOGGER.info("Broker pod log line about user {}: {}", testStorage.getUserName(), m.group());
        }
        if (!found) {
            LOGGER.warn("No broker pod log lines about user {}", testStorage.getUserName());
            LOGGER.info("Broker pod log:\n----\n{}\n----\n", brokerPodLog);
        }

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9095")
            .withUserName(testStorage.getUserName())
            .withTopicName(testStorage.getTopicName())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerScramShaPlainStrimzi(), kafkaClients.consumerScramShaPlainStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        Service kafkaService = kubeClient(testStorage.getNamespaceName()).getService(testStorage.getNamespaceName(), KafkaResources.bootstrapServiceName(testStorage.getClusterName()));
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
    @KRaftNotSupported("Scram-sha is not supported by KRaft mode and is used in this test case")
    void testSendMessagesTlsScramSha(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final int passwordLength = 50;

        // Use a Kafka with plain listener disabled
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
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

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage).build()
        );

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9096")
            .withMessageCount(testStorage.getMessageCount())
            .withUserName(testStorage.getUserName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .build();

        resourceManager.createResource(extensionContext,
            kafkaClients.producerScramShaTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerScramShaTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Checking if generated password has {} characters", passwordLength);
        String password = kubeClient().namespace(testStorage.getNamespaceName()).getSecret(testStorage.getUserName()).getData().get("password");
        String decodedPassword = new String(Base64.getDecoder().decode(password));

        assertEquals(decodedPassword.length(), passwordLength);

        Service kafkaService = kubeClient(testStorage.getNamespaceName()).getService(testStorage.getNamespaceName(), KafkaResources.bootstrapServiceName(testStorage.getClusterName()));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(serviceDiscoveryArray, is(StUtils.expectedServiceDiscoveryInfo(9096, "kafka", "scram-sha-512", true)));
    }

    /**
     * Test custom listener configured with scram sha auth and tls.
     */
    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    @Tag(INTERNAL_CLIENTS_USED)
    @KRaftNotSupported("Scram-sha is not supported by KRaft mode and is used in this test case")
    void testSendMessagesCustomListenerTlsScramSha(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        // Use a Kafka with plain listener disabled
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withType(KafkaListenerType.INTERNAL)
                        .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9122)
                        .withTls(true)
                        .withNewKafkaListenerAuthenticationCustomAuth()
                        .withSasl(true)
                        .withListenerConfig(Map.of("scram-sha-512.sasl.jaas.config",
                                "org.apache.kafka.common.security.scram.ScramLoginModule required;",
                                "sasl.enabled.mechanisms", "SCRAM-SHA-512"))
                        .endKafkaListenerAuthenticationCustomAuth()
                        .build())
                .endKafka()
                .endSpec()
                .build());

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage).build()
        );

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9122")
            .withMessageCount(testStorage.getMessageCount())
            .withUserName(testStorage.getUserName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .build();

        resourceManager.createResource(extensionContext,
            kafkaClients.producerScramShaTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerScramShaTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testNodePort(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final Map<String, String> label = Collections.singletonMap("my-label", "value");
        final Map<String, String> anno = Collections.singletonMap("my-annotation", "value");

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
                    .withConfig(Collections.singletonMap("default.replication.factor", 3))
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

                List<String> nodeIps = kubeClient(namespaceName).listPods(KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName)))
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
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
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
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
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
                    .withConfig(Collections.singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(namespaceName, clusterName, userName).build());

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
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
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
                    .withConfig(Collections.singletonMap("default.replication.factor", 3))
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
    @Tag(SANITY)
    @Tag(ACCEPTANCE)
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testLoadBalancerTls(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
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
                    .withConfig(Collections.singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(namespaceName, clusterName, userName).build());

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

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testClusterIp(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        final String namespaceName = testStorage.getNamespaceName();
        final String clusterName = testStorage.getClusterName();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName(Constants.CLUSTER_IP_LISTENER_DEFAULT_NAME)
                        .withPort(9102)
                        .withType(KafkaListenerType.CLUSTER_IP)
                        .withTls(false)
                        .build())
                    .withConfig(Collections.singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
                .withNamespaceName(namespaceName)
                .withTopicName(testStorage.getTopicName())
                .withBootstrapAddress(KafkaUtils.bootstrapAddressFromStatus(clusterName, namespaceName, Constants.CLUSTER_IP_LISTENER_DEFAULT_NAME))
                .withMessageCount(MESSAGE_COUNT)
                .withUserName(testStorage.getUserName())
                .withProducerName(testStorage.getProducerName())
                .withConsumerName(testStorage.getConsumerName())
                .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getProducerName(), namespaceName, MESSAGE_COUNT);

        resourceManager.createResource(extensionContext, kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientSuccess(testStorage.getConsumerName(), namespaceName, MESSAGE_COUNT);

    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testClusterIpTls(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName(Constants.CLUSTER_IP_LISTENER_DEFAULT_NAME)
                        .withPort(9103)
                        .withType(KafkaListenerType.CLUSTER_IP)
                        .withTls(true)
                        .withAuth(new KafkaListenerAuthenticationTls())
                        .build())
                    .withConfig(Collections.singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(testStorage).build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
                .withNamespaceName(testStorage.getNamespaceName())
                .withTopicName(testStorage.getTopicName())
                .withBootstrapAddress(KafkaUtils.bootstrapAddressFromStatus(testStorage.getClusterName(), testStorage.getNamespaceName(), Constants.CLUSTER_IP_LISTENER_DEFAULT_NAME))
                .withMessageCount(testStorage.getMessageCount())
                .withUserName(testStorage.getUserName())
                .withProducerName(testStorage.getProducerName())
                .withConsumerName(testStorage.getConsumerName())
                .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

    }

//    ##########################################
//    #### Custom Certificates in Listeners ####
//    ##########################################

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomSoloCertificatesForNodePort(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String clusterCustomCertServer1 = testStorage.getClusterName() + "-" + customCertServer1;

        final SystemTestCertAndKey root1 = generateRootCaCertAndKey();
        final SystemTestCertAndKey intermediate1 = generateIntermediateCaCertAndKey(root1);
        final SystemTestCertAndKey strimzi1 = generateEndEntityCertAndKey(intermediate1, this.retrieveKafkaBrokerSANs(testStorage));

        final CertAndKeyFiles strimziCertAndKey1 = exportToPemFiles(strimzi1);

        SecretUtils.createCustomSecret(clusterCustomCertServer1, testStorage.getClusterName(), testStorage.getNamespaceName(), strimziCertAndKey1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 3)
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

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(testStorage).build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUserName())
            .withMessageCount(testStorage.getMessageCount())
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(customListenerName)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9104")
            .withMessageCount(testStorage.getMessageCount())
            .withUserName(testStorage.getUserName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withConsumerGroup("consumer-group-certs-1")
            .withCaCertSecretName(clusterCustomCertServer1)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(2 * testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomChainCertificatesForNodePort(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String clusterCustomCertChain1 = testStorage.getClusterName() + "-" + customCertChain1;
        final String clusterCustomRootCA1 = testStorage.getClusterName() + "-" + customRootCA1;

        final SystemTestCertAndKey root1 = generateRootCaCertAndKey();
        final SystemTestCertAndKey intermediate1 = generateIntermediateCaCertAndKey(root1);
        final SystemTestCertAndKey strimzi1 = generateEndEntityCertAndKey(intermediate1, this.retrieveKafkaBrokerSANs(testStorage));

        final CertAndKeyFiles rootCertAndKey1 = exportToPemFiles(root1);
        final CertAndKeyFiles chainCertAndKey1 = exportToPemFiles(strimzi1, intermediate1, root1);

        SecretUtils.createCustomSecret(clusterCustomCertChain1, testStorage.getClusterName(), testStorage.getNamespaceName(), chainCertAndKey1);
        SecretUtils.createCustomSecret(clusterCustomRootCA1, testStorage.getClusterName(), testStorage.getNamespaceName(), rootCertAndKey1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 1, 1)
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

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(testStorage).build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUserName())
            .withMessageCount(testStorage.getMessageCount())
            .withCertificateAuthorityCertificateName(clusterCustomRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9106")
            .withMessageCount(testStorage.getMessageCount())
            .withUserName(testStorage.getUserName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withCaCertSecretName(clusterCustomCertChain1)
            .withConsumerGroup("consumer-group-certs-2")
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(2 * testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomSoloCertificatesForLoadBalancer(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String clusterCustomCertServer1 = testStorage.getClusterName() + "-" + customCertServer1;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, testStorage.getClusterName(), testStorage.getNamespaceName(), STRIMZI_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
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


        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(testStorage).build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUserName())
            .withMessageCount(testStorage.getMessageCount())
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9107")
            .withMessageCount(testStorage.getMessageCount())
            .withUserName(testStorage.getUserName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withConsumerGroup("consumer-group-certs-3")
            .withCaCertSecretName(clusterCustomCertServer1)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(2 * testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    void testCustomChainCertificatesForLoadBalancer(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String clusterCustomCertChain1 = testStorage.getClusterName() + "-" + customCertChain1;
        final String clusterCustomRootCA1 = testStorage.getClusterName() + "-" + customRootCA1;

        SecretUtils.createCustomSecret(clusterCustomCertChain1, testStorage.getClusterName(), testStorage.getNamespaceName(), CHAIN_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomRootCA1, testStorage.getClusterName(), testStorage.getNamespaceName(), ROOT_CA_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
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

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUserName())
            .withMessageCount(testStorage.getMessageCount())
            .withCertificateAuthorityCertificateName(clusterCustomRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9109")
            .withMessageCount(testStorage.getMessageCount())
            .withUserName(testStorage.getUserName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withConsumerGroup("consumer-group-certs-4")
            .withCaCertSecretName(clusterCustomCertChain1)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(2 * testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(SANITY)
    @Tag(ACCEPTANCE)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @OpenShiftOnly
    void testCustomSoloCertificatesForRoute(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String clusterCustomCertServer1 = testStorage.getClusterName() + "-" + customCertServer1;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, testStorage.getClusterName(), testStorage.getNamespaceName(), STRIMZI_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
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

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(testStorage).build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUserName())
            .withMessageCount(testStorage.getMessageCount())
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9111")
            .withMessageCount(testStorage.getMessageCount())
            .withUserName(testStorage.getUserName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withConsumerGroup("consumer-group-certs-5")
            .withCaCertSecretName(clusterCustomCertServer1)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(2 * testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @OpenShiftOnly
    void testCustomChainCertificatesForRoute(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        final String clusterCustomCertChain1 = testStorage.getClusterName() + "-" + customCertChain1;
        final String clusterCustomRootCA1 = testStorage.getClusterName() + "-" + customRootCA1;

        SecretUtils.createCustomSecret(clusterCustomCertChain1, testStorage.getClusterName(), testStorage.getNamespaceName(), CHAIN_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomRootCA1, testStorage.getClusterName(), testStorage.getNamespaceName(), ROOT_CA_CERT_AND_KEY_1);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
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

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(testStorage).build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUserName())
            .withMessageCount(testStorage.getMessageCount())
            .withCertificateAuthorityCertificateName(clusterCustomRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9112")
            .withMessageCount(testStorage.getMessageCount())
            .withUserName(testStorage.getUserName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withConsumerGroup("consumer-group-certs-6")
            .withCaCertSecretName(clusterCustomCertChain1)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(2 * testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }


    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testCustomCertLoadBalancerAndTlsRollingUpdate(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String clusterCustomCertServer1 = testStorage.getClusterName() + "-" + customCertServer1;
        final String clusterCustomCertServer2 = testStorage.getClusterName() + "-" + customCertServer2;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, testStorage.getClusterName(), testStorage.getNamespaceName(), STRIMZI_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, testStorage.getClusterName(), testStorage.getNamespaceName(), STRIMZI_CERT_AND_KEY_2);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3)
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

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(testStorage).build());

        String externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());
        String externalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cluster-ca-cert", "ca.crt");

        String internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Check if KafkaStatus certificates from external listeners are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        //External secret cert is same as internal in this case
        assertThat(externalSecretCerts, is(internalCerts));

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUserName())
            .withMessageCount(testStorage.getMessageCount())
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withCertificateAuthorityCertificateName(null)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> kafkaSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
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
        }, testStorage.getNamespaceName());

        kafkaSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaSnapshot);

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());
        externalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());
        String internalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer2, "ca.crt");

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

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9113")
            .withMessageCount(testStorage.getMessageCount())
            .withUserName(testStorage.getUserName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withConsumerGroup("consumer-group-certs-6")
            .withCaCertSecretName(clusterCustomCertServer2)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(3 * testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientSuccess(testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount() * 3);

        SecretUtils.createCustomSecret(clusterCustomCertServer1, testStorage.getClusterName(), testStorage.getNamespaceName(), STRIMZI_CERT_AND_KEY_2);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, testStorage.getClusterName(), testStorage.getNamespaceName(), STRIMZI_CERT_AND_KEY_1);

        kafkaSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaSnapshot);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());
        externalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());
        internalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        int sent = externalKafkaClient.sendMessagesTls() + testStorage.getMessageCount();

        externalKafkaClient.setMessageCount(2 * testStorage.getMessageCount());

        externalKafkaClient.verifyProducedAndConsumedMessages(
            sent,
            externalKafkaClient.receiveMessagesTls()
        );

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withConsumerGroup("consumer-group-certs-71")
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(testStorage.getMessageCount() * 5)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientSuccess(testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount() * 5);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
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
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaSnapshot);

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());
        externalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cluster-ca-cert", "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());
        internalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(null)
            .withMessageCount(testStorage.getMessageCount())
            .build();

        sent = externalKafkaClient.sendMessagesTls() + testStorage.getMessageCount();

        externalKafkaClient.setMessageCount(2 * testStorage.getMessageCount());

        externalKafkaClient.verifyProducedAndConsumedMessages(
            sent,
            externalKafkaClient.receiveMessagesTls()
        );

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withConsumerGroup("consumer-group-certs-83")
            .withMessageCount(testStorage.getMessageCount() * 6)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientSuccess(testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount() * 6);
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testCustomCertNodePortAndTlsRollingUpdate(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String clusterCustomCertServer1 = testStorage.getClusterName() + "-" + customCertServer1;
        final String clusterCustomCertServer2 = testStorage.getClusterName() + "-" + customCertServer2;

        final SystemTestCertAndKey root1 = generateRootCaCertAndKey();
        final SystemTestCertAndKey intermediate1 = generateIntermediateCaCertAndKey(root1);
        final SystemTestCertAndKey strimzi1 = generateEndEntityCertAndKey(intermediate1, this.retrieveKafkaBrokerSANs(testStorage));

        final CertAndKeyFiles strimziCertAndKey1 = exportToPemFiles(strimzi1);

        final SystemTestCertAndKey root2 = generateRootCaCertAndKey();
        final SystemTestCertAndKey intermediate2 = generateIntermediateCaCertAndKey(root2);
        final SystemTestCertAndKey strimzi2 = generateEndEntityCertAndKey(intermediate2, this.retrieveKafkaBrokerSANs(testStorage));

        final CertAndKeyFiles strimziCertAndKey2 = exportToPemFiles(strimzi2);

        SecretUtils.createCustomSecret(clusterCustomCertServer1, testStorage.getClusterName(), testStorage.getNamespaceName(), strimziCertAndKey1);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, testStorage.getClusterName(), testStorage.getNamespaceName(), strimziCertAndKey2);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3)
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


        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(testStorage).build();
        resourceManager.createResource(extensionContext, aliceUser);

        StUtils.waitUntilSuppliersAreMatching(
            // external certs
            () -> getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName()),
            // external secret certs
            () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cluster-ca-cert", "ca.crt"));
        StUtils.waitUntilSuppliersAreMatching(
            // external secret certs
            () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cluster-ca-cert", "ca.crt"),
            // internal certs
            () -> getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName()));


        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUserName())
            .withMessageCount(testStorage.getMessageCount())
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> kafkaSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
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
        }, testStorage.getNamespaceName());

        kafkaSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaSnapshot);

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());

        StUtils.waitUntilSuppliersAreMatching(
            // external certs
            () -> getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName()),
            // external secret certs
            () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer1, "ca.crt"));
        StUtils.waitUntilSuppliersAreMatching(
            // internal certs
            () -> getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName()),
            // internal secret certs
            () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer2, "ca.crt"));

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        int expectedMessageCountForNewGroup = testStorage.getMessageCount() * 3;

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9115")
            .withMessageCount(testStorage.getMessageCount())
            .withUserName(testStorage.getUserName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withConsumerGroup("consumer-group-certs-71")
            .withCaCertSecretName(clusterCustomCertServer2)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        int expectedMessageCountForExternalClient = testStorage.getMessageCount();

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(expectedMessageCountForNewGroup)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientSuccess(testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount() * 3);

        SecretUtils.createCustomSecret(clusterCustomCertServer1, testStorage.getClusterName(), testStorage.getNamespaceName(), strimziCertAndKey2);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, testStorage.getClusterName(), testStorage.getNamespaceName(), strimziCertAndKey1);

        kafkaSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaSnapshot);

        StUtils.waitUntilSuppliersAreMatching(
                // external certs
                () -> getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName()),
                // external secret certs
                () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer1, "ca.crt"));
        StUtils.waitUntilSuppliersAreMatching(
                // internal certs
                () -> getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName()),
                // internal secret certs
                () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer2, "ca.crt"));


        externalKafkaClient.verifyProducedAndConsumedMessages(
            expectedMessageCountForExternalClient,
            externalKafkaClient.receiveMessagesTls()
        );

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withConsumerGroup("consumer-group-certs-72")
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        expectedMessageCountForNewGroup += testStorage.getMessageCount();

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(expectedMessageCountForNewGroup)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
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
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaSnapshot);

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());

        StUtils.waitUntilSuppliersAreMatching(
                // external certs
                () -> getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName()),
                // external secret certs
                () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cluster-ca-cert", "ca.crt"));
        StUtils.waitUntilSuppliersAreMatching(
                // internal certs
                () -> getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName()),
                // internal secret certs
                () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer2, "ca.crt"));

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(null)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            expectedMessageCountForExternalClient,
            externalKafkaClient.receiveMessagesTls()
        );

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withConsumerGroup("consumer-group-certs-73")
            .withMessageCount(expectedMessageCountForNewGroup)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @OpenShiftOnly
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testCustomCertRouteAndTlsRollingUpdate(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String clusterCustomCertServer1 = testStorage.getClusterName() + "-" + customCertServer1;
        final String clusterCustomCertServer2 = testStorage.getClusterName() + "-" + customCertServer2;

        SecretUtils.createCustomSecret(clusterCustomCertServer1, testStorage.getClusterName(), testStorage.getNamespaceName(), STRIMZI_CERT_AND_KEY_1);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, testStorage.getClusterName(), testStorage.getNamespaceName(), STRIMZI_CERT_AND_KEY_2);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3)
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

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(testStorage).build());

        String externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());
        String externalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cluster-ca-cert", "ca.crt");

        String internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Check if KafkaStatus certificates from external listeners are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        //External secret cert is same as internal in this case
        assertThat(externalSecretCerts, is(internalCerts));

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUserName())
            .withMessageCount(testStorage.getMessageCount())
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withCertificateAuthorityCertificateName(null)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> kafkaSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
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
        }, testStorage.getNamespaceName());

        kafkaSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaSnapshot);

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());
        externalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());
        String internalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(null)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls() + testStorage.getMessageCount(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9117")
            .withMessageCount(testStorage.getMessageCount())
            .withUserName(testStorage.getUserName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withCaCertSecretName(clusterCustomCertServer2)
            .withConsumerGroup("consumer-group-certs-91")
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(testStorage.getMessageCount() * 3)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        SecretUtils.createCustomSecret(clusterCustomCertServer1, testStorage.getClusterName(), testStorage.getNamespaceName(), STRIMZI_CERT_AND_KEY_2);
        SecretUtils.createCustomSecret(clusterCustomCertServer2, testStorage.getClusterName(), testStorage.getNamespaceName(), STRIMZI_CERT_AND_KEY_1);

        kafkaSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaSnapshot);

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());
        externalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());
        internalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls() + testStorage.getMessageCount(),
            externalKafkaClient.receiveMessagesTls()
        );

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withConsumerGroup("consumer-group-certs-92")
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(testStorage.getMessageCount() * 5)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
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
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaSnapshot);

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());

        externalCerts = getKafkaStatusCertificates(Constants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());
        externalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cluster-ca-cert", "ca.crt");

        internalCerts = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, testStorage.getNamespaceName(), testStorage.getClusterName());
        internalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(),  clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(null)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        int sent = 6 * testStorage.getMessageCount();

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withMessageCount(6 * testStorage.getMessageCount())
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            sent,
            externalKafkaClient.receiveMessagesTls()
        );

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(testStorage.getMessageCount() * 6)
            .withConsumerGroup("consumer-group-certs-93")
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    void testNonExistingCustomCertificate(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String nonExistingCertName = "non-existing-certificate";
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

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

        if (!Environment.isKRaftModeEnabled()) {
            PodUtils.waitForPodsReady(namespaceName, zkSelector, 1, true);
        }

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, namespaceName, ".*Secret " + nonExistingCertName + " with custom TLS certificate does not exist.*");

        KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).delete();
    }

    @ParallelNamespaceTest
    void testCertificateWithNonExistingDataCrt(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String nonExistingCertName = "non-existing-crt";
        final String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

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

        if (!Environment.isKRaftModeEnabled()) {
            PodUtils.waitForPodsReady(namespaceName, zkSelector, 1, true);
        }

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, namespaceName,
                ".*Secret " + clusterCustomCertServer1 + " does not contain certificate under the key " + nonExistingCertName + ".*");

        KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).delete();
    }

    @ParallelNamespaceTest
    void testCertificateWithNonExistingDataKey(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String nonExistingCertKey = "non-existing-key";
        final String clusterCustomCertServer1 = clusterName + "-" + customCertServer1;
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

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

        if (!Environment.isKRaftModeEnabled()) {
            PodUtils.waitForPodsReady(namespaceName, zkSelector, 1, true);
        }

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, namespaceName,
                ".*Secret " + clusterCustomCertServer1 + " does not contain custom certificate private key under the key " + nonExistingCertKey + ".*");

        KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).delete();
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("Scram-sha is not supported by KRaft mode and is used in this test case")
    void testMessagesTlsScramShaWithPredefinedPassword(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        // FIPS needs the passwords at least 32 characters long
        final String firstUnencodedPassword = "completely_secret_password_long_enough_for_fips";
        final String secondUnencodedPassword = "completely_different_secret_password";

        final String firstEncodedPassword = Base64.getEncoder().encodeToString(firstUnencodedPassword.getBytes(StandardCharsets.UTF_8));
        final String secondEncodedPassword = Base64.getEncoder().encodeToString(secondUnencodedPassword.getBytes(StandardCharsets.UTF_8));

        final String secretName = testStorage.getClusterName() + "-secret";

        Secret password = new SecretBuilder()
            .withNewMetadata()
                .withName(secretName)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .addToData("password", firstEncodedPassword)
            .build();

        kubeClient().namespace(testStorage.getNamespaceName()).createSecret(password);
        assertThat("Password in secret is not correct", kubeClient().namespace(testStorage.getNamespaceName()).getSecret(secretName).getData().get("password"), is(firstEncodedPassword));

        KafkaUser kafkaUser = KafkaUserTemplates.scramShaUser(testStorage)
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

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
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
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getUserName()).build()
        );

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9096")
            .withMessageCount(testStorage.getMessageCount())
            .withUserName(testStorage.getUserName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .build();

        resourceManager.createResource(extensionContext,
            kafkaClients.producerScramShaTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerScramShaTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Changing password in secret: {}, we should be able to send/receive messages", secretName);

        password = new SecretBuilder(password)
            .addToData("password", secondEncodedPassword)
            .build();

        kubeClient().namespace(testStorage.getNamespaceName()).createSecret(password);
        SecretUtils.waitForUserPasswordChange(testStorage.getNamespaceName(), testStorage.getUserName(), secondEncodedPassword);

        LOGGER.info("Receiving messages with new password");

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResource(extensionContext,
            kafkaClients.producerScramShaTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerScramShaTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    @Tag(NODEPORT_SUPPORTED)
    @ParallelNamespaceTest
    void testAdvertisedHostNamesAppearsInBrokerCerts(ExtensionContext extensionContext) throws CertificateException {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);

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

    @AfterEach
    void afterEach(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        kubeClient(namespaceName).getClient().persistentVolumeClaims().inNamespace(namespaceName).delete();
    }

    private ASN1Encodable[] retrieveKafkaBrokerSANs(final TestStorage testStorage) {
        return new ASN1Encodable[] {
            new GeneralName(GeneralName.dNSName, "*.127.0.0.1.nip.io"),
            new GeneralName(GeneralName.dNSName, "*." + testStorage.getClusterName() + "-kafka-brokers"),
            new GeneralName(GeneralName.dNSName, "*." + testStorage.getClusterName() + "-kafka-brokers." + testStorage.getNamespaceName() + ".svc"),
            new GeneralName(GeneralName.dNSName, testStorage.getClusterName() + "-kafka-bootstrap"),
            new GeneralName(GeneralName.dNSName, testStorage.getClusterName() + "-kafka-bootstrap." + testStorage.getNamespaceName() + ".svc")
        };
    }
}
