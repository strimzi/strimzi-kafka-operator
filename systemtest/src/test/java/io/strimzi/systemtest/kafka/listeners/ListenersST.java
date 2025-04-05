/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka.listeners;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.listener.ListenerAddress;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.security.CertAndKeyFiles;
import io.strimzi.systemtest.security.SystemTestCertAndKey;
import io.strimzi.systemtest.security.SystemTestCertManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

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

import static io.strimzi.systemtest.TestTags.ACCEPTANCE;
import static io.strimzi.systemtest.TestTags.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestTags.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.TestTags.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.ROUTE;
import static io.strimzi.systemtest.TestTags.SANITY;
import static io.strimzi.systemtest.security.SystemTestCertManager.exportToPemFiles;
import static io.strimzi.systemtest.security.SystemTestCertManager.generateEndEntityCertAndKey;
import static io.strimzi.systemtest.security.SystemTestCertManager.generateIntermediateCaCertAndKey;
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
@SuiteDoc(
    description = @Desc("This class demonstrates various tests for Kafka listeners using different authentication mechanisms."),
    beforeTestSteps = {
        @Step(value = "Install the Cluster Operator with default settings.", expected = "Cluster Operator is installed successfully.")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA)
    }
)
public class ListenersST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(ListenersST.class);

    private final String customCertChain1 = "custom-certificate-chain-1";
    private final String customCertServer1 = "custom-certificate-server-1";
    private final String customCertServer2 = "custom-certificate-server-2";
    private final String customRootCA1 = "custom-certificate-root-1";
    private final String customListenerName = "randname";

    /**
     * Test sending messages over plain transport, without auth
     */
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test sending messages over plain transport, without auth"),
        steps = {
            @Step(value = "Create Kafka resources with wait.", expected = "Kafka broker, controller, and topic are created."),
            @Step(value = "Log transmission message.", expected = "Transmission message is logged."),
            @Step(value = "Produce and consume messages with plain clients.", expected = "Messages are successfully produced and consumed."),
            @Step(value = "Validate Kafka service discovery annotation.", expected = "The discovery annotation is validated successfully.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testSendMessagesPlainAnonymous() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());
        kubeResourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        LOGGER.info("Transmitting messages over plain transport and without auth.Bootstrap address: {}", KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage);
        kubeResourceManager.createResourceWithWait(kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        Service kafkaService = kubeClient(testStorage.getNamespaceName()).getService(testStorage.getNamespaceName(), KafkaResources.bootstrapServiceName(testStorage.getClusterName()));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(StUtils.expectedServiceDiscoveryInfo("none", "none", false, true), is(serviceDiscoveryArray));
    }

    /**
     * Test sending messages over tls transport using mutual tls auth
     */
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test sending messages over tls transport using mutual tls auth."),
        steps = {
            @Step(value = "CreateKafkaNodePool resources.", expected = "Persistent storage KafkaNodePools are created."),
            @Step(value = "Disable plain listener and enable tls listener in Kafka resource.", expected = "Kafka with plain listener disabled and tls listener enabled is created."),
            @Step(value = "Create Kafka topic and user.", expected = "Kafka topic and tls user are created."),
            @Step(value = "Configure and deploy Kafka clients.", expected = "Kafka clients producer and consumer with tls are deployed."),
            @Step(value = "Wait for clients to successfully send and receive messages.", expected = "Clients successfully send and receive messages over tls."),
            @Step(value = "Assert that the service discovery contains expected info.", expected = "Service discovery matches expected info.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testSendMessagesTlsAuthenticated() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        // Use a Kafka with plain listener disabled
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(
                        new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withTls(false)
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                        .build()
                    )
                .endKafka()
            .endSpec()
            .build());

        kubeResourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withUsername(testStorage.getUsername())
            .withTopicName(testStorage.getTopicName())
            .build();

        kubeResourceManager.createResourceWithWait(
            kafkaClients.producerTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        Service kafkaService = kubeClient(testStorage.getNamespaceName()).getService(testStorage.getNamespaceName(), KafkaResources.bootstrapServiceName(testStorage.getClusterName()));
        String kafkaServiceDiscoveryAnnotation = kafkaService.getMetadata().getAnnotations().get("strimzi.io/discovery");
        JsonArray serviceDiscoveryArray = new JsonArray(kafkaServiceDiscoveryAnnotation);
        assertThat(StUtils.expectedServiceDiscoveryInfo("none", TestConstants.TLS_LISTENER_DEFAULT_NAME, false, true), is(serviceDiscoveryArray));
    }

    /**
     * Test sending messages over plain transport using scram sha auth
     */
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test sending messages over plain transport using scram sha auth."),
        steps = {
            @Step(value = "Create Kafka brokers and controllers.", expected = "Kafka brokers and controllers are created."),
            @Step(value = "Enable Kafka with plain listener disabled and scram sha auth.", expected = "Kafka instance with scram sha auth is enabled on a specified listener."),
            @Step(value = "Set up topic and user.", expected = "Kafka topic and Kafka user are set up with scram sha auth credentials."),
            @Step(value = "Check logs in broker pod for authentication.", expected = "Logs show that scram sha authentication succeeded."),
            @Step(value = "Send messages over plain transport using scram sha authentication.", expected = "Messages are successfully sent over plain transport using scram sha auth."),
            @Step(value = "Verify service discovery annotation.", expected = "Service discovery annotation is checked and validated.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testSendMessagesPlainScramSha() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        // Use a Kafka with plain listener disabled
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
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

        kubeResourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaUserTemplates.scramShaUser(testStorage).build()
        );

        String brokerPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        String brokerPodLog = kubeClient(testStorage.getNamespaceName()).logsInSpecificNamespace(testStorage.getNamespaceName(),
            brokerPodName, "kafka");

        Pattern p = Pattern.compile("^.*" + Pattern.quote(testStorage.getUsername()) + ".*$", Pattern.MULTILINE);
        Matcher m = p.matcher(brokerPodLog);
        boolean found = false;
        while (m.find()) {
            found = true;
            LOGGER.info("Broker Pod log line about user: {} -> {}", testStorage.getUsername(), m.group());
        }
        if (!found) {
            LOGGER.warn("No Broker Pod log lines about user: {}/{}", testStorage.getNamespaceName(), testStorage.getUsername());
            LOGGER.info("Broker Pod log:\n----\n{}\n----\n", brokerPodLog);
        }

        final String boostrapAddress = KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9095";
        LOGGER.info("Transmitting messages over plain transport using scram sha auth with bootstrap address: {}", boostrapAddress);
        final KafkaClients kafkaClients = ClientUtils.getInstantScramShaClients(testStorage, boostrapAddress);
        kubeResourceManager.createResourceWithWait(kafkaClients.producerScramShaPlainStrimzi(), kafkaClients.consumerScramShaPlainStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

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
    @TestDoc(
        description = @Desc("Test sending messages over TLS transport using SCRAM-SHA authentication."),
        steps = {
            @Step(value = "Create resources for KafkaNodePools.", expected = "KafkaNodePools are created."),
            @Step(value = "Create Kafka cluster with SCRAM-SHA-512 authentication.", expected = "Kafka cluster is created with SCRAM-SHA authentication."),
            @Step(value = "Create Kafka topic and user.", expected = "Kafka topic and user are created."),
            @Step(value = "Transmit messages over TLS using SCRAM-SHA.", expected = "Messages are successfully transmitted."),
            @Step(value = "Check if generated password has the expected length.", expected = "Password length is as expected."),
            @Step(value = "Verify Kafka service discovery annotation.", expected = "Service discovery annotation is as expected.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testSendMessagesTlsScramSha() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int passwordLength = 50;

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        // Use a Kafka with plain listener disabled
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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

        kubeResourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaUserTemplates.scramShaUser(testStorage).build()
        );

        final String boostrapAddress = KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9096";
        LOGGER.info("Transmitting messages over tls using scram sha auth with bootstrap address: {}", boostrapAddress);
        KafkaClients kafkaClients = ClientUtils.getInstantScramShaClients(testStorage, boostrapAddress);
        kubeResourceManager.createResourceWithWait(
            kafkaClients.producerScramShaTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerScramShaTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Checking if generated password has {} characters", passwordLength);
        String password = kubeClient().namespace(testStorage.getNamespaceName()).getSecret(testStorage.getUsername()).getData().get("password");
        String decodedPassword = Util.decodeFromBase64(password);

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
    @TestDoc(
        description = @Desc("Test custom listener configured with scram SHA authentication and TLS."),
        steps = {
            @Step(value = "Create a Kafka cluster with broker and controller KafkaNodePools.", expected = "Kafka cluster is created with KafkaNodePools."),
            @Step(value = "Create a Kafka cluster with custom listener using TLS and SCRAM-SHA authentication.", expected = "Kafka cluster with custom listener is ready."),
            @Step(value = "Create a Kafka topic and SCRAM-SHA user.", expected = "Kafka topic and user are created."),
            @Step(value = "Transmit messages over TLS using SCRAM-SHA authentication.", expected = "Messages are transmitted successfully.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testSendMessagesCustomListenerTlsScramSha() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        // Use a Kafka with plain listener disabled
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withType(KafkaListenerType.INTERNAL)
                        .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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

        kubeResourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaUserTemplates.scramShaUser(testStorage).build()
        );


        final String boostrapAddress = KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9122";
        LOGGER.info("Transmitting messages over tls using scram sha auth with bootstrap address: {}", boostrapAddress);
        final KafkaClients kafkaClients = ClientUtils.getInstantScramShaClients(testStorage, boostrapAddress);
        kubeResourceManager.createResourceWithWait(
            kafkaClients.producerScramShaTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerScramShaTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @TestDoc(
        description = @Desc("Test checking the functionality of Kafka cluster with NodePort external listener configurations."),
        steps = {
            @Step(value = "Create resource with Kafka broker pool and controller pool.", expected = "Resources with Kafka pools are created successfully."),
            @Step(value = "Create Kafka cluster with NodePort and TLS listeners.", expected = "Kafka cluster is set up with the specified listeners."),
            @Step(value = "Create ExternalKafkaClient and verify message production and consumption.", expected = "Messages are produced and consumed successfully."),
            @Step(value = "Check Kafka status for proper listener addresses.", expected = "Listener addresses in Kafka status are validated successfully."),
            @Step(value = "Check ClusterRoleBinding annotations and labels in Kafka cluster.", expected = "Annotations and labels match the expected values.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testNodePort() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final Map<String, String> label = Collections.singletonMap("my-label", "value");
        final Map<String, String> anno = Collections.singletonMap("my-annotation", "value");

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9097)
                            .withTls(true)
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.NODEPORT)
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withMessageCount(testStorage.getMessageCount())
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesPlain(),
            externalKafkaClient.receiveMessagesPlain()
        );

        StUtils.waitUntilSupplierIsSatisfied("Kafka status contains proper data about listeners (check test log for more info)", () -> {
            // Check that Kafka status has correct addresses in NodePort external listener part
            for (ListenerStatus listenerStatus : KafkaResource.getKafkaStatus(testStorage.getNamespaceName(), testStorage.getClusterName()).getListeners()) {
                if (listenerStatus.getName().equals(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)) {
                    List<String> listStatusAddresses = listenerStatus.getAddresses().stream().map(ListenerAddress::getHost).sorted(Comparator.comparing(String::toString)).toList();
                    List<Integer> listStatusPorts = listenerStatus.getAddresses().stream().map(ListenerAddress::getPort).toList();
                    Integer nodePort = kubeClient(testStorage.getNamespaceName()).getService(testStorage.getNamespaceName(), testStorage.getClusterName() + "-kafka-external-bootstrap").getSpec().getPorts().get(0).getNodePort();

                    List<String> nodeIPsBrokers = kubeClient(testStorage.getNamespaceName()).listPods(testStorage.getBrokerSelector())
                            .stream().map(pods -> pods.getStatus().getHostIP()).distinct().sorted(Comparator.comparing(String::toString)).toList();

                    List<String> nodeIPsControllers = new java.util.ArrayList<>(kubeClient(testStorage.getNamespaceName()).listPods(testStorage.getControllerSelector())
                            .stream().map(pods -> pods.getStatus().getHostIP()).distinct().sorted(Comparator.comparing(String::toString)).toList());

                    // Remove all nodes that contains broker nodes
                    nodeIPsControllers.removeAll(nodeIPsBrokers);

                    if (!listStatusAddresses.equals(nodeIPsBrokers)) {
                        LOGGER.info("Expected IPs: {}, actual IPs: {}. Waiting for next round of validation", nodeIPsBrokers, listStatusAddresses);
                        return false;
                    }

                    for (Integer port : listStatusPorts) {
                        if (!listStatusAddresses.equals(nodeIPsBrokers)) {
                            LOGGER.info("Expected Port: {}, actual Port: {}. Waiting for next round of validation", nodePort, port);
                            return false;
                        }
                    }

                    boolean containsAny = listStatusAddresses.stream().anyMatch(nodeIPsControllers::contains);
                    if (containsAny) {
                        LOGGER.info("ListenerStatus Addresses {} contains some of controller nodes {}. Waiting for next round of validation", listStatusAddresses, nodeIPsControllers);
                        return false;
                    }
                }
            }
            return true;
        });

        // check the ClusterRoleBinding annotations and labels in Kafka cluster
        Map<String, String> actualLabel = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getKafka().getTemplate().getClusterRoleBinding().getMetadata().getLabels();
        Map<String, String> actualAnno = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getKafka().getTemplate().getClusterRoleBinding().getMetadata().getAnnotations();

        assertThat(actualLabel, is(label));
        assertThat(actualAnno, is(anno));
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @TestDoc(
        description = @Desc("Test verifying that NodePort configuration can be overridden for Kafka brokers and bootstrap service."),
        steps = {
            @Step(value = "Create Kafka broker and controller pools using resource manager.", expected = "Kafka broker and controller pools are created successfully."),
            @Step(value = "Deploy Kafka cluster with overridden NodePort configuration for brokers and bootstrap.", expected = "Kafka cluster is deployed with specified NodePort values."),
            @Step(value = "Verify that the bootstrap service NodePort matches the configured value.", expected = "Bootstrap NodePort matches the configured value of 32100."),
            @Step(value = "Verify that the broker service NodePort matches the configured value.", expected = "Broker NodePort matches the configured value of 32000."),
            @Step(value = "Produce and consume messages using an external Kafka client.", expected = "Messages are produced and consumed successfully using the external client.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testOverrideNodePortConfiguration() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final int brokerNodePort = 32000;
        final int brokerId = 0;
        final int clusterBootstrapNodePort = 32100;

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.INTERNAL)
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9099)
                            .withTls(true)
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withType(KafkaListenerType.NODEPORT)
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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

        LOGGER.info("Checking nodePort to {} for bootstrap service {}", clusterBootstrapNodePort, testStorage.getClusterName() + "-kafka-external-bootstrap");
        assertThat(kubeClient(testStorage.getNamespaceName()).getService(testStorage.getNamespaceName(), testStorage.getClusterName() + "-kafka-external-bootstrap")
                .getSpec().getPorts().get(0).getNodePort(), is(clusterBootstrapNodePort));
        String firstExternalService = KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), KafkaNodePoolResource.getBrokerPoolName(testStorage.getClusterName())) + "-" + TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME + "-" + 0;
        LOGGER.info("Checking nodePort to {} for kafka-broker service {}", brokerNodePort, firstExternalService);
        assertThat(kubeClient(testStorage.getNamespaceName()).getService(testStorage.getNamespaceName(), firstExternalService)
                .getSpec().getPorts().get(0).getNodePort(), is(brokerNodePort));

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withMessageCount(testStorage.getMessageCount())
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesPlain(),
            externalKafkaClient.receiveMessagesPlain()
        );
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @TestDoc(
        description = @Desc("Test the NodePort TLS functionality for Kafka brokers in a Kubernetes environment."),
        steps = {
            @Step(value = "Create Kafka broker and controller KafkaNodePools.", expected = "Broker and controller KafkaNodePools are created"),
            @Step(value = "Deploy Kafka cluster with NodePort listener and TLS enabled", expected = "Kafka cluster is deployed with NodePort listener and TLS"),
            @Step(value = "Create a Kafka topic", expected = "Kafka topic is created"),
            @Step(value = "Create a Kafka user with TLS authentication", expected = "Kafka user with TLS authentication is created"),
            @Step(value = "Configure external Kafka client and send and receive messages using TLS", expected = "External Kafka client sends and receives messages using TLS successfully")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testNodePortTls() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9101)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                        .build())
                    .withConfig(Collections.singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .build());

        kubeResourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName()).build());
        kubeResourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getKafkaUsername(), testStorage.getClusterName()).build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withMessageCount(testStorage.getMessageCount())
            .withKafkaUsername(testStorage.getKafkaUsername())
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );
    }

    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @TestDoc(
        description = @Desc("Test verifying load balancer functionality with external clients."),
        steps = {
            @Step(value = "Create Kafka broker and controller KafkaNodePools.", expected = "Broker and controller KafkaNodePools are created"),
            @Step(value = "Create Kafka cluster with ephemeral storage and load balancer listener", expected = "Kafka cluster is created with the specified configuration"),
            @Step(value = "Wait until the load balancer address is reachable", expected = "Address is reachable"),
            @Step(value = "Configure external Kafka client and send messages", expected = "Messages are sent successfully"),
            @Step(value = "Verify that messages are correctly produced and consumed", expected = "Messages are produced and consumed as expected")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testLoadBalancer() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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

        ServiceUtils.waitUntilAddressIsReachable(KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getListeners().get(0).getAddresses().get(0).getHost());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withMessageCount(testStorage.getMessageCount())
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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
    @TestDoc(
        description = @Desc("Test validating the TLS connection through a Kafka LoadBalancer."),
        steps = {
            @Step(value = "Create and configure KafkaNodePools", expected = "KafkaNodePools for brokers and controllers are created"),
            @Step(value = "Create and configure Kafka cluster with TLS listener", expected = "Kafka cluster with TLS enabled LoadBalancer listener is created"),
            @Step(value = "Create and configure Kafka user with TLS authentication", expected = "Kafka user with TLS authentication is created"),
            @Step(value = "Wait for the LoadBalancer address to be reachable", expected = "LoadBalancer address becomes reachable"),
            @Step(value = "Send and receive messages using external Kafka client", expected = "Messages are successfully produced and consumed over the TLS connection")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testLoadBalancerTls() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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

        kubeResourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getKafkaUsername(), testStorage.getClusterName()).build());

        ServiceUtils.waitUntilAddressIsReachable(KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getListeners().get(0).getAddresses().get(0).getHost());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withMessageCount(testStorage.getMessageCount())
            .withKafkaUsername(testStorage.getKafkaUsername())
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test verifies the functionality of Kafka with a cluster IP listener."),
        steps = {
            @Step(value = "Create the Kafka broker and controller pools", expected = "Kafka broker and controller pools are created"),
            @Step(value = "Create the Kafka cluster with a cluster IP listener", expected = "Kafka cluster with cluster IP listener is created"),
            @Step(value = "Retrieve the cluster IP bootstrap address", expected = "Cluster IP bootstrap address is correctly retrieved"),
            @Step(value = "Deploy Kafka clients", expected = "Kafka clients are deployed successfully"),
            @Step(value = "Wait for Kafka clients to succeed", expected = "Kafka clients successfully produce and consume messages")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testClusterIp() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName(TestConstants.CLUSTER_IP_LISTENER_DEFAULT_NAME)
                        .withPort(9102)
                        .withType(KafkaListenerType.CLUSTER_IP)
                        .withTls(false)
                        .build())
                    .withConfig(Collections.singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .build());

        final String clusterIPBoostrapAddress = KafkaUtils.bootstrapAddressFromStatus(testStorage.getNamespaceName(), testStorage.getClusterName(), TestConstants.CLUSTER_IP_LISTENER_DEFAULT_NAME);
        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage, clusterIPBoostrapAddress);
        kubeResourceManager.createResourceWithWait(kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test validates the creation of Kafka resources with TLS authentication, ensuring proper setup and functionality of the Kafka cluster in a parallel namespace."),
        steps = {
            @Step(value = "Create ephemeral Kafka cluster with TLS enabled on ClusterIP listener", expected = "Kafka cluster is created with TLS enabled listener on port 9103"),
            @Step(value = "Create Kafka user with TLS authentication", expected = "Kafka user is created successfully"),
            @Step(value = "Retrieve the ClusterIP bootstrap address for the Kafka cluster", expected = "Bootstrap address for the Kafka cluster is retrieved"),
            @Step(value = "Instantiate TLS Kafka Clients (producer and consumer)", expected = "TLS Kafka clients are instantiated successfully"),
            @Step(value = "Wait for the Kafka Clients to complete their tasks and verify success", expected = "Kafka Clients complete their tasks successfully")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testClusterIpTls() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName(TestConstants.CLUSTER_IP_LISTENER_DEFAULT_NAME)
                        .withPort(9103)
                        .withType(KafkaListenerType.CLUSTER_IP)
                        .withTls(true)
                        .withAuth(new KafkaListenerAuthenticationTls())
                        .build())
                    .withConfig(Collections.singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .build());

        kubeResourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        final String clusterIPBoostrapAddress = KafkaUtils.bootstrapAddressFromStatus(testStorage.getNamespaceName(), testStorage.getClusterName(), TestConstants.CLUSTER_IP_LISTENER_DEFAULT_NAME);
        final KafkaClients kafkaClients = ClientUtils.getInstantTlsClients(testStorage, clusterIPBoostrapAddress);
        kubeResourceManager.createResourceWithWait(
            kafkaClients.producerTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

//    ##########################################
//    #### Custom Certificates in Listeners ####
//    ##########################################

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @TestDoc(
        description = @Desc("Test custom certificates in Kafka listeners, specifically for the NodePort type."),
        steps = {
            @Step(value = "Generate Root CA certificate and key", expected = "Root CA certificate and key are generated"),
            @Step(value = "Generate Intermediate CA certificate and key using Root CA", expected = "Intermediate CA certificate and key are generated"),
            @Step(value = "Generate Kafka Broker certificate and key using Intermediate CA", expected = "Broker certificate and key are generated"),
            @Step(value = "Export generated certificates and keys to PEM files", expected = "PEM files are created with certificates and keys"),
            @Step(value = "Create custom secret with the PEM files", expected = "Custom secret is created within the required namespace"),
            @Step(value = "Deploy and wait for Kafka cluster resources with custom certificates", expected = "Kafka cluster is deployed successfully with custom certificates"),
            @Step(value = "Create and wait for TLS KafkaUser", expected = "TLS KafkaUser is created successfully"),
            @Step(value = "Produce and consume messages using ExternalKafkaClient", expected = "Messages are successfully produced and consumed"),
            @Step(value = "Produce and consume messages using internal TLS client", expected = "Messages are successfully produced and consumed with internal TLS client")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testCustomSoloCertificatesForNodePort() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String clusterCustomCertServer1 = testStorage.getClusterName() + "-" + customCertServer1;
        final CertAndKeyFiles strimziCertAndKey1 = SystemTestCertManager.createBrokerCertChain(testStorage);

        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer1, strimziCertAndKey1);

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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

        kubeResourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUsername())
            .withMessageCount(testStorage.getMessageCount())
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(customListenerName)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9104")
            .withConsumerGroup("consumer-group-certs-1")
            .withCaCertSecretName(clusterCustomCertServer1)
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(2 * testStorage.getMessageCount())
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @TestDoc(
        description = @Desc("Test verifies the custom chain certificates configuration for Kafka NodePort listener."),
        steps = {
            @Step(value = "Generate custom root CA and intermediate certificates.", expected = "Root and intermediate certificates are generated."),
            @Step(value = "Generate end entity certificates using intermediate CA.", expected = "End entity certificates are generated."),
            @Step(value = "Export certificates to PEM files.", expected = "Certificates are exported to PEM files."),
            @Step(value = "Create Kubernetes secrets with the custom certificates.", expected = "Custom certificate secrets are created."),
            @Step(value = "Deploy Kafka cluster with NodePort listener using the custom certificates.", expected = "Kafka cluster is deployed successfully."),
            @Step(value = "Create a Kafka user with TLS authentication.", expected = "Kafka user is created."),
            @Step(value = "Verify message production and consumption with external Kafka client.", expected = "Messages are produced and consumed successfully."),
            @Step(value = "Verify message production with internal Kafka client.", expected = "Messages are produced successfully."),
            @Step(value = "Verify message consumption with internal Kafka client.", expected = "Messages are consumed successfully.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testCustomChainCertificatesForNodePort() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String clusterCustomCertChain1 = testStorage.getClusterName() + "-" + customCertChain1;
        final String clusterCustomRootCA1 = testStorage.getClusterName() + "-" + customRootCA1;

        final SystemTestCertAndKey root1 = generateRootCaCertAndKey();
        final SystemTestCertAndKey intermediate1 = generateIntermediateCaCertAndKey(root1);
        final SystemTestCertAndKey strimzi1 = generateEndEntityCertAndKey(intermediate1, SystemTestCertManager.retrieveKafkaBrokerSANs(testStorage));

        final CertAndKeyFiles rootCertAndKey1 = exportToPemFiles(root1);
        final CertAndKeyFiles chainCertAndKey1 = exportToPemFiles(strimzi1, intermediate1, root1);

        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertChain1, chainCertAndKey1);
        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomRootCA1, rootCertAndKey1);

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
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
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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

        kubeResourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUsername())
            .withMessageCount(testStorage.getMessageCount())
            .withCertificateAuthorityCertificateName(clusterCustomRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = ClientUtils.getInstantPlainClientBuilder(testStorage, KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9106")
            .withUsername(testStorage.getUsername())
            .withCaCertSecretName(clusterCustomCertChain1)
            .withConsumerGroup("consumer-group-certs-2")
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(2 * testStorage.getMessageCount())
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @TestDoc(
        description = @Desc("Test verifying custom solo certificates for load balancer in a Kafka cluster."),
        steps = {
            @Step(value = "Create custom secret", expected = "Custom secret is created with the specified certificate and key"),
            @Step(value = "Create Kafka resources with KafkaNodePools", expected = "Kafka brokers and controller pools are created and configured"),
            @Step(value = "Create Kafka cluster with listeners", expected = "Kafka cluster is created with internal and load balancer listeners using the custom certificates"),
            @Step(value = "Create TLS user", expected = "TLS user is created"),
            @Step(value = "Verify produced and consumed messages via external client", expected = "Messages are successfully produced and consumed using the custom certificates"),
            @Step(value = "Create and verify TLS producer client", expected = "TLS producer client is created and verified for success"),
            @Step(value = "Create and verify TLS consumer client", expected = "TLS consumer client is created and verified for success")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testCustomSoloCertificatesForLoadBalancer() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String clusterCustomCertServer1 = testStorage.getClusterName() + "-" + customCertServer1;
        final CertAndKeyFiles strimziCertAndKey1 = SystemTestCertManager.createBrokerCertChain(testStorage);

        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer1, strimziCertAndKey1);

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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


        kubeResourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUsername())
            .withMessageCount(testStorage.getMessageCount())
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9107")
            .withConsumerGroup("consumer-group-certs-3")
            .withCaCertSecretName(clusterCustomCertServer1)
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(2 * testStorage.getMessageCount())
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @TestDoc(
        description = @Desc("Verifies custom certificate chain configuration for Kafka load balancer, ensuring proper secret creation, resource setup, and message sending/receiving functionality."),
        steps = {
            @Step(value = "Create custom secrets for certificate chains and root CA", expected = "Secrets are created successfully"),
            @Step(value = "Deploy Kafka broker and controller pools with custom certificates", expected = "Kafka pools are deployed without issues"),
            @Step(value = "Deploy Kafka cluster with custom listener configurations", expected = "Kafka cluster is deployed with custom listener configurations"),
            @Step(value = "Set up Kafka topic and user", expected = "Kafka topic and user are created successfully"),
            @Step(value = "Verify message production and consumption via external Kafka client with TLS", expected = "Messages are produced and consumed successfully"),
            @Step(value = "Set up Kafka clients for further messaging operations", expected = "Kafka clients are set up without issues"),
            @Step(value = "Produce messages using Kafka producer", expected = "Messages are produced successfully"),
            @Step(value = "Consume messages using Kafka consumer", expected = "Messages are consumed successfully")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testCustomChainCertificatesForLoadBalancer() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String clusterCustomCertChain1 = testStorage.getClusterName() + "-" + customCertChain1;
        final String clusterCustomRootCA1 = testStorage.getClusterName() + "-" + customRootCA1;

        final SystemTestCertAndKey root1 = generateRootCaCertAndKey();
        final SystemTestCertAndKey intermediate1 = generateIntermediateCaCertAndKey(root1);
        final SystemTestCertAndKey strimzi1 = generateEndEntityCertAndKey(intermediate1, SystemTestCertManager.retrieveKafkaBrokerSANs(testStorage));
        final CertAndKeyFiles rootCertAndKey1 = exportToPemFiles(root1);
        final CertAndKeyFiles chainCertAndKey1 = exportToPemFiles(strimzi1, intermediate1, root1);

        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertChain1, chainCertAndKey1);
        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomRootCA1, rootCertAndKey1);

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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

        kubeResourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUsername())
            .withMessageCount(testStorage.getMessageCount())
            .withCertificateAuthorityCertificateName(clusterCustomRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9109")
            .withConsumerGroup("consumer-group-certs-4")
            .withCaCertSecretName(clusterCustomCertChain1)
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(2 * testStorage.getMessageCount())
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(SANITY)
    @Tag(ACCEPTANCE)
    @Tag(ROUTE)
    @Tag(EXTERNAL_CLIENTS_USED)
    @OpenShiftOnly
    @TestDoc(
        description = @Desc("Test custom solo certificates for Kafka route and client communication."),
        steps = {
            @Step(value = "Generate root CA certificate and key", expected = "Root CA certificate and key are generated"),
            @Step(value = "Generate intermediate CA certificate and key", expected = "Intermediate CA certificate and key are generated"),
            @Step(value = "Generate end-entity certificate and key for Strimzi", expected = "End-entity certificate and key for Strimzi are generated"),
            @Step(value = "Export certificates and keys to PEM files", expected = "Certificates and keys are exported to PEM files"),
            @Step(value = "Create custom secret with certificates and keys", expected = "Custom secret is created in the namespace with certificates and keys"),
            @Step(value = "Deploy Kafka cluster with custom certificates", expected = "Kafka cluster is deployed with custom certificates"),
            @Step(value = "Create TLS Kafka user", expected = "TLS Kafka user is created"),
            @Step(value = "Verify client communication using external Kafka client", expected = "Messages are successfully produced and consumed using external Kafka client"),
            @Step(value = "Deploy Kafka clients with custom certificates", expected = "Kafka clients are deployed with custom certificates"),
            @Step(value = "Verify client communication using internal Kafka client", expected = "Messages are successfully produced and consumed using internal Kafka client")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testCustomSoloCertificatesForRoute() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String clusterCustomCertServer1 = testStorage.getClusterName() + "-" + customCertServer1;
        final CertAndKeyFiles strimziCertAndKey1 = SystemTestCertManager.createBrokerCertChain(testStorage);

        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer1, strimziCertAndKey1);

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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

        kubeResourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUsername())
            .withMessageCount(testStorage.getMessageCount())
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9111")
            .withConsumerGroup("consumer-group-certs-5")
            .withCaCertSecretName(clusterCustomCertServer1)
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(2 * testStorage.getMessageCount())
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(ROUTE)
    @OpenShiftOnly
    @TestDoc(
        description = @Desc("Test to verify custom chain certificates for a Kafka Route."),
        steps = {
            @Step(value = "Generate root and intermediate certificates", expected = "Root and intermediate CA keys are generated"),
            @Step(value = "Create cluster custom certificate chain and root CA secrets", expected = "Custom certificate chain and root CA secrets are created in OpenShift"),
            @Step(value = "Create Kafka cluster with custom certificates", expected = "Kafka cluster is deployed with custom certificates for internal and external listeners"),
            @Step(value = "Create Kafka user", expected = "Kafka user with TLS authentication is created"),
            @Step(value = "Verify message production and consumption with external Kafka client", expected = "Messages are produced and consumed successfully using the external Kafka client"),
            @Step(value = "Create Kafka clients for internal message production and consumption", expected = "Internal Kafka clients are created and configured with TLS authentication"),
            @Step(value = "Verify internal message production with Kafka client", expected = "Messages are produced successfully using the internal Kafka client"),
            @Step(value = "Verify internal message consumption with Kafka client", expected = "Messages are consumed successfully using the internal Kafka client")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testCustomChainCertificatesForRoute() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final String clusterCustomCertChain1 = testStorage.getClusterName() + "-" + customCertChain1;
        final String clusterCustomRootCA1 = testStorage.getClusterName() + "-" + customRootCA1;

        final SystemTestCertAndKey root1 = generateRootCaCertAndKey();
        final SystemTestCertAndKey intermediate1 = generateIntermediateCaCertAndKey(root1);
        final SystemTestCertAndKey strimzi1 = generateEndEntityCertAndKey(intermediate1, SystemTestCertManager.retrieveKafkaBrokerSANs(testStorage));

        final CertAndKeyFiles rootCertAndKey1 = exportToPemFiles(root1);
        final CertAndKeyFiles chainCertAndKey1 = exportToPemFiles(strimzi1, intermediate1, root1);

        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertChain1, chainCertAndKey1);
        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomRootCA1, rootCertAndKey1);

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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

        kubeResourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUsername())
            .withMessageCount(testStorage.getMessageCount())
            .withCertificateAuthorityCertificateName(clusterCustomRootCA1)
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9112")
            .withConsumerGroup("consumer-group-certs-6")
            .withCaCertSecretName(clusterCustomCertChain1)
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(2 * testStorage.getMessageCount())
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @SuppressWarnings({"checkstyle:MethodLength"})
    @TestDoc(
        description = @Desc("This test verifies the behavior of Kafka with custom certificates for load balancer and TLS rolling updates."),
        steps = {
            @Step(value = "Create custom secrets for Kafka clusters", expected = "Secrets created and available in namespace"),
            @Step(value = "Deploy Kafka resources with load balancer and internal TLS listener", expected = "Kafka resources deployed with respective configurations"),
            @Step(value = "Create Kafka user and retrieve certificates", expected = "Kafka user created and certificates retrieved from Kafka status and secrets"),
            @Step(value = "Compare Kafka certificates with secret certificates", expected = "Certificates from Kafka status and secrets match"),
            @Step(value = "Verify message production and consumption using an external Kafka client", expected = "Messages successfully produced and consumed over SSL"),
            @Step(value = "Trigger and verify TLS rolling update", expected = "TLS rolling update completed successfully"),
            @Step(value = "Repeat certificate verification steps after rolling update", expected = "Certificates from Kafka status and secrets match post update"),
            @Step(value = "Repeatedly produce and consume messages to ensure Kafka stability", expected = "Messages successfully produced and consumed, ensuring stability"),
            @Step(value = "Revert the certificate updates and verify Kafka status", expected = "Certificates reverted and verified, Kafka operates normally")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testCustomCertLoadBalancerAndTlsRollingUpdate() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String clusterCustomCertServer1 = testStorage.getClusterName() + "-" + customCertServer1;
        final String clusterCustomCertServer2 = testStorage.getClusterName() + "-" + customCertServer2;
        final CertAndKeyFiles strimziCertAndKey1 = SystemTestCertManager.createBrokerCertChain(testStorage);
        final CertAndKeyFiles strimziCertAndKey2 = SystemTestCertManager.createBrokerCertChain(testStorage);

        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer1, strimziCertAndKey1);
        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer2, strimziCertAndKey2);

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9113)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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

        kubeResourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        String externalCerts = getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getClusterName());
        String externalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cluster-ca-cert", "ca.crt");

        String internalCerts = getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.TLS_LISTENER_DEFAULT_NAME, testStorage.getClusterName());

        LOGGER.info("Check if KafkaStatus certificates from external listeners are the same as Secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as Secret certificates");
        //External secret cert is same as internal in this case
        assertThat(externalSecretCerts, is(internalCerts));

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUsername())
            .withMessageCount(testStorage.getMessageCount())
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withCertificateAuthorityCertificateName(null)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> kafkaSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().setListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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
        });

        kafkaSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, kafkaSnapshot);

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());

        externalCerts = getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getClusterName());
        externalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.TLS_LISTENER_DEFAULT_NAME, testStorage.getClusterName());
        String internalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as Secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as Secret certificates");
        assertThat(internalSecretCerts, is(internalCerts));

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9113")
            .withConsumerGroup("consumer-group-certs-6")
            .withCaCertSecretName(clusterCustomCertServer2)
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(3 * testStorage.getMessageCount())
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount() * 3);

        externalCerts = getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getClusterName());
        externalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer1, "ca.crt");

        internalCerts = getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.TLS_LISTENER_DEFAULT_NAME, testStorage.getClusterName());
        internalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as Secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as Secret certificates");
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

        kubeResourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(testStorage.getMessageCount() * 5)
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount() * 5);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().setListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9114)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withNewConfiguration()
                                .withFinalizers(LB_FINALIZERS)
                            .endConfiguration()
                            .withTls(true)
                            .build()
            ));
        });

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, kafkaSnapshot);

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());

        externalCerts = getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getClusterName());
        externalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cluster-ca-cert", "ca.crt");

        internalCerts = getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.TLS_LISTENER_DEFAULT_NAME, testStorage.getClusterName());
        internalSecretCerts = getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer2, "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as Secret certificates");
        assertThat(externalSecretCerts, is(externalCerts));
        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as Secret certificates");
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

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount() * 6);
    }

    @ParallelNamespaceTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @SuppressWarnings({"checkstyle:MethodLength"})
    @TestDoc(
        description = @Desc("Test verifies custom certificates with NodePort and rolling update in Kafka."),
        steps = {
            @Step(value = "Generate root and intermediate certificates", expected = "Certificates are generated successfully"),
            @Step(value = "Generate end-entity certificates", expected = "End-entity certificates are generated successfully"),
            @Step(value = "Create custom secrets with generated certificates", expected = "Secrets are created in Kubernetes"),
            @Step(value = "Deploy Kafka cluster with custom NodePort and TLS settings", expected = "Kafka cluster is deployed and running"),
            @Step(value = "Verify messages sent and received through external Kafka client", expected = "Messages are produced and consumed successfully"),
            @Step(value = "Perform rolling update and update certificates in custom secrets", expected = "Rolling update is performed and certificates are updated"),
            @Step(value = "Verify messages sent and received after rolling update", expected = "Messages are produced and consumed successfully after update"),
            @Step(value = "Restore default certificate configuration and perform rolling update", expected = "Default certificates are restored and rolling update is completed"),
            @Step(value = "Verify messages sent and received with restored configuration", expected = "Messages are produced and consumed successfully with restored configuration")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testCustomCertNodePortAndTlsRollingUpdate() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String clusterCustomCertServer1 = testStorage.getClusterName() + "-" + customCertServer1;
        final String clusterCustomCertServer2 = testStorage.getClusterName() + "-" + customCertServer2;
        final CertAndKeyFiles strimziCertAndKey1 = SystemTestCertManager.createBrokerCertChain(testStorage);
        final CertAndKeyFiles strimziCertAndKey2 = SystemTestCertManager.createBrokerCertChain(testStorage);

        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer1, strimziCertAndKey1);
        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer2, strimziCertAndKey2);

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9115)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9116)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                        .build())
                .endKafka()
            .endSpec()
            .build());


        KafkaUser aliceUser = KafkaUserTemplates.tlsUser(testStorage).build();
        kubeResourceManager.createResourceWithWait(aliceUser);

        StUtils.waitUntilSuppliersAreMatching(
            // external certs
            () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
            // external secret certs
            () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cluster-ca-cert", "ca.crt"));
        StUtils.waitUntilSuppliersAreMatching(
            // external secret certs
            () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cluster-ca-cert", "ca.crt"),
            // internal certs
            () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.TLS_LISTENER_DEFAULT_NAME, testStorage.getClusterName()));


        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUsername())
            .withMessageCount(testStorage.getMessageCount())
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> kafkaSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().setListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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
        });

        kafkaSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, kafkaSnapshot);

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());

        StUtils.waitUntilSuppliersAreMatching(
            // external certs
            () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
            // external secret certs
            () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer1, "ca.crt"));
        StUtils.waitUntilSuppliersAreMatching(
            // internal certs
            () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.TLS_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
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

        KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9115")
            .withConsumerGroup("consumer-group-certs-71")
            .withCaCertSecretName(clusterCustomCertServer2)
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        int expectedMessageCountForExternalClient = testStorage.getMessageCount();

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(expectedMessageCountForNewGroup)
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount() * 3);

        SecretUtils.updateCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer1, strimziCertAndKey2);
        SecretUtils.updateCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer2, strimziCertAndKey1);

        kafkaSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, kafkaSnapshot);

        StUtils.waitUntilSuppliersAreMatching(
                // external certs
                () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
                // external secret certs
                () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer1, "ca.crt"));
        StUtils.waitUntilSuppliersAreMatching(
                // internal certs
                () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.TLS_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
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

        kubeResourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        expectedMessageCountForNewGroup += testStorage.getMessageCount();

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(expectedMessageCountForNewGroup)
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().setListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9116)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .build()
            ));
        });

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, kafkaSnapshot);

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());

        StUtils.waitUntilSuppliersAreMatching(
                // external certs
                () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
                // external secret certs
                () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cluster-ca-cert", "ca.crt"));
        StUtils.waitUntilSuppliersAreMatching(
                // internal certs
                () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.TLS_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
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

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(ROUTE)
    @OpenShiftOnly
    @SuppressWarnings({"checkstyle:MethodLength"})
    @TestDoc(
        description = @Desc("This test verifies the custom certificate handling and TLS rolling update mechanisms for Kafka brokers using OpenShift-specific configurations."),
        steps = {
            @Step(value = "Create various certificate chains and export them to PEM files", expected = "Certificates are created and exported successfully"),
            @Step(value = "Create custom secrets with the generated certificates", expected = "Secrets are created in the specified namespace"),
            @Step(value = "Deploy Kafka cluster and TLS user with specified configurations", expected = "Kafka cluster and TLS user are deployed successfully"),
            @Step(value = "Verify certificates in KafkaStatus match those in the secrets", expected = "Certificates are verified to match"),
            @Step(value = "Use external Kafka client to produce and consume messages", expected = "Messages are produced and consumed successfully"),
            @Step(value = "Update Kafka listeners with new certificates and perform rolling update", expected = "Kafka cluster rolls out successfully with updated certificates"),
            @Step(value = "Verify certificates in KafkaStatus match after update", expected = "Certificates are verified to match after the update"),
            @Step(value = "Repeat message production and consumption with updated certificates", expected = "Messages are produced and consumed successfully with new certificates")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testCustomCertRouteAndTlsRollingUpdate() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String clusterCustomCertServer1 = testStorage.getClusterName() + "-" + customCertServer1;
        final String clusterCustomCertServer2 = testStorage.getClusterName() + "-" + customCertServer2;
        final CertAndKeyFiles strimziCertAndKey1 = SystemTestCertManager.createBrokerCertChain(testStorage);
        final CertAndKeyFiles strimziCertAndKey2 = SystemTestCertManager.createBrokerCertChain(testStorage);

        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer1, strimziCertAndKey1);
        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer2, strimziCertAndKey2);

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9117)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                        .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9118)
                            .withType(KafkaListenerType.ROUTE)
                            .withTls(true)
                        .build())
                .endKafka()
            .endSpec()
            .build());

        kubeResourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");

        KafkaUtils.waitForKafkaSecretAndStatusCertsMatches(
            () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
            () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cluster-ca-cert", "ca.crt")
        );

        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");

        KafkaUtils.waitForKafkaSecretAndStatusCertsMatches(
            () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.TLS_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
            () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getClusterName())
        );

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withClusterName(testStorage.getClusterName())
            .withKafkaUsername(testStorage.getUsername())
            .withMessageCount(testStorage.getMessageCount())
            .withSecurityProtocol(SecurityProtocol.SSL)
            .withCertificateAuthorityCertificateName(null)
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls(),
            externalKafkaClient.receiveMessagesTls()
        );

        Map<String, String> kafkaSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().setListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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
        });

        kafkaSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, kafkaSnapshot);

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");

        KafkaUtils.waitForKafkaSecretAndStatusCertsMatches(
            () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
            () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer1, "ca.crt")
        );

        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");

        KafkaUtils.waitForKafkaSecretAndStatusCertsMatches(
            () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.TLS_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
            () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer2, "ca.crt")
        );

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(clusterCustomCertServer1)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls() + testStorage.getMessageCount(),
            externalKafkaClient.receiveMessagesTls()
        );

        KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9117")
            .withCaCertSecretName(clusterCustomCertServer2)
            .withConsumerGroup("consumer-group-certs-91")
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(testStorage.getMessageCount() * 3)
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // Delete already existing secrets
        SecretUtils.deleteSecretWithWait(testStorage.getNamespaceName(), clusterCustomCertServer1);
        SecretUtils.deleteSecretWithWait(testStorage.getNamespaceName(), clusterCustomCertServer2);
        // Create Secrets with new values (update)
        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer1, strimziCertAndKey2);
        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer2, strimziCertAndKey1);

        kafkaSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, kafkaSnapshot);

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");

        KafkaUtils.waitForKafkaSecretAndStatusCertsMatches(
            () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
            () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer1, "ca.crt")
        );

        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");

        KafkaUtils.waitForKafkaSecretAndStatusCertsMatches(
            () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.TLS_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
            () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), clusterCustomCertServer2, "ca.crt")
        );

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesTls() + testStorage.getMessageCount(),
            externalKafkaClient.receiveMessagesTls()
        );

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withConsumerGroup("consumer-group-certs-92")
            .withMessageCount(testStorage.getMessageCount())
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withMessageCount(testStorage.getMessageCount() * 5)
            .build();

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().setListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9118)
                            .withType(KafkaListenerType.ROUTE)
                            .withTls(true)
                            .build()
            ));
        });

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, kafkaSnapshot);

        KafkaUtils.waitForKafkaStatusUpdate(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");

        KafkaUtils.waitForKafkaSecretAndStatusCertsMatches(
            () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
            () -> getKafkaSecretCertificates(testStorage.getNamespaceName(), testStorage.getClusterName() + "-cluster-ca-cert", "ca.crt")
        );

        LOGGER.info("Check if KafkaStatus certificates from internal TLS listener are the same as secret certificates");

        KafkaUtils.waitForKafkaSecretAndStatusCertsMatches(
            () -> getKafkaStatusCertificates(testStorage.getNamespaceName(), TestConstants.TLS_LISTENER_DEFAULT_NAME, testStorage.getClusterName()),
            () -> getKafkaSecretCertificates(testStorage.getNamespaceName(),  clusterCustomCertServer2, "ca.crt")
        );

        externalKafkaClient = externalKafkaClient.toBuilder()
            .withCertificateAuthorityCertificateName(null)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        externalKafkaClient.sendMessagesTls();

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

        kubeResourceManager.createResourceWithWait(kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test for verifying non-existing custom certificate handling by creating necessary resources and ensuring correct error message check."),
        steps = {
            @Step(value = "Create necessary KafkaNodePools", expected = "KafkaNodePools are created and initialized"),
            @Step(value = "Create Kafka cluster with a listener using non-existing certificate", expected = "Kafka cluster resource is initialized with non-existing TLS certificate"),
            @Step(value = "Wait for pods to be ready if not in KRaft mode", expected = "Pods are ready"),
            @Step(value = "Wait for Kafka status condition message indicating the non-existing secret", expected = "Correct error message regarding the non-existing secret appears")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testNonExistingCustomCertificate() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String nonExistingCertName = "non-existing-certificate";

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        kubeResourceManager.createResourceWithoutWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(testStorage.getNamespaceName(), testStorage.getClusterName(), ".*Secret " + nonExistingCertName + " with custom TLS certificate does not exist.*");
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test checking behavior when Kafka is configured with a non-existing certificate in the TLS listener."),
        steps = {
            @Step(value = "Define non-existing certificate name.", expected = "Non-existing certificate name is defined."),
            @Step(value = "Create a custom secret for Kafka with the defined certificate.", expected = "Custom secret created successfully."),
            @Step(value = "Create KafkaNodePool resources.", expected = "KafkaNodePool resources created."),
            @Step(value = "Create Kafka cluster with ephemeral storage and the non-existing certificate.", expected = "Kafka cluster creation initiated."),
            @Step(value = "Wait for controller pods to be ready if in non-KRaft mode.", expected = "Controller pods are ready."),
            @Step(value = "Wait until Kafka status message indicates missing certificate.", expected = "Error message about missing certificate is found in Kafka status condition.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testCertificateWithNonExistingDataCrt() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String nonExistingCertName = "non-existing-crt";
        final String clusterCustomCertServer1 = testStorage.getClusterName() + "-" + customCertServer1;
        final CertAndKeyFiles strimziCertAndKey1 = SystemTestCertManager.createBrokerCertChain(testStorage);

        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer1, strimziCertAndKey1);

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        kubeResourceManager.createResourceWithoutWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(testStorage.getNamespaceName(), testStorage.getClusterName(),
            ".*Secret " + clusterCustomCertServer1 + " does not contain certificate under the key " + nonExistingCertName + ".*");
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test verifies that a Kafka cluster correctly identifies and reports the absence of a specified custom certificate private key."),
        steps = {
            @Step(value = "Define the non-existing certificate key.", expected = "The non-existing certificate key string is defined."),
            @Step(value = "Create a custom secret with a certificate for Kafka server.", expected = "Custom secret is created in the namespace."),
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Resources are created and ready."),
            @Step(value = "Deploy a Kafka cluster with a listener using the custom secret and non-existing key.", expected = "Deployment initiated without waiting for the resources to be ready."),
            @Step(value = "If not in KRaft mode, wait for controller pods to be ready.", expected = "Controller pods are in ready state (if applicable)."),
            @Step(value = "Check Kafka status condition for custom certificate error message.", expected = "Error message indicating the missing custom certificate private key is present in Kafka status conditions.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testCertificateWithNonExistingDataKey() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String nonExistingCertKey = "non-existing-key";
        final String clusterCustomCertServer1 = testStorage.getClusterName() + "-" + customCertServer1;
        final CertAndKeyFiles strimziCertAndKey1 = SystemTestCertManager.createBrokerCertChain(testStorage);

        SecretUtils.createCustomSecret(testStorage.getNamespaceName(), testStorage.getClusterName(), clusterCustomCertServer1, strimziCertAndKey1);

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        kubeResourceManager.createResourceWithoutWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(testStorage.getNamespaceName(), testStorage.getClusterName(),
            ".*Secret " + clusterCustomCertServer1 + " does not contain custom certificate private key under the key " + nonExistingCertKey + ".*");
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Validates that messages can be sent and received over TLS with SCRAM-SHA authentication using a predefined password, and that the password can be updated and still be functional."),
        steps = {
            @Step(value = "Create and encode the initial password", expected = "Initial password is encoded"),
            @Step(value = "Create and encode the secondary password", expected = "Secondary password is encoded"),
            @Step(value = "Create a secret in Kubernetes with the initial password", expected = "Secret is created and contains the initial password"),
            @Step(value = "Verify the password in the secret", expected = "Password in the secret is verified to be correct"),
            @Step(value = "Create a KafkaUser with SCRAM-SHA authentication using the secret", expected = "KafkaUser is created with correct authentication settings"),
            @Step(value = "Create Kafka cluster and topic with SCRAM-SHA authentication", expected = "Kafka cluster and topic are created correctly"),
            @Step(value = "Produce and consume messages using TLS and SCRAM-SHA", expected = "Messages are successfully transmitted and received"),
            @Step(value = "Update the secret with the secondary password", expected = "Secret is updated with the new password"),
            @Step(value = "Wait for the user password change to take effect", expected = "Password change is detected and applied"),
            @Step(value = "Produce and consume messages with the updated password", expected = "Messages are successfully transmitted and received with the new password")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testMessagesTlsScramShaWithPredefinedPassword() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

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

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withType(KafkaListenerType.INTERNAL)
                        .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9096)
                        .withTls(true)
                        .withNewKafkaListenerAuthenticationScramSha512Auth()
                        .endKafkaListenerAuthenticationScramSha512Auth()
                        .build())
                .endKafka()
            .endSpec()
            .build(),
            kafkaUser,
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getUsername(), testStorage.getClusterName()).build()
        );

        final String boostrapAddress = KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9096";
        LOGGER.info("Transmitting messages over tls using scram sha auth with bootstrap address: {} with predefined password", boostrapAddress);
        KafkaClients kafkaClients = ClientUtils.getInstantScramShaClients(testStorage, boostrapAddress);
        kubeResourceManager.createResourceWithWait(
            kafkaClients.producerScramShaTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerScramShaTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Changing password in Secret: {}/{}, we should be able to send/receive messages", testStorage.getNamespaceName(), secretName);

        password = new SecretBuilder(password)
            .addToData("password", secondEncodedPassword)
            .build();

        kubeClient().namespace(testStorage.getNamespaceName()).updateSecret(password);
        SecretUtils.waitForUserPasswordChange(testStorage.getNamespaceName(), testStorage.getUsername(), secondEncodedPassword);

        LOGGER.info("Receiving messages with new password");

        kafkaClients.generateNewConsumerGroup();

        kubeResourceManager.createResourceWithWait(
            kafkaClients.producerScramShaTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerScramShaTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @Tag(NODEPORT_SUPPORTED)
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Verify that advertised hostnames appear correctly in broker certificates."),
        steps = {
            @Step(value = "Define internal and external advertised hostnames and ports", expected = "Hostnames and ports are defined and listed"),
            @Step(value = "Create broker configurations with advertised hostnames and ports", expected = "Broker configurations are created"),
            @Step(value = "Deploy resources with Wait function and create Kafka instance", expected = "Resources and Kafka instance are successfully created"),
            @Step(value = "Retrieve broker certificates from Kubernetes secrets", expected = "Certificates are retrieved correctly from secrets"),
            @Step(value = "Validate that each broker's certificate contains the expected internal and external advertised hostnames", expected = "Certificates contain the correct advertised hostnames")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testAdvertisedHostNamesAppearsInBrokerCerts() throws CertificateException {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

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

        kubeResourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        kubeResourceManager.createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .editKafka()
                        .withListeners(asList(
                            new GenericKafkaListenerBuilder()
                                .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9098)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withBrokers(asList(brokerInternal0, brokerInternal1, brokerInternal2))
                                .endConfiguration()
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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

        List<String> brokerPods = kubeClient().listPodNamesInSpecificNamespace(testStorage.getNamespaceName(), Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND)
            .stream().filter(podName -> podName.contains("kafka")).collect(Collectors.toList());

        int index = 0;
        for (String kafkaBroker : brokerPods) {
            Map<String, String> secretData = kubeClient().getSecret(testStorage.getNamespaceName(), kafkaBroker).getData();
            String cert = secretData.get(kafkaBroker + ".crt");

            LOGGER.info("Encoding {}.crt", kafkaBroker);

            ByteArrayInputStream publicCert = new ByteArrayInputStream(Util.decodeBytesFromBase64(cert.getBytes()));
            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            Certificate certificate = certificateFactory.generateCertificate(publicCert);

            assertThat(certificate.toString(), containsString(advertHostInternalList.get(index)));
            assertThat(certificate.toString(), containsString(advertHostExternalList.get(index++)));
        }
    }

    @AfterEach
    void afterEach() {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, KubeResourceManager.get().getTestContext());
        kubeClient(namespaceName).getClient().persistentVolumeClaims().inNamespace(namespaceName).delete();
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator
            .get()
            .withDefaultConfiguration()
            .install();
    }
}
