/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.mirrormaker;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.PasswordSecretSource;
import io.strimzi.api.kafka.model.common.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMakerStatus;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.VerificationUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.COMPONENT_SCALING;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.MIRROR_MAKER;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@Tag(MIRROR_MAKER)
@Tag(INTERNAL_CLIENTS_USED)
public class MirrorMakerST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MirrorMakerST.class);

    @ParallelNamespaceTest
    void testMirrorMaker() {
        TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getSourceClusterName(), 1).build());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getTargetClusterName(), 1).build());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getSourceClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build());

        final KafkaClients sourceClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getSourceClusterName()));
        resourceManager.createResourceWithWait(sourceClients.producerStrimzi(), sourceClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // Deploy MirrorMaker
        resourceManager.createResourceWithWait(KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), testStorage.getSourceClusterName(), testStorage.getTargetClusterName(), ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editSpec()
            .withResources(new ResourceRequirementsBuilder()
                    .addToLimits("memory", new Quantity("400M"))
                    .addToLimits("cpu", new Quantity("2"))
                    .addToRequests("memory", new Quantity("300M"))
                    .addToRequests("cpu", new Quantity("1"))
                    .build())
            .withNewJvmOptions()
                .withXmx("200m")
                .withXms("200m")
                .withXx(jvmOptionsXX)
            .endJvmOptions()
            .endSpec()
            .build());

        VerificationUtils.verifyPodsLabels(testStorage.getNamespaceName(), KafkaMirrorMakerResources.componentName(testStorage.getClusterName()),
            KafkaMirrorMakerResource.getLabelSelector(testStorage.getClusterName(), KafkaMirrorMakerResources.componentName(testStorage.getClusterName())));

        VerificationUtils.verifyConfigMapsLabels(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), testStorage.getTargetClusterName());
        VerificationUtils.verifyServiceAccountsLabels(testStorage.getNamespaceName(), testStorage.getSourceClusterName());

        String mmDepName = KafkaMirrorMakerResources.componentName(testStorage.getClusterName());
        String mirrorMakerPodName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(mmDepName).get(0).getMetadata().getName();
        String kafkaMirrorMakerLogs = kubeClient(testStorage.getNamespaceName()).logs(mirrorMakerPodName);

        assertThat(kafkaMirrorMakerLogs,
            not(containsString("keytool error: java.io.FileNotFoundException: /opt/kafka/consumer-oauth-certs/**/* (No such file or directory)")));

        String podName = kubeClient(testStorage.getNamespaceName()).listPodsByNamespace(testStorage.getNamespaceName(), testStorage.getClusterName()).stream().filter(n -> n.getMetadata().getName().startsWith(KafkaMirrorMakerResources.componentName(testStorage.getClusterName()))).findFirst().orElseThrow().getMetadata().getName();
        VerificationUtils.assertPodResourceRequests(testStorage.getNamespaceName(), podName, mmDepName,
                "400M", "2", "300M", "1");
        VerificationUtils.assertJvmOptions(testStorage.getNamespaceName(), podName, KafkaMirrorMakerResources.componentName(testStorage.getClusterName()),
                "-Xmx200m", "-Xms200m", "-XX:+UseG1GC");

        final KafkaClients targetClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()));
        resourceManager.createResourceWithWait(targetClients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    /**
     * Test mirroring messages by MirrorMaker over tls transport using mutual tls auth
     */
    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testMirrorMakerTlsAuthenticated() {
        TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
            )
        );

        // Deploy source kafka with tls listener and mutual tls auth
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getSourceClusterName(), 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
            )
        );

        // Deploy target kafka with tls listener and mutual tls auth
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getTargetClusterName(), 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage.getSourceClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), testStorage.getSourceUsername()).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), testStorage.getTargetUsername()).build()
        );

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getSourceClusterName()));

        // Initialize CertSecretSource with certificate and secret names for producer
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getTargetClusterName()));

        final KafkaClients sourceClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getSourceClusterName()))
            .withUsername(testStorage.getSourceUsername())
            .build();
        resourceManager.createResourceWithWait(sourceClients.producerTlsStrimzi(testStorage.getSourceClusterName()), sourceClients.consumerTlsStrimzi(testStorage.getSourceClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // Deploy MirrorMaker with tls listener and mutual tls auth
        resourceManager.createResourceWithWait(KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), testStorage.getSourceClusterName(), testStorage.getTargetClusterName(), ClientUtils.generateRandomConsumerGroup(), 1, true)
            .editSpec()
                .editConsumer()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(testStorage.getSourceUsername())
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                .endConsumer()
                .editProducer()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(testStorage.getTargetUsername())
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                .endProducer()
            .endSpec()
            .build());

        final KafkaClients targetClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getTargetClusterName()))
            .withUsername(testStorage.getTargetUsername())
            .build();
        resourceManager.createResourceWithWait(targetClients.consumerTlsStrimzi(testStorage.getTargetClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    /**
     * Test mirroring messages by MirrorMaker over tls transport using scram-sha auth
     */
    @ParallelNamespaceTest
    @SuppressWarnings("checkstyle:methodlength")
    void testMirrorMakerTlsScramSha() {
        TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
            )
        );

        // Deploy source kafka with tls listener and SCRAM-SHA authentication
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getSourceClusterName(), 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
            )
        );

        // Deploy target kafka with tls listener and SCRAM-SHA authentication
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getTargetClusterName(), 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        // Deploy topic
        resourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage.getSourceClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), testStorage.getSourceUsername()).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), testStorage.getTargetUsername()).build()
        );

        // Initialize PasswordSecretSource to set this as PasswordSecret in MirrorMaker spec
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName(testStorage.getSourceUsername());
        passwordSecretSource.setPassword("password");

        // Initialize PasswordSecretSource to set this as PasswordSecret in MirrorMaker spec
        PasswordSecretSource passwordSecretTarget = new PasswordSecretSource();
        passwordSecretTarget.setSecretName(testStorage.getTargetUsername());
        passwordSecretTarget.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getSourceClusterName()));

        // Initialize CertSecretSource with certificate and secret names for producer
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(testStorage.getTargetClusterName()));

        final KafkaClients sourceClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getSourceClusterName()))
            .withUsername(testStorage.getSourceUsername())
            .withTopicName(testStorage.getTopicName())
            .build();
        resourceManager.createResourceWithWait(sourceClients.producerScramShaTlsStrimzi(testStorage.getSourceClusterName()), sourceClients.consumerScramShaTlsStrimzi(testStorage.getSourceClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // Deploy MirrorMaker with TLS and ScramSha512
        resourceManager.createResourceWithWait(KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), testStorage.getSourceClusterName(), testStorage.getTargetClusterName(), ClientUtils.generateRandomConsumerGroup(), 1, true)
            .editSpec()
                .editConsumer()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(testStorage.getSourceUsername())
                        .withPasswordSecret(passwordSecretSource)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                .endConsumer()
                .editProducer()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(testStorage.getTargetUsername())
                        .withPasswordSecret(passwordSecretTarget)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                .endProducer()
            .endSpec()
            .build());

        final KafkaClients targetClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(testStorage.getTargetClusterName()))
            .withUsername(testStorage.getTargetUsername())
            .build();
        resourceManager.createResourceWithWait(targetClients.consumerScramShaTlsStrimzi(testStorage.getTargetClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    void testIncludeList() {
        TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        String topicName = "included-topic";
        String topicNotIncluded = "not-included-topic";

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getSourceClusterName(), 1).build());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getTargetClusterName(), 1).build());


        resourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage.getSourceClusterName(), topicName, testStorage.getNamespaceName()).build(),
            KafkaTopicTemplates.topic(testStorage.getSourceClusterName(), topicNotIncluded, testStorage.getNamespaceName()).build()
        );

        KafkaClients sourceClients = ClientUtils.getInstantPlainClientBuilder(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getSourceClusterName()))
            .withTopicName(topicName)
            .build();

        resourceManager.createResourceWithWait(sourceClients.producerStrimzi(), sourceClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        sourceClients = new KafkaClientsBuilder(sourceClients)
            .withTopicName(topicNotIncluded)
            .build();

        resourceManager.createResourceWithWait(sourceClients.producerStrimzi(), sourceClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        resourceManager.createResourceWithWait(KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), testStorage.getSourceClusterName(), testStorage.getTargetClusterName(), ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .withInclude(topicName)
            .endSpec()
            .build());

        KafkaClients targetClients = ClientUtils.getInstantPlainClientBuilder(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getTargetClusterName()))
            .withTopicName(topicName)
            .build();

        resourceManager.createResourceWithWait(targetClients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        targetClients = new KafkaClientsBuilder(targetClients)
            .withTopicName(topicNotIncluded)
            .build();

        LOGGER.info("Becuase {} is not included, we should not receive any message", topicNotIncluded);
        resourceManager.createResourceWithWait(targetClients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientTimeout(testStorage);
    }

    @ParallelNamespaceTest
    void testCustomAndUpdatedValues() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1, 1).build());

        String usedVariable = "KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER";

        LinkedHashMap<String, String> envVarGeneral = new LinkedHashMap<>();
        envVarGeneral.put("TEST_ENV_1", "test.env.one");
        envVarGeneral.put("TEST_ENV_2", "test.env.two");
        envVarGeneral.put(usedVariable, "test.value");

        LinkedHashMap<String, String> envVarUpdated = new LinkedHashMap<>();
        envVarUpdated.put("TEST_ENV_2", "updated.test.env.two");
        envVarUpdated.put("TEST_ENV_3", "test.env.three");

        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put("acks", "all");

        Map<String, Object> updatedProducerConfig = new HashMap<>();
        updatedProducerConfig.put("acks", "0");

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("auto.offset.reset", "latest");

        Map<String, Object> updatedConsumerConfig = new HashMap<>();
        updatedConsumerConfig.put("auto.offset.reset", "earliest");

        int initialDelaySeconds = 30;
        int timeoutSeconds = 10;
        int updatedInitialDelaySeconds = 31;
        int updatedTimeoutSeconds = 11;
        int periodSeconds = 10;
        int successThreshold = 1;
        int failureThreshold = 3;
        int updatedPeriodSeconds = 5;
        int updatedFailureThreshold = 1;

        resourceManager.createResourceWithWait(KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), testStorage.getClusterName(), testStorage.getClusterName(), ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editSpec()
                .editProducer()
                    .withConfig(producerConfig)
                .endProducer()
                .editConsumer()
                    .withConfig(consumerConfig)
                .endConsumer()
                .withNewTemplate()
                    .withNewMirrorMakerContainer()
                        .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                    .endMirrorMakerContainer()
                .endTemplate()
                .withNewReadinessProbe()
                    .withInitialDelaySeconds(initialDelaySeconds)
                    .withTimeoutSeconds(timeoutSeconds)
                    .withPeriodSeconds(periodSeconds)
                    .withSuccessThreshold(successThreshold)
                    .withFailureThreshold(failureThreshold)
                .endReadinessProbe()
                .withNewLivenessProbe()
                    .withInitialDelaySeconds(initialDelaySeconds)
                    .withTimeoutSeconds(timeoutSeconds)
                    .withPeriodSeconds(periodSeconds)
                    .withSuccessThreshold(successThreshold)
                    .withFailureThreshold(failureThreshold)
                .endLivenessProbe()
            .endSpec()
            .build());

        Map<String, String> mirrorMakerSnapshot = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaMirrorMakerResources.componentName(testStorage.getClusterName()));

        // Remove variable which is already in use
        envVarGeneral.remove(usedVariable);
        LOGGER.info("Verifying values before update");
        VerificationUtils.verifyReadinessAndLivenessProbes(testStorage.getNamespaceName(), KafkaMirrorMakerResources.componentName(testStorage.getClusterName()),
            KafkaMirrorMakerResources.componentName(testStorage.getClusterName()), initialDelaySeconds, timeoutSeconds, periodSeconds,
            successThreshold, failureThreshold);
        VerificationUtils.verifyContainerEnvVariables(testStorage.getNamespaceName(), KafkaMirrorMakerResources.componentName(testStorage.getClusterName()),
            KafkaMirrorMakerResources.componentName(testStorage.getClusterName()), envVarGeneral);
        VerificationUtils.verifyComponentConfiguration(testStorage.getNamespaceName(), KafkaMirrorMakerResources.componentName(testStorage.getClusterName()),
            KafkaMirrorMakerResources.componentName(testStorage.getClusterName()), "KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER", producerConfig);
        VerificationUtils.verifyComponentConfiguration(testStorage.getNamespaceName(), KafkaMirrorMakerResources.componentName(testStorage.getClusterName()),
            KafkaMirrorMakerResources.componentName(testStorage.getClusterName()), "KAFKA_MIRRORMAKER_CONFIGURATION_CONSUMER", consumerConfig);

        LOGGER.info("Check if actual env variable {} has different value than {}", usedVariable, "test.value");
        assertThat(StUtils.checkEnvVarInPod(testStorage.getNamespaceName(), kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL,
            KafkaMirrorMaker.RESOURCE_KIND).get(0).getMetadata().getName(), usedVariable), CoreMatchers.is(not("test.value")));

        LOGGER.info("Updating values in MirrorMaker container");
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(testStorage.getClusterName(), kmm -> {
            kmm.getSpec().getTemplate().getMirrorMakerContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            kmm.getSpec().getProducer().setConfig(updatedProducerConfig);
            kmm.getSpec().getConsumer().setConfig(updatedConsumerConfig);
            kmm.getSpec().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kmm.getSpec().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kmm.getSpec().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kmm.getSpec().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kmm.getSpec().getLivenessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kmm.getSpec().getReadinessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kmm.getSpec().getLivenessProbe().setFailureThreshold(updatedFailureThreshold);
            kmm.getSpec().getReadinessProbe().setFailureThreshold(updatedFailureThreshold);
        }, testStorage.getNamespaceName());

        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), KafkaMirrorMakerResources.componentName(testStorage.getClusterName()), 1, mirrorMakerSnapshot);

        LOGGER.info("Verifying values after update");
        VerificationUtils.verifyReadinessAndLivenessProbes(testStorage.getNamespaceName(), KafkaMirrorMakerResources.componentName(testStorage.getClusterName()),
            KafkaMirrorMakerResources.componentName(testStorage.getClusterName()), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        VerificationUtils.verifyContainerEnvVariables(testStorage.getNamespaceName(), KafkaMirrorMakerResources.componentName(testStorage.getClusterName()),
            KafkaMirrorMakerResources.componentName(testStorage.getClusterName()), envVarUpdated);
        VerificationUtils.verifyComponentConfiguration(testStorage.getNamespaceName(), KafkaMirrorMakerResources.componentName(testStorage.getClusterName()),
            KafkaMirrorMakerResources.componentName(testStorage.getClusterName()), "KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER", updatedProducerConfig);
        VerificationUtils.verifyComponentConfiguration(testStorage.getNamespaceName(), KafkaMirrorMakerResources.componentName(testStorage.getClusterName()),
            KafkaMirrorMakerResources.componentName(testStorage.getClusterName()), "KAFKA_MIRRORMAKER_CONFIGURATION_CONSUMER", updatedConsumerConfig);
    }

    @ParallelNamespaceTest
    @Tag(COMPONENT_SCALING)
    void testScaleMirrorMakerUpAndDownToZero() {
        TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getSourceClusterName(), 1).build());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getTargetClusterName(), 1).build());

        resourceManager.createResourceWithWait(KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), testStorage.getTargetClusterName(), testStorage.getSourceClusterName(), ClientUtils.generateRandomConsumerGroup(), 1, false).build());

        int scaleTo = 2;
        long mmObsGen = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getObservedGeneration();
        String mmDepName = KafkaMirrorMakerResources.componentName(testStorage.getClusterName());
        String mmGenName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaMirrorMaker up <-------");

        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient().namespace(testStorage.getNamespaceName()).scaleByName(KafkaMirrorMaker.RESOURCE_KIND, testStorage.getClusterName(), scaleTo);
        DeploymentUtils.waitForDeploymentAndPodsReady(testStorage.getNamespaceName(), KafkaMirrorMakerResources.componentName(testStorage.getClusterName()), scaleTo);

        LOGGER.info("Check if replicas is set to {}, naming prefix should be same and observed generation higher", scaleTo);

        StUtils.waitUntilSupplierIsSatisfied(() -> kubeClient().listPodNames(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND).size() == scaleTo &&
            KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getReplicas() == scaleTo &&
            KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getReplicas() == scaleTo);

        /*
        observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
        the observed generation is increased
        */
        long actualObsGen = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getObservedGeneration();
        assertTrue(mmObsGen < actualObsGen);

        List<String> mmPods = kubeClient().listPodNames(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND);

        for (String pod : mmPods) {
            assertTrue(pod.contains(mmGenName));
        }

        mmObsGen = actualObsGen;

        LOGGER.info("-------> Scaling KafkaMirrorMaker down <-------");

        LOGGER.info("Scaling MirrorMaker to zero");
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(testStorage.getClusterName(), mm -> mm.getSpec().setReplicas(0), testStorage.getNamespaceName());

        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), kubeClient().getDeploymentSelectors(testStorage.getNamespaceName(), mmDepName), 0, true);

        mmPods = kubeClient().listPodNames(testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND);
        KafkaMirrorMakerStatus mmStatus = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        actualObsGen = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getObservedGeneration();

        assertThat(mmPods.size(), is(0));
        assertThat(mmStatus.getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(actualObsGen, is(not(mmObsGen)));

    }

    @ParallelNamespaceTest
    void testConfigureDeploymentStrategy() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getSourceClusterName(), 1).build());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getTargetClusterName(), 1).build());

        resourceManager.createResourceWithWait(KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), testStorage.getTargetClusterName(), testStorage.getSourceClusterName(), ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewDeployment()
                        .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                    .endDeployment()
                .endTemplate()
            .endSpec()
            .build());

        String mmDepName = KafkaMirrorMakerResources.componentName(testStorage.getClusterName());

        LOGGER.info("Adding label to MirrorMaker resource, the CR should be recreateAndWaitForReadinessd");
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(testStorage.getClusterName(),
            mm -> mm.getMetadata().setLabels(Collections.singletonMap("some", "label")), testStorage.getNamespaceName());
        DeploymentUtils.waitForDeploymentAndPodsReady(testStorage.getNamespaceName(), mmDepName, 1);

        KafkaMirrorMaker kmm = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get();

        LOGGER.info("Checking that observed gen. is still on 1 (recreation) and new label is present");
        assertThat(kmm.getStatus().getObservedGeneration(), is(1L));
        assertThat(kmm.getMetadata().getLabels().toString(), Matchers.containsString("some=label"));
        assertThat(kmm.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.RECREATE));

        LOGGER.info("Changing Deployment strategy to {}", DeploymentStrategy.ROLLING_UPDATE);
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(testStorage.getClusterName(),
            mm -> mm.getSpec().getTemplate().getDeployment().setDeploymentStrategy(DeploymentStrategy.ROLLING_UPDATE), testStorage.getNamespaceName());
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Adding another label to MirrorMaker resource, Pods should be rolled");
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(testStorage.getClusterName(), mm -> mm.getMetadata().getLabels().put("another", "label"), testStorage.getNamespaceName());
        DeploymentUtils.waitForDeploymentAndPodsReady(testStorage.getNamespaceName(), mmDepName, 1);

        LOGGER.info("Checking that observed gen. higher (rolling update) and label is changed");
        StUtils.waitUntilSupplierIsSatisfied(
            () -> {
                final KafkaMirrorMaker kMM = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get();

                return kMM.getStatus().getObservedGeneration() == 2L &&
                        kMM.getMetadata().getLabels().toString().contains("another=label") &&
                        kMM.getSpec().getTemplate().getDeployment().getDeploymentStrategy().equals(DeploymentStrategy.ROLLING_UPDATE);
            }
        );
    }

    @BeforeAll
    void setupEnvironment() {
        clusterOperator = clusterOperator.defaultInstallation()
            .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_MEDIUM)
            .createInstallation()
            .runInstallation();
    }
}
