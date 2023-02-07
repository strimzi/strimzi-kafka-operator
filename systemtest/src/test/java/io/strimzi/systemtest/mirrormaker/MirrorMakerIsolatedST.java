/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.mirrormaker;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaMirrorMakerStatus;
import io.strimzi.api.kafka.model.template.DeploymentStrategy;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.BeforeAllOnce;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.rmi.UnexpectedException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.COMPONENT_SCALING;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@Tag(MIRROR_MAKER)
@Tag(INTERNAL_CLIENTS_USED)
@IsolatedSuite
public class MirrorMakerIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MirrorMakerIsolatedST.class);

    @ParallelNamespaceTest
    void testMirrorMaker(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build(),
            KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build()
        );

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, testStorage.getTopicName()).build());

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        // Deploy Mirror Maker
        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), kafkaClusterSourceName, kafkaClusterTargetName, ClientUtils.generateRandomConsumerGroup(), 1, false)
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

        verifyLabelsOnPods(testStorage.getNamespaceName(), testStorage.getClusterName(), "mirror-maker", KafkaMirrorMaker.RESOURCE_KIND);

        verifyLabelsForConfigMaps(testStorage.getNamespaceName(), kafkaClusterSourceName, null, kafkaClusterTargetName);
        verifyLabelsForServiceAccounts(testStorage.getNamespaceName(), kafkaClusterSourceName, null);

        String mmDepName = KafkaMirrorMakerResources.deploymentName(testStorage.getClusterName());
        String mirrorMakerPodName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(mmDepName).get(0).getMetadata().getName();
        String kafkaMirrorMakerLogs = kubeClient(testStorage.getNamespaceName()).logs(mirrorMakerPodName);

        assertThat(kafkaMirrorMakerLogs,
            not(containsString("keytool error: java.io.FileNotFoundException: /opt/kafka/consumer-oauth-certs/**/* (No such file or directory)")));

        String podName = kubeClient(testStorage.getNamespaceName()).listPodsByNamespace(testStorage.getNamespaceName(), testStorage.getClusterName()).stream().filter(n -> n.getMetadata().getName().startsWith(KafkaMirrorMakerResources.deploymentName(testStorage.getClusterName()))).findFirst().orElseThrow().getMetadata().getName();
        assertResources(testStorage.getNamespaceName(), podName, mmDepName,
                "400M", "2", "300M", "1");
        assertExpectedJavaOpts(testStorage.getNamespaceName(), podName, KafkaMirrorMakerResources.deploymentName(testStorage.getClusterName()),
                "-Xmx200m", "-Xms200m", "-XX:+UseG1GC");

        clients = new KafkaClientsBuilder(clients)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();

        resourceManager.createResource(extensionContext, clients.consumerStrimzi());
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    /**
     * Test mirroring messages by Mirror Maker over tls transport using mutual tls auth
     */
    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testMirrorMakerTlsAuthenticated(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        String kafkaSourceUserName = testStorage.getUserName() + "-source";
        String kafkaTargetUserName = testStorage.getUserName() + "-target";

        // Deploy source kafka with tls listener and mutual tls auth
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        // Deploy target kafka with tls listener and mutual tls auth
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(kafkaClusterSourceName, testStorage.getTopicName()).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), kafkaClusterSourceName, kafkaSourceUserName).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), kafkaClusterTargetName, kafkaTargetUserName).build()
        );

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterSourceName));

        // Initialize CertSecretSource with certificate and secret names for producer
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterTargetName));

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterSourceName))
            .withNamespaceName(testStorage.getNamespaceName())
            .withUserName(kafkaSourceUserName)
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, clients.producerTlsStrimzi(kafkaClusterSourceName), clients.consumerTlsStrimzi(kafkaClusterSourceName));
        ClientUtils.waitForClientsSuccess(testStorage);

        // Deploy Mirror Maker with tls listener and mutual tls auth
        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), kafkaClusterSourceName, kafkaClusterTargetName, ClientUtils.generateRandomConsumerGroup(), 1, true)
            .editSpec()
                .editConsumer()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(kafkaSourceUserName)
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
                            .withSecretName(kafkaTargetUserName)
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                .endProducer()
            .endSpec()
            .build());

        clients = new KafkaClientsBuilder(clients)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterTargetName))
            .withUserName(kafkaTargetUserName)
            .build();

        resourceManager.createResource(extensionContext, clients.consumerTlsStrimzi(kafkaClusterTargetName));
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    /**
     * Test mirroring messages by Mirror Maker over tls transport using scram-sha auth
     */
    @ParallelNamespaceTest
    @KRaftNotSupported("Scram-sha is not supported by KRaft mode and is used in this test case")
    @SuppressWarnings("checkstyle:methodlength")
    void testMirrorMakerTlsScramSha(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        String kafkaSourceUserName = testStorage.getUserName() + "-source";
        String kafkaTargetUserName = testStorage.getUserName() + "-target";

        // Deploy source kafka with tls listener and SCRAM-SHA authentication
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        // Deploy target kafka with tls listener and SCRAM-SHA authentication
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build())
                .endKafka()
            .endSpec()
            .build());

        // Deploy topic
        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(kafkaClusterSourceName, testStorage.getTopicName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), kafkaClusterSourceName, kafkaSourceUserName).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), kafkaClusterTargetName, kafkaTargetUserName).build()
        );

        // Initialize PasswordSecretSource to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName(kafkaSourceUserName);
        passwordSecretSource.setPassword("password");

        // Initialize PasswordSecretSource to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecretTarget = new PasswordSecretSource();
        passwordSecretTarget.setSecretName(kafkaTargetUserName);
        passwordSecretTarget.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterSourceName));

        // Initialize CertSecretSource with certificate and secret names for producer
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterTargetName));

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterSourceName))
            .withNamespaceName(testStorage.getNamespaceName())
            .withUserName(kafkaSourceUserName)
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, clients.producerScramShaTlsStrimzi(kafkaClusterSourceName), clients.consumerScramShaTlsStrimzi(kafkaClusterSourceName));
        ClientUtils.waitForClientsSuccess(testStorage);

        // Deploy Mirror Maker with TLS and ScramSha512
        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), kafkaClusterSourceName, kafkaClusterTargetName, ClientUtils.generateRandomConsumerGroup(), 1, true)
            .editSpec()
                .editConsumer()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(kafkaSourceUserName)
                        .withPasswordSecret(passwordSecretSource)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                .endConsumer()
                .editProducer()
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(kafkaTargetUserName)
                        .withPasswordSecret(passwordSecretTarget)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                .endProducer()
            .endSpec()
            .build());

        clients = new KafkaClientsBuilder(clients)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterTargetName))
            .withUserName(kafkaTargetUserName)
            .build();

        resourceManager.createResource(extensionContext, clients.consumerScramShaTlsStrimzi(kafkaClusterTargetName));
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    void testIncludeList(ExtensionContext extensionContext) throws UnexpectedException {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        String topicName = "included-topic";
        String topicNotIncluded = "not-included-topic";

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build(),
            KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build()
        );

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(kafkaClusterSourceName, topicName).build(),
            KafkaTopicTemplates.topic(kafkaClusterSourceName, topicNotIncluded).build()
        );

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(topicName)
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(topicNotIncluded)
            .build();

        resourceManager.createResource(extensionContext, clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), kafkaClusterSourceName, kafkaClusterTargetName, ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .withInclude(topicName)
            .endSpec()
            .build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(topicName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();

        resourceManager.createResource(extensionContext, clients.consumerStrimzi());
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(topicNotIncluded)
            .build();

        LOGGER.info("Becuase {} is not included, we should not receive any message", topicNotIncluded);

        resourceManager.createResource(extensionContext, clients.consumerStrimzi());
        ClientUtils.waitForConsumerClientTimeout(testStorage);
    }

    @ParallelNamespaceTest
    void testCustomAndUpdatedValues(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .withNewEntityOperator()
                .endEntityOperator()
            .endSpec()
            .build());

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

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(clusterName, clusterName, clusterName, ClientUtils.generateRandomConsumerGroup(), 1, false)
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

        Map<String, String> mirrorMakerSnapshot = DeploymentUtils.depSnapshot(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName));

        // Remove variable which is already in use
        envVarGeneral.remove(usedVariable);
        LOGGER.info("Verify values before update");
        checkReadinessLivenessProbe(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), initialDelaySeconds, timeoutSeconds, periodSeconds,
            successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), envVarGeneral);
        checkComponentConfiguration(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), "KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER", producerConfig);
        checkComponentConfiguration(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), "KAFKA_MIRRORMAKER_CONFIGURATION_CONSUMER", consumerConfig);

        LOGGER.info("Check if actual env variable {} has different value than {}", usedVariable, "test.value");
        assertThat(StUtils.checkEnvVarInPod(namespaceName, kubeClient().listPods(namespaceName, clusterName, Labels.STRIMZI_KIND_LABEL,
            KafkaMirrorMaker.RESOURCE_KIND).get(0).getMetadata().getName(), usedVariable), CoreMatchers.is(not("test.value")));

        LOGGER.info("Updating values in MirrorMaker container");
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(clusterName, kmm -> {
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
        }, namespaceName);

        DeploymentUtils.waitTillDepHasRolled(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName), 1, mirrorMakerSnapshot);

        LOGGER.info("Verify values after update");
        checkReadinessLivenessProbe(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), envVarUpdated);
        checkComponentConfiguration(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), "KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER", updatedProducerConfig);
        checkComponentConfiguration(namespaceName, KafkaMirrorMakerResources.deploymentName(clusterName),
            KafkaMirrorMakerResources.deploymentName(clusterName), "KAFKA_MIRRORMAKER_CONFIGURATION_CONSUMER", updatedConsumerConfig);
    }

    @ParallelNamespaceTest
    @Tag(COMPONENT_SCALING)
    void testScaleMirrorMakerUpAndDownToZero(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, clusterOperator.getDeploymentNamespace());

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build(),
            KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build()
        );

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, ClientUtils.generateRandomConsumerGroup(), 1, false).build());

        int scaleTo = 2;
        long mmObsGen = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getObservedGeneration();
        String mmDepName = KafkaMirrorMakerResources.deploymentName(testStorage.getClusterName());
        String mmGenName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaMirrorMaker up <-------");

        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient().namespace(testStorage.getNamespaceName()).scaleByName(KafkaMirrorMaker.RESOURCE_KIND, testStorage.getClusterName(), scaleTo);
        DeploymentUtils.waitForDeploymentAndPodsReady(testStorage.getNamespaceName(), KafkaMirrorMakerResources.deploymentName(testStorage.getClusterName()), scaleTo);

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
    void testConfigureDeploymentStrategy(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";

        // Deploy source kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewDeployment()
                        .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                    .endDeployment()
                .endTemplate()
            .endSpec()
            .build());

        String mmDepName = KafkaMirrorMakerResources.deploymentName(clusterName);

        LOGGER.info("Adding label to MirrorMaker resource, the CR should be recreateAndWaitForReadinessd");
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(clusterName,
            mm -> mm.getMetadata().setLabels(Collections.singletonMap("some", "label")), namespaceName);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, mmDepName, 1);

        KafkaMirrorMaker kmm = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(namespaceName).withName(clusterName).get();

        LOGGER.info("Checking that observed gen. is still on 1 (recreation) and new label is present");
        assertThat(kmm.getStatus().getObservedGeneration(), is(1L));
        assertThat(kmm.getMetadata().getLabels().toString(), Matchers.containsString("some=label"));
        assertThat(kmm.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.RECREATE));

        LOGGER.info("Changing deployment strategy to {}", DeploymentStrategy.ROLLING_UPDATE);
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(clusterName,
            mm -> mm.getSpec().getTemplate().getDeployment().setDeploymentStrategy(DeploymentStrategy.ROLLING_UPDATE), namespaceName);
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(namespaceName, clusterName);

        LOGGER.info("Adding another label to MirrorMaker resource, pods should be rolled");
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(clusterName, mm -> mm.getMetadata().getLabels().put("another", "label"), namespaceName);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, mmDepName, 1);

        LOGGER.info("Checking that observed gen. higher (rolling update) and label is changed");
        StUtils.waitUntilSupplierIsSatisfied(
            () -> {
                final KafkaMirrorMaker kMM = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(namespaceName).withName(clusterName).get();

                return kMM.getStatus().getObservedGeneration() == 2L &&
                        kMM.getMetadata().getLabels().toString().contains("another=label") &&
                        kMM.getSpec().getTemplate().getDeployment().getDeploymentStrategy().equals(DeploymentStrategy.ROLLING_UPDATE);
            }
        );
    }

    @BeforeAll
    void setupEnvironment(ExtensionContext extensionContext) {
        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
            .withNamespace(Constants.INFRA_NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_MEDIUM)
            .createInstallation()
            .runInstallation();
    }
}
