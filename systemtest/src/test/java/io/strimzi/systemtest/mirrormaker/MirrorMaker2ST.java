/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.mirrormaker;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.template.DeploymentStrategy;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.KRaftWithoutUTONotSupported;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.COMPONENT_SCALING;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
@Tag(MIRROR_MAKER2)
@Tag(CONNECT_COMPONENTS)
@Tag(INTERNAL_CLIENTS_USED)
class MirrorMaker2ST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MirrorMaker2ST.class);

    @ParallelNamespaceTest
    void testMirrorMaker2(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        String sourceMirroredTopicName = kafkaClusterSourceName + "." + testStorage.getTopicName();

        Map<String, Object> expectedConfig = StUtils.loadProperties("group.id=mirrormaker2-cluster\n" +
                "key.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "value.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "header.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "config.storage.topic=mirrormaker2-cluster-configs\n" +
                "status.storage.topic=mirrormaker2-cluster-status\n" +
                "offset.storage.topic=mirrormaker2-cluster-offsets\n" +
                "config.storage.replication.factor=-1\n" +
                "status.storage.replication.factor=-1\n" +
                "offset.storage.replication.factor=-1\n" +
                "config.providers=file\n" + 
                "config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider\n");

        // Deploy source and target kafka
        resourceManager.createResourceWithWait(extensionContext,
            KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1).build(),
            KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1).build()
        );

        // Deploy source topic
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, testStorage.getTopicName(), 3, testStorage.getNamespaceName()).build());

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        // Check brokers availability
        LOGGER.info("Messages exchange - Topic: {}, cluster: {} and message count of {}",
            testStorage.getTopicName(), kafkaClusterSourceName, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(extensionContext, clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
                .editFirstMirror()
                    .editSourceConnector()
                        .addToConfig("refresh.topics.interval.seconds", "1")
                    .endSourceConnector()
                .endMirror()
            .endSpec()
            .build());

        String podName = PodUtils.getPodNameByPrefix(testStorage.getNamespaceName(), KafkaMirrorMaker2Resources.deploymentName(testStorage.getClusterName()));
        String kafkaPodJson = TestUtils.toJsonString(kubeClient().getPod(testStorage.getNamespaceName(), podName));

        assertThat(kafkaPodJson, hasJsonPath(StUtils.globalVariableJsonPathBuilder(0, "KAFKA_CONNECT_BOOTSTRAP_SERVERS"),
                hasItem(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))));
        assertThat(StUtils.getPropertiesFromJson(0, kafkaPodJson, "KAFKA_CONNECT_CONFIGURATION"), is(expectedConfig));
        testDockerImagesForKafkaMirrorMaker2(testStorage.getClusterName(), clusterOperator.getDeploymentNamespace(), testStorage.getNamespaceName());

        verifyLabelsOnPods(testStorage.getNamespaceName(), testStorage.getClusterName(), "mirrormaker2", KafkaMirrorMaker2.RESOURCE_KIND);
        verifyLabelsForService(testStorage.getNamespaceName(), testStorage.getClusterName(), "mirrormaker2", "mirrormaker2-api", KafkaMirrorMaker2.RESOURCE_KIND);
        verifyLabelsForConfigMaps(testStorage.getNamespaceName(), kafkaClusterSourceName, null, kafkaClusterTargetName);
        verifyLabelsForServiceAccounts(testStorage.getNamespaceName(), kafkaClusterSourceName, null);

        LOGGER.info("Now setting Topic to {} and cluster to {} - the messages should be mirrored",
            sourceMirroredTopicName, kafkaClusterTargetName);

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(sourceMirroredTopicName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();

        LOGGER.info("Consumer in target cluster and Topic should receive {} messages", testStorage.getMessageCount());

        resourceManager.createResourceWithWait(extensionContext, clients.consumerStrimzi());
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        LOGGER.info("Mirrored successful");

        if (!Environment.isKRaftModeEnabled()) {
            KafkaTopic mirroredTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(testStorage.getNamespaceName()).withName(sourceMirroredTopicName).get();
            assertThat(mirroredTopic.getSpec().getPartitions(), is(3));
            assertThat(mirroredTopic.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(kafkaClusterTargetName));

            // Replace source topic resource with new data and check that mm2 update target topi
            KafkaTopicResource.replaceTopicResourceInSpecificNamespace(testStorage.getTopicName(), kt -> kt.getSpec().setPartitions(8), testStorage.getNamespaceName());
            KafkaTopicUtils.waitForKafkaTopicPartitionChange(testStorage.getNamespaceName(), sourceMirroredTopicName, 8);
        }
    }

    /**
     * Test mirroring messages by MirrorMaker 2 over tls transport using mutual tls auth
     */
    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    void testMirrorMaker2TlsAndTlsClientAuth(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        String sourceMirroredTopicName = kafkaClusterSourceName + "." + testStorage.getTopicName();

        String kafkaUserSourceName = testStorage.getUsername() + "-source";
        String kafkaUserTargetName = testStorage.getUsername() + "-target";

        // Deploy source kafka with tls listener and mutual tls auth
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1)
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
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1)
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

        // Deploy topic
        resourceManager.createResourceWithWait(extensionContext,
            KafkaTopicTemplates.topic(kafkaClusterSourceName, testStorage.getTopicName(), 3, testStorage.getNamespaceName()).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), kafkaClusterSourceName, kafkaUserSourceName).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), kafkaClusterTargetName, kafkaUserTargetName).build()
        );

        // Initialize CertSecretSource with certificate and secret names for source
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterSourceName));

        // Initialize CertSecretSource with certificate and secret names for target
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterTargetName));

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterSourceName))
            .withUsername(kafkaUserSourceName)
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        // Check brokers availability
        LOGGER.info("Messages exchange - Topic: {}, cluster: {} and message count of {}",
            testStorage.getTopicName(), kafkaClusterSourceName, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(kafkaClusterSourceName), clients.consumerTlsStrimzi(kafkaClusterSourceName));
        ClientUtils.waitForClientsSuccess(testStorage);

        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, true)
            .editSpec()
                .editMatchingCluster(spec -> spec.getAlias().equals(kafkaClusterSourceName))
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(kafkaUserSourceName)
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                .endCluster()
                .editMatchingCluster(spec -> spec.getAlias().equals(kafkaClusterTargetName))
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(kafkaUserTargetName)
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                .endCluster()
            .endSpec()
            .build());


        LOGGER.info("Changing to mirrored Topic - Topic: {}, cluster: {}, user: {}", sourceMirroredTopicName, kafkaClusterTargetName, kafkaClusterTargetName);

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(sourceMirroredTopicName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterTargetName))
            .withUsername(kafkaUserTargetName)
            .build();

        LOGGER.info("Now messages should be mirrored to target Topic and cluster");
        resourceManager.createResourceWithWait(extensionContext, clients.consumerTlsStrimzi(kafkaClusterTargetName));
        ClientUtils.waitForConsumerClientSuccess(testStorage);
        LOGGER.info("Messages successfully mirrored");

        if (!Environment.isKRaftModeEnabled()) {
            KafkaTopicUtils.waitForKafkaTopicCreation(testStorage.getNamespaceName(), sourceMirroredTopicName);
            KafkaTopic mirroredTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(testStorage.getNamespaceName()).withName(sourceMirroredTopicName).get();

            assertThat(mirroredTopic.getSpec().getPartitions(), is(3));
            assertThat(mirroredTopic.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(kafkaClusterTargetName));
        }
    }

    /**
     * Test mirroring messages by MirrorMaker 2 over tls transport using scram-sha-512 auth
     */
    @ParallelNamespaceTest
    @KRaftWithoutUTONotSupported
    void testMirrorMaker2TlsAndScramSha512Auth(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        String sourceMirroredTopicName = kafkaClusterSourceName + "." + testStorage.getTopicName();

        String kafkaUserSourceName = testStorage.getUsername() + "-source";
        String kafkaUserTargetName = testStorage.getUsername() + "-target";

        resourceManager.createResourceWithWait(extensionContext,
            KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1)
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
            .build(),
            KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1)
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
                .build()
        );

        resourceManager.createResourceWithWait(extensionContext,
            KafkaTopicTemplates.topic(kafkaClusterSourceName, testStorage.getTopicName(), 3, testStorage.getNamespaceName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), kafkaClusterSourceName, kafkaUserSourceName).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), kafkaClusterTargetName, kafkaUserTargetName).build()
        );

        // Initialize PasswordSecretSource to set this as PasswordSecret in MirrorMaker2 spec
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName(kafkaUserSourceName);
        passwordSecretSource.setPassword("password");

        // Initialize PasswordSecretSource to set this as PasswordSecret in MirrorMaker2 spec
        PasswordSecretSource passwordSecretTarget = new PasswordSecretSource();
        passwordSecretTarget.setSecretName(kafkaUserTargetName);
        passwordSecretTarget.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for source
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterSourceName));

        // Initialize CertSecretSource with certificate and secret names for target
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterTargetName));

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterSourceName))
            .withUsername(kafkaUserSourceName)
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        // Check brokers availability
        LOGGER.info("Messages exchange - Topic: {}, cluster: {} and message count of {}",
            testStorage.getTopicName(), kafkaClusterSourceName, testStorage.getMessageCount());

        resourceManager.createResourceWithWait(extensionContext, clients.producerScramShaTlsStrimzi(kafkaClusterSourceName), clients.consumerScramShaTlsStrimzi(kafkaClusterSourceName));
        ClientUtils.waitForClientsSuccess(testStorage);

        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, true)
            .editSpec()
                .editMatchingCluster(spec -> spec.getAlias().equals(kafkaClusterSourceName))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(kafkaUserSourceName)
                        .withPasswordSecret(passwordSecretSource)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                .endCluster()
                .editMatchingCluster(spec -> spec.getAlias().equals(kafkaClusterTargetName))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(kafkaUserTargetName)
                        .withPasswordSecret(passwordSecretTarget)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                .endCluster()
            .endSpec()
            .build());

        LOGGER.info("Changing to mirrored Topic - Topic: {}, cluster: {}, user: {}", sourceMirroredTopicName, kafkaClusterTargetName, kafkaClusterTargetName);

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(sourceMirroredTopicName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterTargetName))
            .withUsername(kafkaUserTargetName)
            .build();

        LOGGER.info("Now messages should be mirrored to target Topic and cluster");
        resourceManager.createResourceWithWait(extensionContext, clients.consumerScramShaTlsStrimzi(kafkaClusterTargetName));
        ClientUtils.waitForConsumerClientSuccess(testStorage);
        LOGGER.info("Messages successfully mirrored");

        // https://github.com/strimzi/strimzi-kafka-operator/issues/8864
        if (!Environment.isUnidirectionalTopicOperatorEnabled()) {
            KafkaTopicUtils.waitForKafkaTopicCreation(testStorage.getNamespaceName(), sourceMirroredTopicName);
            KafkaTopic mirroredTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(testStorage.getNamespaceName()).withName(sourceMirroredTopicName).get();

            assertThat(mirroredTopic.getSpec().getPartitions(), is(3));
            assertThat(mirroredTopic.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(kafkaClusterTargetName));
        }
    }

    @ParallelNamespaceTest
    @Tag(COMPONENT_SCALING)
    void testScaleMirrorMaker2UpAndDownToZero(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        String kafkaClusterSourceName = testStorage.getClusterName();
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        LabelSelector mmSelector = KafkaMirrorMaker2Resource.getLabelSelector(testStorage.getClusterName(), KafkaMirrorMaker2Resources.deploymentName(testStorage.getClusterName()));

        // Deploy source kafka
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, false).build());

        int scaleTo = 2;
        long mm2ObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getObservedGeneration();
        String mm2GenName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaMirrorMaker2 up <-------");

        LOGGER.info("Scaling subresource replicas to {}", scaleTo);

        cmdKubeClient(testStorage.getNamespaceName()).scaleByName(KafkaMirrorMaker2.RESOURCE_KIND, testStorage.getClusterName(), scaleTo);
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), mmSelector, scaleTo);

        LOGGER.info("Check if replicas is set to {}, naming prefix should be same and observed generation higher", scaleTo);
        List<String> mm2Pods = kubeClient(testStorage.getNamespaceName()).listPodNames(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND);

        StUtils.waitUntilSupplierIsSatisfied(() -> kubeClient(testStorage.getNamespaceName()).listPodNames(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).size() == scaleTo &&
                KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getSpec().getReplicas() == scaleTo &&
                KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getReplicas() == scaleTo);

        /*
        observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
        the observed generation is increased
        */
        long actualObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getMetadata().getGeneration();

        assertTrue(mm2ObsGen < actualObsGen);
        // StrimziPodSet does not have .metadata.generateName attribute and thus we ignoring such assert here
        if (!Environment.isStableConnectIdentitiesEnabled()) {
            for (String pod : mm2Pods) {
                assertTrue(pod.contains(mm2GenName));
            }
        }

        mm2ObsGen = actualObsGen;

        LOGGER.info("-------> Scaling KafkaMirrorMaker2 down <-------");

        LOGGER.info("Scaling MirrorMaker2 to zero");
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(testStorage.getClusterName(), mm2 -> mm2.getSpec().setReplicas(0), testStorage.getNamespaceName());

        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), mmSelector, 0, true, () -> { });

        mm2Pods = kubeClient().listPodNames(testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND);
        KafkaMirrorMaker2Status mm2Status = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        actualObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getMetadata().getGeneration();

        assertThat(mm2Pods.size(), is(0));
        assertThat(mm2Status.getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(actualObsGen, is(not(mm2ObsGen)));

        TestUtils.waitFor("Until MirrorMaker2 status url is null", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            KafkaMirrorMaker2Status mm2StatusUrl = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
            return mm2StatusUrl.getUrl() == null;
        });
    }

    @ParallelNamespaceTest
    void testMirrorMaker2CorrectlyMirrorsHeaders(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        String sourceMirroredTopicName = kafkaClusterSourceName + "." + testStorage.getTopicName();

        // Deploy source kafka
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());
        // Deploy Topic for example clients
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, testStorage.getTopicName(), testStorage.getNamespaceName()).build());

        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
                .editSpec()
                    .editMirror(0)
                        .editSourceConnector()
                            .addToConfig("refresh.topics.interval.seconds", "1")
                        .endSourceConnector()
                    .endMirror()
                .endSpec().build());

        // Deploying example clients for checking if mm2 will mirror messages with headers

        KafkaClients targetKafkaClientsJob = new KafkaClientsBuilder()
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .withTopicName(sourceMirroredTopicName)
            .withMessageCount(testStorage.getMessageCount())
            .withDelayMs(1000)
            .build();

        resourceManager.createResourceWithWait(extensionContext, targetKafkaClientsJob.consumerStrimzi());

        KafkaClients sourceKafkaClientsJob = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withHeaders("header_key_one=header_value_one, header_key_two=header_value_two")
            .withDelayMs(1000)
            .build();

        resourceManager.createResourceWithWait(extensionContext, sourceKafkaClientsJob.producerStrimzi());

        ClientUtils.waitForClientsSuccess(testStorage, false);

        LOGGER.info("Checking log of {} Job if the headers are correct", testStorage.getConsumerName());
        String header1 = "key: header_key_one, value: header_value_one";
        String header2 = "key: header_key_two, value: header_value_two";
        String log = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(testStorage.getConsumerName()).get(0).getMetadata().getName(), "", testStorage.getMessageCount() + "s");
        assertThat(log, containsString(header1));
        assertThat(log, containsString(header2));
    }

    /*
     * This test is using the Kafka Identity Replication policy. This is what should be used by all new users.
     */
    @ParallelNamespaceTest
    void testIdentityReplicationPolicy(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        resourceManager.createResourceWithWait(extensionContext,
            KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build(),
            KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        final String scraperPodName =  kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        // Create topic
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, testStorage.getTopicName(), 3, testStorage.getNamespaceName()).build());

        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
                .editMirror(0)
                    .editSourceConnector()
                        .addToConfig("replication.policy.class", "org.apache.kafka.connect.mirror.IdentityReplicationPolicy")
                        .addToConfig("refresh.topics.interval.seconds", "1")
                    .endSourceConnector()
                .endMirror()
            .endSpec()
            .build());

        LOGGER.info("Sending and receiving messages via {}", kafkaClusterSourceName);

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Changing to {} and will try to receive messages", kafkaClusterTargetName);

        clients = new KafkaClientsBuilder(clients)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.consumerStrimzi());
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        if (!Environment.isKRaftModeEnabled()) {
            LOGGER.info("Checking if the mirrored Topic name is same as the original one");

            List<String> kafkaTopics = KafkaCmdClient.listTopicsUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));
            assertNotNull(kafkaTopics.stream().filter(kafkaTopic -> kafkaTopic.equals(testStorage.getTopicName())).findAny());

            String kafkaTopicSpec = KafkaCmdClient.describeTopicUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(kafkaClusterTargetName), testStorage.getTopicName());
            assertThat(kafkaTopicSpec, containsString("Topic: " + testStorage.getTopicName()));
            assertThat(kafkaTopicSpec, containsString("PartitionCount: 3"));
        }
    }

    /*
     * This test is using the Strimzi Identity Replication policy. This is needed for backwards compatibility for users
     * who might still have it configured.
     *
     * This ST should be deleted once we drop the Strimzi policy completely.
     */
    @ParallelNamespaceTest
    void testStrimziIdentityReplicationPolicy(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        resourceManager.createResourceWithWait(extensionContext,
            KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build(),
            KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        final String scraperPodName =  kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        // Create topic
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(kafkaClusterSourceName, testStorage.getTopicName(), 3, testStorage.getNamespaceName()).build());

        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
                .editMirror(0)
                    .editSourceConnector()
                        .addToConfig("replication.policy.class", "io.strimzi.kafka.connect.mirror.IdentityReplicationPolicy")
                        .addToConfig("refresh.topics.interval.seconds", "1")
                    .endSourceConnector()
                .endMirror()
            .endSpec()
            .build());

        LOGGER.info("Sending and receiving messages via {}", kafkaClusterSourceName);

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Changing to {} and will try to receive messages", kafkaClusterTargetName);

        clients = new KafkaClientsBuilder(clients)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.consumerStrimzi());
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        if (!Environment.isKRaftModeEnabled()) {
            LOGGER.info("Checking if the mirrored Topic name is same as the original one");

            List<String> kafkaTopics = KafkaCmdClient.listTopicsUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));
            assertNotNull(kafkaTopics.stream().filter(kafkaTopic -> kafkaTopic.equals(testStorage.getTopicName())).findAny());

            String kafkaTopicSpec = KafkaCmdClient.describeTopicUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(kafkaClusterTargetName), testStorage.getTopicName());
            assertThat(kafkaTopicSpec, containsString("Topic: " + testStorage.getTopicName()));
            assertThat(kafkaTopicSpec, containsString("PartitionCount: 3"));
        }
    }

    @ParallelNamespaceTest
    void testConfigureDeploymentStrategy(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        // Deploy source kafka
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewDeployment()
                        .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                    .endDeployment()
                .endTemplate()
            .endSpec()
            .build());

        LabelSelector mmSelector = KafkaMirrorMaker2Resource.getLabelSelector(testStorage.getClusterName(), KafkaMirrorMaker2Resources.deploymentName(testStorage.getClusterName()));

        LOGGER.info("Adding label to MirrorMaker2 resource, the CR should be recreated");
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(testStorage.getClusterName(),
            mm2 -> mm2.getMetadata().setLabels(Collections.singletonMap("some", "label")), testStorage.getNamespaceName());
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), mmSelector, 1);

        KafkaMirrorMaker2 kmm2 = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get();

        LOGGER.info("Checking that observed gen. is still on 1 (recreation) and new label is present");
        assertThat(kmm2.getStatus().getObservedGeneration(), is(1L));
        assertThat(kmm2.getMetadata().getLabels().toString(), containsString("some=label"));
        assertThat(kmm2.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.RECREATE));

        LOGGER.info("Changing Deployment strategy to {}", DeploymentStrategy.ROLLING_UPDATE);
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(testStorage.getClusterName(),
            mm2 -> mm2.getSpec().getTemplate().getDeployment().setDeploymentStrategy(DeploymentStrategy.ROLLING_UPDATE), testStorage.getNamespaceName());
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(testStorage.getNamespaceName(), testStorage.getClusterName());


        LOGGER.info("Checking that observed gen. higher (rolling update) and label is changed");
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(testStorage.getClusterName(), mm2 -> mm2.getMetadata().getLabels().put("another", "label"), testStorage.getNamespaceName());
        StUtils.waitUntilSupplierIsSatisfied(
                () -> {
                    final KafkaMirrorMaker2 kMM2 = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get();

                    return kMM2.getStatus().getObservedGeneration() == 2L &&
                            kMM2.getMetadata().getLabels().toString().contains("another=label") &&
                            kMM2.getSpec().getTemplate().getDeployment().getDeploymentStrategy().equals(DeploymentStrategy.ROLLING_UPDATE);
                }
        );

        LOGGER.info("Adding another label to MirrorMaker2 resource, Pods should be rolled");
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), mmSelector, 1);
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("This the is failing with KRaft and need more investigation")
    void testRestoreOffsetsInConsumerGroup(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        final String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        final String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        final String sourceMirroredTopicName = kafkaClusterSourceName + "." + testStorage.getTopicName();
        final String consumerGroup = ClientUtils.generateRandomConsumerGroup();

        final String sourceProducerName = testStorage.getProducerName() + "-source";
        final String sourceConsumerName = testStorage.getConsumerName() + "-source";

        final String targetProducerName = testStorage.getProducerName() + "-target";
        final String targetConsumerName = testStorage.getProducerName() + "-target";

        resourceManager.createResourceWithWait(extensionContext,
            KafkaTemplates.kafkaPersistent(kafkaClusterSourceName, 1).build(),
            KafkaTemplates.kafkaPersistent(kafkaClusterTargetName, 1).build()
        );

        Map<String, Object> sourceConnectorConfig = new HashMap<>();
        sourceConnectorConfig.put("refresh.topics.interval.seconds", "1");
        sourceConnectorConfig.put("replication.factor", "1");
        sourceConnectorConfig.put("offset-syncs.topic.replication.factor", "1");

        Map<String, Object> checkpointConnectorConfig = new HashMap<>();
        checkpointConnectorConfig.put("refresh.groups.interval.seconds", "1");
        checkpointConnectorConfig.put("sync.group.offsets.enabled", "true");
        checkpointConnectorConfig.put("sync.group.offsets.interval.seconds", "1");
        checkpointConnectorConfig.put("emit.checkpoints.enabled", "true");
        checkpointConnectorConfig.put("emit.checkpoints.interval.seconds", "1");
        checkpointConnectorConfig.put("checkpoints.topic.replication.factor", "1");

        Map<String, Object> heartbeatConnectorConfig = new HashMap<>();
        heartbeatConnectorConfig.put("heartbeats.topic.replication.factor", "1");

        resourceManager.createResourceWithWait(extensionContext,
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(kafkaClusterSourceName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
                .editSpec()
                    .editFirstMirror()
                        .editSourceConnector()
                            .addToConfig(sourceConnectorConfig)
                        .endSourceConnector()
                        .editCheckpointConnector()
                            .addToConfig(checkpointConnectorConfig)
                        .endCheckpointConnector()
                        .editOrNewHeartbeatConnector()
                            .addToConfig(heartbeatConnectorConfig)
                        .endHeartbeatConnector()
                        .withTopicsPattern(".*")
                        .withGroupsPattern(".*")
                    .endMirror()
                .endSpec()
                .build(),
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(kafkaClusterTargetName, kafkaClusterSourceName, kafkaClusterTargetName, 1, false)
                .editSpec()
                    .editFirstMirror()
                        .editSourceConnector()
                            .addToConfig(sourceConnectorConfig)
                        .endSourceConnector()
                        .editCheckpointConnector()
                            .addToConfig(checkpointConnectorConfig)
                        .endCheckpointConnector()
                        .editOrNewHeartbeatConnector()
                            .addToConfig(heartbeatConnectorConfig)
                        .endHeartbeatConnector()
                        .withTopicsPattern(".*")
                        .withGroupsPattern(".*")
                    .endMirror()
                .endSpec()
                .build(),
            KafkaTopicTemplates.topic(kafkaClusterSourceName, testStorage.getTopicName(), 3, testStorage.getNamespaceName()).build()
        );

        KafkaClients initialInternalClientSourceJob = new KafkaClientsBuilder()
                .withProducerName(sourceProducerName)
                .withConsumerName(sourceConsumerName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
                .withTopicName(testStorage.getTopicName())
                .withMessageCount(testStorage.getMessageCount())
                .withMessage("Producer A")
                .withConsumerGroup(consumerGroup)
                .withNamespaceName(testStorage.getNamespaceName())
                .build();

        KafkaClients initialInternalClientTargetJob = new KafkaClientsBuilder()
                .withProducerName(targetProducerName)
                .withConsumerName(targetConsumerName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
                .withTopicName(sourceMirroredTopicName)
                .withMessageCount(testStorage.getMessageCount())
                .withConsumerGroup(consumerGroup)
                .withNamespaceName(testStorage.getNamespaceName())
                .build();

        LOGGER.info("Send & receive {} messages to/from Source cluster", testStorage.getMessageCount());
        resourceManager.createResourceWithWait(extensionContext,
            initialInternalClientSourceJob.producerStrimzi(),
            initialInternalClientSourceJob.consumerStrimzi());

        ClientUtils.waitForClientsSuccess(sourceProducerName, sourceConsumerName, testStorage.getNamespaceName(), testStorage.getMessageCount());

        LOGGER.info("Send {} messages to Source cluster", testStorage.getMessageCount());
        KafkaClients internalClientSourceJob = new KafkaClientsBuilder(initialInternalClientSourceJob).withMessage("Producer B").build();

        resourceManager.createResourceWithWait(extensionContext,
            internalClientSourceJob.producerStrimzi());
        ClientUtils.waitForClientSuccess(sourceProducerName, testStorage.getNamespaceName(), testStorage.getMessageCount());

        LOGGER.info("Receive {} messages from mirrored Topic on target cluster", testStorage.getMessageCount());
        resourceManager.createResourceWithWait(extensionContext,
            initialInternalClientTargetJob.consumerStrimzi());
        ClientUtils.waitForClientSuccess(targetConsumerName, testStorage.getNamespaceName(), testStorage.getMessageCount());

        LOGGER.info("Send 50 messages to Source cluster");
        internalClientSourceJob = new KafkaClientsBuilder(internalClientSourceJob).withMessageCount(50).withMessage("Producer C").build();
        resourceManager.createResourceWithWait(extensionContext,
            internalClientSourceJob.producerStrimzi());
        ClientUtils.waitForClientSuccess(sourceProducerName, testStorage.getNamespaceName(), 50);

        LOGGER.info("Receive 10 messages from source cluster");
        internalClientSourceJob = new KafkaClientsBuilder(internalClientSourceJob).withMessageCount(10).withAdditionalConfig("max.poll.records=10").build();
        resourceManager.createResourceWithWait(extensionContext,
            internalClientSourceJob.consumerStrimzi());
        ClientUtils.waitForClientSuccess(sourceConsumerName, testStorage.getNamespaceName(), 10);

        LOGGER.info("Receive 40 messages from mirrored Topic on target cluster");
        KafkaClients internalClientTargetJob = new KafkaClientsBuilder(initialInternalClientTargetJob).withMessageCount(40).build();
        resourceManager.createResourceWithWait(extensionContext,
            internalClientTargetJob.consumerStrimzi());
        ClientUtils.waitForClientSuccess(targetConsumerName, testStorage.getNamespaceName(), 40);

        LOGGER.info("There should be no more messages to read. Try to consume at least 1 message. " +
                "This client Job should fail on timeout.");
        resourceManager.createResourceWithWait(extensionContext,
            initialInternalClientTargetJob.consumerStrimzi());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(targetConsumerName, testStorage.getNamespaceName(), 1));

        LOGGER.info("As it's Active-Active MirrorMaker2 mode, there should be no more messages to read from Source cluster" +
                " topic. This client Job should fail on timeout.");
        resourceManager.createResourceWithWait(extensionContext,
            initialInternalClientSourceJob.consumerStrimzi());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(sourceConsumerName, testStorage.getNamespaceName(), 1));
    }

    @ParallelNamespaceTest
    void testKafkaMirrorMaker2ReflectsConnectorsState(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";
        String errorMessage = "One or more connectors are in FAILED state";

        resourceManager.createResourceWithWait(extensionContext,
            KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build(),
            KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        resourceManager.createResourceWithoutWait(extensionContext,
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
                .editSpec()
                    .editMatchingCluster(spec -> spec.getAlias().equals(kafkaClusterSourceName))
                        // typo in bootstrap name, connectors should not connect and MM2 should be in NotReady state with error
                        .withBootstrapServers(KafkaResources.bootstrapServiceName(kafkaClusterSourceName) + ".:9092")
                    .endCluster()
                .endSpec()
                .build());

        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2StatusMessage(testStorage.getNamespaceName(), testStorage.getClusterName(), errorMessage);

        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(testStorage.getClusterName(), mm2 ->
            mm2.getSpec().getClusters().stream().filter(mm2ClusterSpec -> mm2ClusterSpec.getAlias().equals(kafkaClusterSourceName))
                .findFirst().get().setBootstrapServers(KafkaUtils.namespacedPlainBootstrapAddress(kafkaClusterSourceName, testStorage.getNamespaceName())), testStorage.getNamespaceName());

        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(testStorage.getNamespaceName(), testStorage.getClusterName());

        KafkaMirrorMaker2Status kmm2Status = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        assertFalse(kmm2Status.getConditions().stream().anyMatch(condition -> condition.getMessage() != null && condition.getMessage().contains(errorMessage)));
    }

    /**
     * Test mirroring messages by MirrorMaker 2 over tls transport using scram-sha-512 auth
     * while user Scram passwords, CA cluster and clients certificates are changed.
     */
    @ParallelNamespaceTest
    @KRaftWithoutUTONotSupported
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testKMM2RollAfterSecretsCertsUpdateScramSha(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext);

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        String sourceMirroredTopicName = kafkaClusterSourceName + "." + testStorage.getTopicName();

        String kafkaUserSourceName = testStorage.getUsername() + "-source";
        String kafkaUserTargetName = testStorage.getUsername() + "-target";

        final String customSecretSource = "custom-secret-source";
        final String customSecretTarget = "custom-secret-target";

        // Deploy source and target kafkas with tls listener and SCRAM-SHA authentication
        resourceManager.createResourceWithWait(extensionContext,
            KafkaTemplates.kafkaPersistent(kafkaClusterSourceName, 1, 1)
                .editSpec()
                    .editKafka()
                        .withListeners(
                            new GenericKafkaListenerBuilder()
                                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationScramSha512Auth()
                                .endKafkaListenerAuthenticationScramSha512Auth()
                                .build()
                        )
                    .endKafka()
                .endSpec()
                .build(),
            KafkaTemplates.kafkaPersistent(kafkaClusterTargetName, 1, 1)
                .editSpec()
                    .editKafka()
                        .withListeners(
                            new GenericKafkaListenerBuilder()
                                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationScramSha512Auth()
                                .endKafkaListenerAuthenticationScramSha512Auth()
                                .build()
                        )
                    .endKafka()
                .endSpec()
                .build()
        );

        resourceManager.createResourceWithWait(extensionContext,
            KafkaTopicTemplates.topic(kafkaClusterSourceName, testStorage.getTopicName(), testStorage.getNamespaceName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), kafkaClusterSourceName, kafkaUserSourceName).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), kafkaClusterTargetName, kafkaUserTargetName).build()
        );

        // Initialize PasswordSecretSource to set this as PasswordSecret in Source/Target MirrorMaker2 spec
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName(kafkaUserSourceName);
        passwordSecretSource.setPassword("password");

        PasswordSecretSource passwordSecretTarget = new PasswordSecretSource();
        passwordSecretTarget.setSecretName(kafkaUserTargetName);
        passwordSecretTarget.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for source
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterSourceName));

        // Initialize CertSecretSource with certificate and secret names for target
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterTargetName));

        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, true)
            .editSpec()
                .editMatchingCluster(spec -> spec.getAlias().equals(kafkaClusterSourceName))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(kafkaUserSourceName)
                        .withPasswordSecret(passwordSecretSource)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                .endCluster()
                .editMatchingCluster(spec -> spec.getAlias().equals(kafkaClusterTargetName))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(kafkaUserTargetName)
                        .withPasswordSecret(passwordSecretTarget)
                    .endKafkaClientAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                .endCluster()
            .endSpec()
            .build());

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterSourceName))
            .withUsername(kafkaUserSourceName)
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerScramShaTlsStrimzi(kafkaClusterSourceName), clients.consumerScramShaTlsStrimzi(kafkaClusterSourceName));
        ClientUtils.waitForClientsSuccess(testStorage);

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(sourceMirroredTopicName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterTargetName))
            .withUsername(kafkaUserTargetName)
            .build();

        LOGGER.info("Now messages should be mirrored to target Topic and cluster");

        resourceManager.createResourceWithWait(extensionContext, clients.consumerScramShaTlsStrimzi(kafkaClusterTargetName));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        LOGGER.info("Messages successfully mirrored");

        LabelSelector mmSelector = KafkaMirrorMaker2Resource.getLabelSelector(testStorage.getClusterName(), KafkaMirrorMaker2Resources.deploymentName(testStorage.getClusterName()));
        Map<String, String> mmSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), mmSelector);

        LOGGER.info("Changing KafkaUser sha-password on MirrorMaker2 Source and make sure it rolled");

        KafkaUserUtils.modifyKafkaUserPasswordWithNewSecret(testStorage.getNamespaceName(), kafkaUserSourceName, customSecretSource, "c291cmNlLXBhc3N3b3Jk", extensionContext);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), mmSelector, 1, mmSnapshot);
        mmSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), mmSelector);

        KafkaUserUtils.modifyKafkaUserPasswordWithNewSecret(testStorage.getNamespaceName(), kafkaUserTargetName, customSecretTarget, "dGFyZ2V0LXBhc3N3b3Jk", extensionContext);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), mmSelector, 1, mmSnapshot);

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), mmSelector, 1, mmSnapshot);

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterSourceName))
            .withUsername(kafkaUserSourceName)
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerScramShaTlsStrimzi(kafkaClusterSourceName));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(sourceMirroredTopicName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterTargetName))
            .withUsername(kafkaUserTargetName)
            .build();

        LOGGER.info("Now messages should be mirrored to target Topic and cluster");

        resourceManager.createResourceWithWait(extensionContext, clients.consumerScramShaTlsStrimzi(kafkaClusterTargetName));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        LOGGER.info("Messages successfully mirrored");
    }

    @ParallelNamespaceTest
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testKMM2RollAfterSecretsCertsUpdateTLS(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext);

        String kafkaClusterSourceName = testStorage.getClusterName() + "-source";
        String kafkaClusterTargetName = testStorage.getClusterName() + "-target";

        String sourceMirroredTopicName = kafkaClusterSourceName + "." + testStorage.getTopicName();

        String kafkaUserSourceName = testStorage.getUsername() + "-source";
        String kafkaUserTargetName = testStorage.getUsername() + "-target";

        // Deploy source kafka with tls listener and mutual tls auth
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaPersistent(kafkaClusterSourceName, 1)
            .editSpec()
                .editKafka()
                    .addToConfig("min.insync.replicas", 1)
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
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaPersistent(kafkaClusterTargetName, 1)
            .editSpec()
                .editKafka()
                    .addToConfig("min.insync.replicas", 1)
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

        resourceManager.createResourceWithWait(extensionContext,
            KafkaTopicTemplates.topic(kafkaClusterSourceName, testStorage.getTopicName(), 3, testStorage.getNamespaceName()).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), kafkaClusterSourceName, kafkaUserSourceName).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), kafkaClusterTargetName, kafkaUserTargetName).build()
        );

        // Initialize CertSecretSource with certificate and secret names for source
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterSourceName));

        // Initialize CertSecretSource with certificate and secret names for target
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterTargetName));

        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, true)
            .editSpec()
                .editMatchingCluster(spec -> spec.getAlias().equals(kafkaClusterSourceName))
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(kafkaUserSourceName)
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                .endCluster()
                .editMatchingCluster(spec -> spec.getAlias().equals(kafkaClusterTargetName))
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(kafkaUserTargetName)
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                .endCluster()
                .editFirstMirror()
                        .editSourceConnector()
                            .addToConfig("refresh.topics.interval.seconds", 1)
                        .endSourceConnector()
                .endMirror()
            .endSpec()
            .build());

        LabelSelector mmSelector = KafkaMirrorMaker2Resource.getLabelSelector(testStorage.getClusterName(), KafkaMirrorMaker2Resources.deploymentName(testStorage.getClusterName()));
        Map<String, String> mmSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), mmSelector);

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterSourceName))
            .withUsername(kafkaUserSourceName)
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(MESSAGE_COUNT)
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(kafkaClusterSourceName), clients.consumerTlsStrimzi(kafkaClusterSourceName));
        ClientUtils.waitForClientsSuccess(testStorage);

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(sourceMirroredTopicName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterTargetName))
            .withUsername(kafkaUserTargetName)
            .build();

        LOGGER.info("Consumer in target cluster and Topic should receive {} messages", MESSAGE_COUNT);

        resourceManager.createResourceWithWait(extensionContext, clients.consumerTlsStrimzi(kafkaClusterTargetName));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        LOGGER.info("Messages successfully mirrored");

        LabelSelector zkSourceSelector = KafkaResource.getLabelSelector(kafkaClusterSourceName, KafkaResources.zookeeperStatefulSetName(kafkaClusterSourceName));
        LabelSelector kafkaSourceSelector = KafkaResource.getLabelSelector(kafkaClusterSourceName, KafkaResources.kafkaStatefulSetName(kafkaClusterSourceName));

        LabelSelector zkTargetSelector = KafkaResource.getLabelSelector(kafkaClusterTargetName, KafkaResources.zookeeperStatefulSetName(kafkaClusterTargetName));
        LabelSelector kafkaTargetSelector = KafkaResource.getLabelSelector(kafkaClusterTargetName, KafkaResources.kafkaStatefulSetName(kafkaClusterTargetName));

        Map<String, String> kafkaSourcePods = PodUtils.podSnapshot(testStorage.getNamespaceName(), kafkaSourceSelector);
        Map<String, String> zkSourcePods = PodUtils.podSnapshot(testStorage.getNamespaceName(), zkSourceSelector);
        Map<String, String> eoSourcePods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(kafkaClusterSourceName));
        Map<String, String> kafkaTargetPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), kafkaTargetSelector);
        Map<String, String> zkTargetPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), zkTargetSelector);
        Map<String, String> eoTargetPods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(kafkaClusterTargetName));

        LOGGER.info("Renew Clients CA secret for Source cluster via annotation");
        String sourceClientsCaSecretName = KafkaResources.clientsCaCertificateSecretName(kafkaClusterSourceName);
        SecretUtils.annotateSecret(testStorage.getNamespaceName(), sourceClientsCaSecretName, Ca.ANNO_STRIMZI_IO_FORCE_RENEW, "true");
        kafkaSourcePods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), kafkaSourceSelector, 1, kafkaSourcePods);
        mmSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), mmSelector, 1, mmSnapshot);

        LOGGER.info("Renew Clients CA secret for target cluster via annotation");
        String targetClientsCaSecretName = KafkaResources.clientsCaCertificateSecretName(kafkaClusterTargetName);
        SecretUtils.annotateSecret(testStorage.getNamespaceName(), targetClientsCaSecretName, Ca.ANNO_STRIMZI_IO_FORCE_RENEW, "true");
        kafkaTargetPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), kafkaTargetSelector, 1, kafkaTargetPods);
        mmSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), mmSelector, 1, mmSnapshot);

        LOGGER.info("Send and receive messages after clients certs were removed");
        clients = new KafkaClientsBuilder(clients)
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterSourceName))
            .withUsername(kafkaUserSourceName)
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(kafkaClusterSourceName));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        LOGGER.info("Consumer in target cluster and Topic should receive {} messages", MESSAGE_COUNT);

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(sourceMirroredTopicName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterTargetName))
            .withUsername(kafkaUserTargetName)
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.consumerTlsStrimzi(kafkaClusterTargetName));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        LOGGER.info("Messages successfully mirrored");

        LOGGER.info("Renew Cluster CA secret for Source clusters via annotation");
        String sourceClusterCaSecretName = KafkaResources.clusterCaCertificateSecretName(kafkaClusterSourceName);
        SecretUtils.annotateSecret(testStorage.getNamespaceName(), sourceClusterCaSecretName, Ca.ANNO_STRIMZI_IO_FORCE_RENEW, "true");

        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), zkSourceSelector, 1, zkSourcePods);
        }
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), kafkaSourceSelector, 1, kafkaSourcePods);
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(kafkaClusterSourceName), 1, eoSourcePods);
        mmSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), mmSelector, 1, mmSnapshot);

        LOGGER.info("Renew Cluster CA secret for target clusters via annotation");
        String targetClusterCaSecretName = KafkaResources.clusterCaCertificateSecretName(kafkaClusterTargetName);
        SecretUtils.annotateSecret(testStorage.getNamespaceName(), targetClusterCaSecretName, Ca.ANNO_STRIMZI_IO_FORCE_RENEW, "true");

        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), zkTargetSelector, 1, zkTargetPods);
        }
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), kafkaTargetSelector, 1, kafkaTargetPods);
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(kafkaClusterTargetName), 1, eoTargetPods);
        mmSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), mmSelector, 1, mmSnapshot);

        LOGGER.info("Send and receive messages after clients certs were removed");
        clients = new KafkaClientsBuilder(clients)
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterSourceName))
            .withUsername(kafkaUserSourceName)
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(kafkaClusterSourceName));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        LOGGER.info("Consumer in target cluster and Topic should receive {} messages", MESSAGE_COUNT);

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(sourceMirroredTopicName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(kafkaClusterTargetName))
            .withUsername(kafkaUserTargetName)
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.consumerTlsStrimzi(kafkaClusterTargetName));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        LOGGER.info("Messages successfully mirrored");
    }

    private void testDockerImagesForKafkaMirrorMaker2(String clusterName, String clusterOperatorNamespace, String mirrorMakerNamespace) {
        LOGGER.info("Verifying docker image names");
        // we must use INFRA_NAMESPACE because there is CO deployed
        Map<String, String> imgFromDeplConf = getImagesFromConfig(clusterOperatorNamespace);
        // Verifying docker image for kafka mirrormaker2
        String mirrormaker2ImageName = PodUtils.getFirstContainerImageNameFromPod(mirrorMakerNamespace, kubeClient(mirrorMakerNamespace).listPods(mirrorMakerNamespace, clusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND)
            .get(0).getMetadata().getName());

        String mirrormaker2Version = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(mirrorMakerNamespace).withName(clusterName).get().getSpec().getVersion();
        if (mirrormaker2Version == null) {
            mirrormaker2Version = Environment.ST_KAFKA_VERSION;
        }

        assertThat(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_MIRROR_MAKER_2_IMAGE_MAP)).get(mirrormaker2Version), is(mirrormaker2ImageName));
        LOGGER.info("Docker images verified");
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(Constants.CO_NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_SHORT)
            .createInstallation()
            .runInstallation();
    }
}
