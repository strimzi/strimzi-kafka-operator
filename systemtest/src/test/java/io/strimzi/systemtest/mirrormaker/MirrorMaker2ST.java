/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.mirrormaker;

import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.KafkaListenerTls;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.template.DeploymentStrategy;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SCALABILITY;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
@Tag(MIRROR_MAKER2)
@Tag(CONNECT_COMPONENTS)
@Tag(INTERNAL_CLIENTS_USED)
class MirrorMaker2ST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MirrorMaker2ST.class);
    public static final String NAMESPACE = "mirrormaker2-cluster-test";

    private static final String MIRRORMAKER2_TOPIC_NAME = "mirrormaker2-topic-example";
    private static int consumerCounter = 0;
    private final int messagesCount = 200;

    private String kafkaClusterSourceName;
    private String kafkaClusterTargetName;

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    void testMirrorMaker2() {
        Map<String, Object> expectedConfig = StUtils.loadProperties("group.id=mirrormaker2-cluster\n" +
                "key.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "value.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "header.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "config.storage.topic=mirrormaker2-cluster-configs\n" +
                "status.storage.topic=mirrormaker2-cluster-status\n" +
                "offset.storage.topic=mirrormaker2-cluster-offsets\n" +
                "config.storage.replication.factor=1\n" +
                "status.storage.replication.factor=1\n" +
                "offset.storage.replication.factor=1\n" + 
                "config.providers=file\n" + 
                "config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider\n");

        String topicSourceName = MIRRORMAKER2_TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);
        String topicTargetName = kafkaClusterSourceName + "." + topicSourceName;
        String topicSourceNameMirrored = kafkaClusterSourceName + "." + AVAILABILITY_TOPIC_SOURCE_NAME;

        // Deploy source kafka
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());
        // Deploy Topic
        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterSourceName, topicSourceName, 3).build());

        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(false, clusterName + "-" + Constants.KAFKA_CLIENTS).build());

        final String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(AVAILABILITY_TOPIC_SOURCE_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(kafkaClusterSourceName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        // Check brokers availability
        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            AVAILABILITY_TOPIC_SOURCE_NAME, kafkaClusterSourceName, MESSAGE_COUNT);
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        LOGGER.info("Setting topic to {}, cluster to {} and changing consumer group",
            AVAILABILITY_TOPIC_TARGET_NAME, kafkaClusterTargetName);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(AVAILABILITY_TOPIC_TARGET_NAME)
            .withClusterName(kafkaClusterTargetName)
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            AVAILABILITY_TOPIC_TARGET_NAME, kafkaClusterTargetName, MESSAGE_COUNT);
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        KafkaMirrorMaker2Resource.createAndWaitForReadiness(KafkaMirrorMaker2Resource.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
                .editSpec()
                    .editFirstMirror()
                        .editSourceConnector()
                            .addToConfig("refresh.topics.interval.seconds", "60")
                        .endSourceConnector()
                    .endMirror()
                .endSpec()
                .build());

        LOGGER.info("Looks like the mirrormaker2 cluster my-cluster deployed OK");

        String podName = PodUtils.getPodNameByPrefix(KafkaMirrorMaker2Resources.deploymentName(clusterName));
        String kafkaPodJson = TestUtils.toJsonString(kubeClient().getPod(podName));

        assertThat(kafkaPodJson, hasJsonPath(StUtils.globalVariableJsonPathBuilder(0, "KAFKA_CONNECT_BOOTSTRAP_SERVERS"),
                hasItem(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))));
        assertThat(StUtils.getPropertiesFromJson(0, kafkaPodJson, "KAFKA_CONNECT_CONFIGURATION"), is(expectedConfig));
        testDockerImagesForKafkaMirrorMaker2();

        verifyLabelsOnPods(clusterName, "mirrormaker2", null, "KafkaMirrorMaker2");
        verifyLabelsForService(clusterName, "mirrormaker2-api", "KafkaMirrorMaker2");

        verifyLabelsForConfigMaps(kafkaClusterSourceName, null, kafkaClusterTargetName);
        verifyLabelsForServiceAccounts(kafkaClusterSourceName, null);

        LOGGER.info("Setting topic to {}, cluster to {} and changing consumer group",
            topicSourceName, kafkaClusterSourceName);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicSourceName)
            .withClusterName(kafkaClusterSourceName)
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            topicSourceName, kafkaClusterSourceName, MESSAGE_COUNT);
        int sent = internalKafkaClient.sendMessagesPlain();

        LOGGER.info("Consumer in source cluster and topic should receive {} messages", MESSAGE_COUNT);
        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesPlain()
        );

        LOGGER.info("Now setting topic to {} and cluster to {} - the messages should be mirrored",
            topicTargetName, kafkaClusterTargetName);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicTargetName)
            .withClusterName(kafkaClusterTargetName)
            .build();

        LOGGER.info("Consumer in target cluster and topic should receive {} messages", MESSAGE_COUNT);
        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesPlain()
        );

        LOGGER.info("Changing topic to {}", topicSourceNameMirrored);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicSourceNameMirrored)
            .build();

        LOGGER.info("Check if mm2 mirror automatically created topic");
        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesPlain()
        );
        LOGGER.info("Mirrored successful");

        KafkaTopic mirroredTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicTargetName).get();
        assertThat(mirroredTopic.getSpec().getPartitions(), is(3));
        assertThat(mirroredTopic.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(kafkaClusterTargetName));

        // Replace source topic resource with new data and check that mm2 update target topi
        KafkaTopicResource.replaceTopicResource(topicSourceName, kt -> kt.getSpec().setPartitions(8));
        KafkaTopicUtils.waitForKafkaTopicPartitionChange(topicTargetName, 8);
    }

    /**
     * Test mirroring messages by MirrorMaker 2.0 over tls transport using mutual tls auth
     */
    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    @Tag(ACCEPTANCE)
    void testMirrorMaker2TlsAndTlsClientAuth() {
        String topicSourceNameMirrored = kafkaClusterSourceName + "." + AVAILABILITY_TOPIC_SOURCE_NAME;
        String topicSourceName = MIRRORMAKER2_TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);
        String topicTargetName = kafkaClusterSourceName + "." + topicSourceName;
        String kafkaUserSourceName = "my-user-source";
        String kafkaUserTargetName = "my-user-target";

        KafkaListenerAuthenticationTls auth = new KafkaListenerAuthenticationTls();
        KafkaListenerTls listenerTls = new KafkaListenerTls();
        listenerTls.setAuth(auth);

        // Deploy source kafka with tls listener and mutual tls auth
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        // Deploy target kafka with tls listener and mutual tls auth
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        // Deploy topic
        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterSourceName, topicSourceName, 3).build());

        // Create Kafka user
        KafkaUser userSource = KafkaUserResource.createAndWaitForReadiness(KafkaUserResource.tlsUser(kafkaClusterSourceName, kafkaUserSourceName).build());

        KafkaUser userTarget = KafkaUserResource.createAndWaitForReadiness(KafkaUserResource.tlsUser(kafkaClusterTargetName, kafkaUserTargetName).build());

        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, userSource, userTarget).build());

        final String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName("my-topic-test-1")
            .withNamespaceName(NAMESPACE)
            .withClusterName(kafkaClusterSourceName)
            .withKafkaUsername(userSource.getMetadata().getName())
            .withMessageCount(messagesCount)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            "my-topic-test-1", kafkaClusterSourceName, messagesCount);
        // Check brokers availability
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        LOGGER.info("Setting topic to {}, cluster to {} and changing user to {}",
            "my-topic-test-2", kafkaClusterTargetName, userTarget.getMetadata().getName());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withClusterName(kafkaClusterTargetName)
            .withTopicName("my-topic-test-2")
            .withKafkaUsername(userTarget.getMetadata().getName())
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            "my-topic-test-2", kafkaClusterTargetName, messagesCount);
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        // Initialize CertSecretSource with certificate and secret names for source
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterSourceName));

        // Initialize CertSecretSource with certificate and secret names for target
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterTargetName));

        // Deploy Mirror Maker 2.0 with tls listener and mutual tls auth
        KafkaMirrorMaker2ClusterSpec sourceClusterWithTlsAuth = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias(kafkaClusterSourceName)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterSourceName))
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
                .build();

        KafkaMirrorMaker2ClusterSpec targetClusterWithTlsAuth = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias(kafkaClusterTargetName)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterTargetName))
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
                .addToConfig("config.storage.replication.factor", 1)
                .addToConfig("offset.storage.replication.factor", 1)
                .addToConfig("status.storage.replication.factor", 1)
                .build();

        KafkaMirrorMaker2Resource.createAndWaitForReadiness(KafkaMirrorMaker2Resource.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, true)
            .editSpec()
                .withClusters(sourceClusterWithTlsAuth, targetClusterWithTlsAuth)
                .editFirstMirror()
                    .withNewTopicsPattern(MIRRORMAKER2_TOPIC_NAME + ".*")
                .endMirror()
            .endSpec()
            .build());

        LOGGER.info("Setting topic to {}, cluster to {} and changing user to {}",
            topicSourceName, kafkaClusterSourceName, userSource.getMetadata().getName());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicSourceName)
            .withClusterName(kafkaClusterSourceName)
            .withKafkaUsername(userSource.getMetadata().getName())
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            topicSourceName, kafkaClusterSourceName, messagesCount);
        int sent = internalKafkaClient.sendMessagesTls();

        LOGGER.info("Receiving messages from - topic {}, cluster {} and message count of {}",
            topicSourceName, kafkaClusterSourceName, messagesCount);
        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesTls()
        );

        LOGGER.info("Now setting topic to {}, cluster to {} and user to {} - the messages should be mirrored",
            topicTargetName, kafkaClusterTargetName, userTarget.getMetadata().getName());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicTargetName)
            .withClusterName(kafkaClusterTargetName)
            .withKafkaUsername(userTarget.getMetadata().getName())
            .build();

        LOGGER.info("Consumer in target cluster and topic should receive {} messages", messagesCount);
        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesTls()
        );
        LOGGER.info("Messages successfully mirrored");

        KafkaTopic mirroredTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicTargetName).get();
        assertThat(mirroredTopic.getSpec().getPartitions(), is(3));
        assertThat(mirroredTopic.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(kafkaClusterTargetName));

        mirroredTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicSourceNameMirrored).get();
        assertThat(mirroredTopic, nullValue());
    }

    /**
     * Test mirroring messages by MirrorMaker 2.0 over tls transport using scram-sha-512 auth
     */
    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    void testMirrorMaker2TlsAndScramSha512Auth() {
        String topicSourceNameMirrored = kafkaClusterSourceName + "." + AVAILABILITY_TOPIC_SOURCE_NAME;
        String topicSourceName = MIRRORMAKER2_TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);
        String topicTargetName = kafkaClusterSourceName + "." + topicSourceName;
        String kafkaUserSource = "my-user-source";
        String kafkaUserTarget = "my-user-target";

        // Deploy source kafka with tls listener and SCRAM-SHA authentication
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        // Deploy target kafka with tls listener and SCRAM-SHA authentication
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        // Deploy topic
        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterSourceName, topicSourceName, 3).build());

        // Create Kafka user for source cluster
        KafkaUser userSource = KafkaUserResource.createAndWaitForReadiness(KafkaUserResource.scramShaUser(kafkaClusterSourceName, kafkaUserSource).build());

        // Create Kafka user for target cluster
        KafkaUser userTarget = KafkaUserResource.createAndWaitForReadiness(KafkaUserResource.scramShaUser(kafkaClusterTargetName, kafkaUserTarget).build());

        // Initialize PasswordSecretSource to set this as PasswordSecret in MirrorMaker2 spec
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName(kafkaUserSource);
        passwordSecretSource.setPassword("password");

        // Initialize PasswordSecretSource to set this as PasswordSecret in MirrorMaker2 spec
        PasswordSecretSource passwordSecretTarget = new PasswordSecretSource();
        passwordSecretTarget.setSecretName(kafkaUserTarget);
        passwordSecretTarget.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for source
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterSourceName));

        // Initialize CertSecretSource with certificate and secret names for target
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterTargetName));

        // Deploy client
        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, userSource, userTarget).build());

        final String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(AVAILABILITY_TOPIC_SOURCE_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(kafkaClusterSourceName)
            .withKafkaUsername(userSource.getMetadata().getName())
            .withMessageCount(messagesCount)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            AVAILABILITY_TOPIC_SOURCE_NAME, kafkaClusterSourceName, messagesCount);
        // Check brokers availability
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        LOGGER.info("Setting topic to {}, cluster to {} and changing user to {}",
            AVAILABILITY_TOPIC_TARGET_NAME, kafkaClusterTargetName, userTarget.getMetadata().getName());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(AVAILABILITY_TOPIC_TARGET_NAME)
            .withClusterName(kafkaClusterTargetName)
            .withKafkaUsername(userTarget.getMetadata().getName())
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            AVAILABILITY_TOPIC_TARGET_NAME, kafkaClusterTargetName, messagesCount);
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        // Deploy Mirror Maker with TLS and ScramSha512
        KafkaMirrorMaker2ClusterSpec sourceClusterWithScramSha512Auth = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias(kafkaClusterSourceName)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterSourceName))
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername(kafkaUserSource)
                    .withPasswordSecret(passwordSecretSource)
                .endKafkaClientAuthenticationScramSha512()
                .withNewTls()
                    .withTrustedCertificates(certSecretSource)
                .endTls()
                .build();

        KafkaMirrorMaker2ClusterSpec targetClusterWithScramSha512Auth = new KafkaMirrorMaker2ClusterSpecBuilder()
                .withAlias(kafkaClusterTargetName)
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(kafkaClusterTargetName))
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername(kafkaUserTarget)
                    .withPasswordSecret(passwordSecretTarget)
                .endKafkaClientAuthenticationScramSha512()
                .withNewTls()
                    .withTrustedCertificates(certSecretTarget)
                .endTls()
                .addToConfig("config.storage.replication.factor", 1)
                .addToConfig("offset.storage.replication.factor", 1)
                .addToConfig("status.storage.replication.factor", 1)
                .build();

        KafkaMirrorMaker2Resource.createAndWaitForReadiness(KafkaMirrorMaker2Resource.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, true)
            .editSpec()
                .withClusters(targetClusterWithScramSha512Auth, sourceClusterWithScramSha512Auth)
                .editFirstMirror()
                    .withTopicsBlacklistPattern("availability.*")
                .endMirror()
            .endSpec()
            .build());

        LOGGER.info("Setting topic to {}, cluster to {} and changing user to {}",
            topicSourceName, kafkaClusterSourceName, userSource.getMetadata().getName());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicSourceName)
            .withClusterName(kafkaClusterSourceName)
            .withKafkaUsername(userSource.getMetadata().getName())
            .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            topicSourceName, kafkaClusterSourceName, messagesCount);
        int sent = internalKafkaClient.sendMessagesTls();

        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesTls()
        );

        LOGGER.info("Changing to target - topic {}, cluster {}, user {}", topicTargetName, kafkaClusterTargetName, userTarget.getMetadata().getName());

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(topicTargetName)
            .withClusterName(kafkaClusterTargetName)
            .withKafkaUsername(userTarget.getMetadata().getName())
            .build();

        LOGGER.info("Now messages should be mirrored to target topic and cluster");
        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesTls()
        );
        LOGGER.info("Messages successfully mirrored");

        KafkaTopic mirroredTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicTargetName).get();
        assertThat(mirroredTopic.getSpec().getPartitions(), is(3));
        assertThat(mirroredTopic.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(kafkaClusterTargetName));

        mirroredTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicSourceNameMirrored).get();
        assertThat(mirroredTopic, nullValue());
    }

    private void testDockerImagesForKafkaMirrorMaker2() {
        LOGGER.info("Verifying docker image names");
        Map<String, String> imgFromDeplConf = getImagesFromConfig();
        //Verifying docker image for kafka mirrormaker2
        String mirrormaker2ImageName = PodUtils.getFirstContainerImageNameFromPod(kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).
                get(0).getMetadata().getName());

        String mirrormaker2Version = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(clusterName).get().getSpec().getVersion();
        if (mirrormaker2Version == null) {
            mirrormaker2Version = Environment.ST_KAFKA_VERSION;
        }

        assertThat(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_MIRROR_MAKER_2_IMAGE_MAP)).get(mirrormaker2Version), is(mirrormaker2ImageName));
        LOGGER.info("Docker images verified");
    }

    @Test
    @Tag(SCALABILITY)
    void testScaleMirrorMaker2Subresource() {
        // Deploy source kafka
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        KafkaMirrorMaker2Resource.createAndWaitForReadiness(KafkaMirrorMaker2Resource.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false).build());

        int scaleTo = 4;
        long mm2ObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(clusterName).get().getStatus().getObservedGeneration();
        String mm2GenName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaMirrorMaker2 subresource <-------");
        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient().scaleByName(KafkaMirrorMaker2.RESOURCE_KIND, clusterName, scaleTo);
        DeploymentUtils.waitForDeploymentAndPodsReady(KafkaMirrorMaker2Resources.deploymentName(clusterName), scaleTo);

        LOGGER.info("Check if replicas is set to {}, naming prefix should be same and observed generation higher", scaleTo);
        List<String> mm2Pods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND);
        assertThat(mm2Pods.size(), is(4));
        assertThat(KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(clusterName).get().getSpec().getReplicas(), is(4));
        assertThat(KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(clusterName).get().getStatus().getReplicas(), is(4));
        /*
        observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
        the observed generation is increased
        */
        assertThat(mm2ObsGen < KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(clusterName).get().getStatus().getObservedGeneration(), is(true));
        for (String pod : mm2Pods) {
            assertThat(pod.contains(mm2GenName), is(true));
        }
    }

    @Test
    void testMirrorMaker2CorrectlyMirrorsHeaders() {
        String sourceProducerName = "source-producer";
        String targetConsumerName = "target-consumer";
        String sourceExampleTopic = "source-example-topic";
        String targetExampleTopic = kafkaClusterSourceName + "." + sourceExampleTopic;

        // Deploy source kafka
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());
        // Deploy Topic for example clients
        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterSourceName, sourceExampleTopic).build());

        KafkaMirrorMaker2Resource.createAndWaitForReadiness(KafkaMirrorMaker2Resource.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false).build());

        //deploying example clients for checking if mm2 will mirror messages with headers

        KafkaBasicExampleClients targetKafkaClientsJob = new KafkaBasicExampleClients.Builder()
            .withConsumerName(targetConsumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
            .withTopicName(targetExampleTopic)
            .withMessageCount(MESSAGE_COUNT)
            .withDelayMs(1000)
            .build();

        targetKafkaClientsJob.createAndWaitForReadiness(targetKafkaClientsJob.consumerStrimzi().build());

        KafkaBasicExampleClients sourceKafkaClientsJob = new KafkaBasicExampleClients.Builder()
            .withProducerName(sourceProducerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
            .withTopicName(sourceExampleTopic)
            .withMessageCount(MESSAGE_COUNT)
            .withDelayMs(1000)
            .build();

        sourceKafkaClientsJob.createAndWaitForReadiness(sourceKafkaClientsJob.producerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editContainer(0)
                            .addNewEnv()
                                .withName("HEADERS")
                                .withValue("header_key_one=header_value_one, header_key_two=header_value_two")
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build());

        ClientUtils.waitTillContinuousClientsFinish(sourceProducerName, targetConsumerName, NAMESPACE, MESSAGE_COUNT);

        LOGGER.info("Checking log of {} job if the headers are correct", targetConsumerName);
        String header1 = "key: header_key_one, value: header_value_one";
        String header2 = "key: header_key_two, value: header_value_two";
        String log = StUtils.getLogFromPodByTime(kubeClient().listPodsByPrefixInName(targetConsumerName).get(0).getMetadata().getName(), "", MESSAGE_COUNT + "s");
        assertThat(log, containsString(header1));
        assertThat(log, containsString(header2));
    }

    @Test
    @Tag(SCALABILITY)
    void testScaleMirrorMaker2ToZero() {
        // Deploy source kafka
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        KafkaMirrorMaker2Resource.createAndWaitForReadiness(KafkaMirrorMaker2Resource.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 3, false).build());

        long oldObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(clusterName).get().getStatus().getObservedGeneration();
        String mm2DepName = KafkaMirrorMaker2Resources.deploymentName(clusterName);
        List<String> mm2Pods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND);
        assertThat(mm2Pods.size(), is(3));

        LOGGER.info("Scaling MirrorMaker2 to zero");
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(clusterName, mm2 -> mm2.getSpec().setReplicas(0));

        PodUtils.waitForPodsReady(kubeClient().getDeploymentSelectors(mm2DepName), 0, true);

        mm2Pods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND);
        KafkaMirrorMaker2Status mm2Status = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(clusterName).get().getStatus();
        long actualObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(clusterName).get().getMetadata().getGeneration();

        assertThat(mm2Pods.size(), is(0));
        assertThat(mm2Status.getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(actualObsGen, is(not(oldObsGen)));
        assertNull(mm2Status.getUrl());
    }

    @Test
    void testIdentityReplicationPolicy() {
        String originalTopicName = "original-topic";

        // Deploy source kafka
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());
        // Create topic
        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterSourceName, originalTopicName, 3).build());

        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(false, kafkaClientsName).build());

        final String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        KafkaMirrorMaker2Resource.createAndWaitForReadiness(KafkaMirrorMaker2Resource.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
                .editMirror(0)
                    .editSourceConnector()
                        .addToConfig("replication.policy.class", "io.strimzi.kafka.connect.mirror.IdentityReplicationPolicy")
                    .endSourceConnector()
                .endMirror()
            .endSpec()
            .build());

        LOGGER.info("Sending and receiving messages via {}", kafkaClusterSourceName);
        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withNamespaceName(NAMESPACE)
            .withTopicName(originalTopicName)
            .withClusterName(kafkaClusterSourceName)
            .withMessageCount(MESSAGE_COUNT)
            .withUsingPodName(kafkaClientsPodName)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.assertSentAndReceivedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        LOGGER.info("Changing to {} and will try to receive messages", kafkaClusterTargetName);

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withClusterName(kafkaClusterTargetName)
            .build();

        assertThat(internalKafkaClient.receiveMessagesPlain(), equalTo(MESSAGE_COUNT));

        LOGGER.info("Checking if the mirrored topic name is same as the original one");

        List<String> kafkaTopics = KafkaCmdClient.listTopicsUsingPodCli(kafkaClusterTargetName, 0);
        assertNotNull(kafkaTopics.stream().filter(kafkaTopic -> kafkaTopic.equals(originalTopicName)).findAny());

        List<String> kafkaTopicSpec = KafkaCmdClient.describeTopicUsingPodCli(kafkaClusterTargetName, 0, originalTopicName);
        assertThat(kafkaTopicSpec.get(0), equalTo("Topic:" + originalTopicName));
        assertThat(kafkaTopicSpec.get(1), equalTo("PartitionCount:3"));
    }

    @Test
    void testHostAliases() {
        HostAlias hostAlias = new HostAliasBuilder()
            .withIp(aliasIp)
            .withHostnames(aliasHostname)
            .build();

        // Deploy source kafka
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        KafkaMirrorMaker2Resource.createAndWaitForReadiness(KafkaMirrorMaker2Resource.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
                .withNewTemplate()
                    .withNewPod()
                        .withHostAliases(hostAlias)
                    .endPod()
                .endTemplate()
            .endSpec()
            .build());

        String mm2PodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).get(0).getMetadata().getName();

        LOGGER.info("Checking the /etc/hosts file");
        String output = cmdKubeClient().execInPod(mm2PodName, "cat", "/etc/hosts").out();
        assertThat(output, containsString(etcHostsData));
    }

    @Test
    void testConfigureDeploymentStrategy() {
        // Deploy source kafka
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        // Deploy target kafka
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        KafkaMirrorMaker2Resource.createAndWaitForReadiness(KafkaMirrorMaker2Resource.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewDeployment()
                        .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                    .endDeployment()
                .endTemplate()
            .endSpec()
            .build());

        String mm2DepName = KafkaMirrorMaker2Resources.deploymentName(clusterName);

        LOGGER.info("Adding label to MirrorMaker2 resource, the CR should be recreated");
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(clusterName,
            mm2 -> mm2.getMetadata().setLabels(Collections.singletonMap("some", "label")));
        DeploymentUtils.waitForDeploymentAndPodsReady(mm2DepName, 1);

        KafkaMirrorMaker2 kmm2 = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(clusterName).get();

        LOGGER.info("Checking that observed gen. is still on 1 (recreation) and new label is present");
        assertThat(kmm2.getStatus().getObservedGeneration(), is(1L));
        assertThat(kmm2.getMetadata().getLabels().toString(), containsString("some=label"));
        assertThat(kmm2.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.RECREATE));

        LOGGER.info("Changing deployment strategy to {}", DeploymentStrategy.ROLLING_UPDATE);
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(clusterName,
            mm2 -> mm2.getSpec().getTemplate().getDeployment().setDeploymentStrategy(DeploymentStrategy.ROLLING_UPDATE));
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(clusterName);

        LOGGER.info("Adding another label to MirrorMaker2 resource, pods should be rolled");
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(clusterName, mm2 -> mm2.getMetadata().getLabels().put("another", "label"));
        DeploymentUtils.waitForDeploymentAndPodsReady(mm2DepName, 1);

        LOGGER.info("Checking that observed gen. higher (rolling update) and label is changed");
        kmm2 = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(clusterName).get();
        assertThat(kmm2.getStatus().getObservedGeneration(), is(2L));
        assertThat(kmm2.getMetadata().getLabels().toString(), containsString("another=label"));
        assertThat(kmm2.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.ROLLING_UPDATE));
    }

    @Test
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testRestoreOffsetsInConsumerGroup() {
        final String syncGroupOffsetsIntervalSeconds = "1";
        final String topicSourceNameMirrored = "test-sync-offset-" + new Random().nextInt(Integer.MAX_VALUE);
        final String topicTargetNameMirrored = kafkaClusterSourceName + "." + topicSourceNameMirrored;
        final String consumerGroup = "mm2-test-consumer-group";
        final String sourceProducerName = "mm2-producer-source-" + ClientUtils.generateRandomConsumerGroup();
        final String sourceConsumerName = "mm2-consumer-source-" + ClientUtils.generateRandomConsumerGroup();
        final String targetProducerName = "mm2-producer-target-" + ClientUtils.generateRandomConsumerGroup();
        final String targetConsumerName = "mm2-consumer-target-" + ClientUtils.generateRandomConsumerGroup();
        final String mm2SrcTrgName = clusterName + "-src-trg";
        final String mm2TrgSrcName = clusterName + "-trg-src";

        // Deploy source kafka
        KafkaResource.kafkaWithoutWait(KafkaResource.kafkaPersistent(kafkaClusterSourceName, 1, 1).build());

        // Deploy target kafka
        KafkaResource.kafkaWithoutWait(KafkaResource.kafkaPersistent(kafkaClusterTargetName, 1, 1).build());

        // Wait for Kafka clusters readiness
        KafkaUtils.waitForKafkaReady(kafkaClusterSourceName);
        KafkaUtils.waitForKafkaReady(kafkaClusterTargetName);

        // MM2 Active (S) <-> Active (T) // direction S -> T mirroring
        // *.replication.factor(s) to 1 are added just to speed up test by using only 1 ZK and 1 Kafka
        KafkaMirrorMaker2Resource.createAndWaitForReadiness(KafkaMirrorMaker2Resource.kafkaMirrorMaker2(mm2TrgSrcName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
            .editSpec()
                .editFirstMirror()
                    .editSourceConnector()
                        .addToConfig("refresh.topics.interval.seconds", "1")
                        .addToConfig("replication.factor", "1")
                        .addToConfig("offset-syncs.topic.replication.factor", "1")
                    .endSourceConnector()
                    .editCheckpointConnector()
                        .addToConfig("refresh.groups.interval.seconds", "1")
                        .addToConfig("sync.group.offsets.enabled", "true")
                        .addToConfig("sync.group.offsets.interval.seconds", syncGroupOffsetsIntervalSeconds)
                        .addToConfig("emit.checkpoints.enabled", "true")
                        .addToConfig("emit.checkpoints.interval.seconds", "1")
                        .addToConfig("checkpoints.topic.replication.factor", "1")
                    .endCheckpointConnector()
                    .editHeartbeatConnector()
                        .addToConfig("heartbeats.topic.replication.factor", "1")
                    .endHeartbeatConnector()
                    .withTopicsPattern(".*")
                    .withGroupsPattern(".*")
                .endMirror()
            .endSpec().build());

        // MM2 Active (S) <-> Active (T) // direction S <- T mirroring
        KafkaMirrorMaker2Resource.createAndWaitForReadiness(KafkaMirrorMaker2Resource.kafkaMirrorMaker2(mm2SrcTrgName, kafkaClusterSourceName, kafkaClusterTargetName, 1, false)
            .editSpec()
                .editFirstMirror()
                    .editSourceConnector()
                        .addToConfig("refresh.topics.interval.seconds", "1")
                        .addToConfig("replication.factor", "1")
                        .addToConfig("offset-syncs.topic.replication.factor", "1")
                    .endSourceConnector()
                    .editCheckpointConnector()
                        .addToConfig("refresh.groups.interval.seconds", "1")
                        .addToConfig("sync.group.offsets.enabled", "true")
                        .addToConfig("sync.group.offsets.interval.seconds", syncGroupOffsetsIntervalSeconds)
                        .addToConfig("emit.checkpoints.enabled", "true")
                        .addToConfig("emit.checkpoints.interval.seconds", "1")
                        .addToConfig("checkpoints.topic.replication.factor", "1")
                    .endCheckpointConnector()
                    .editHeartbeatConnector()
                        .addToConfig("heartbeats.topic.replication.factor", "1")
                    .endHeartbeatConnector()
                    .withTopicsPattern(".*")
                    .withGroupsPattern(".*")
                .endMirror()
            .endSpec().build());

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterSourceName, topicSourceNameMirrored, 3).build());

        KafkaBasicExampleClients initialInternalClientSourceJob = new KafkaBasicExampleClients.Builder()
                .withProducerName(sourceProducerName)
                .withConsumerName(sourceConsumerName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName))
                .withTopicName(topicSourceNameMirrored)
                .withMessageCount(MESSAGE_COUNT)
                .withMessage("Producer A")
                .withConsumerGroup(consumerGroup)
                .build();

        KafkaBasicExampleClients initialInternalClientTargetJob = new KafkaBasicExampleClients.Builder()
                .withProducerName(targetProducerName)
                .withConsumerName(targetConsumerName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
                .withTopicName(topicTargetNameMirrored)
                .withMessageCount(MESSAGE_COUNT)
                .withConsumerGroup(consumerGroup)
                .build();

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
                AVAILABILITY_TOPIC_SOURCE_NAME, kafkaClusterSourceName, MESSAGE_COUNT);

        LOGGER.info("Send & receive {} messages to/from Source cluster.", MESSAGE_COUNT);
        initialInternalClientSourceJob.createAndWaitForReadiness(initialInternalClientSourceJob.producerStrimzi().build());
        ClientUtils.waitForClientSuccess(sourceProducerName, NAMESPACE, MESSAGE_COUNT);
        initialInternalClientSourceJob.createAndWaitForReadiness(initialInternalClientSourceJob.consumerStrimzi().build());
        ClientUtils.waitForClientSuccess(sourceConsumerName, NAMESPACE, MESSAGE_COUNT);

        JobUtils.deleteJobWithWait(NAMESPACE, sourceProducerName);
        JobUtils.deleteJobWithWait(NAMESPACE, sourceConsumerName);

        LOGGER.info("Send {} messages to Source cluster.", MESSAGE_COUNT);
        KafkaBasicExampleClients internalClientSourceJob = initialInternalClientSourceJob.toBuilder().withMessage("Producer B").build();
        internalClientSourceJob.createAndWaitForReadiness(internalClientSourceJob.producerStrimzi().build());
        ClientUtils.waitForClientSuccess(sourceProducerName, NAMESPACE, MESSAGE_COUNT);

        LOGGER.info("Wait 1 second as 'sync.group.offsets.interval.seconds=1'. As this is insignificant wait, we're skipping it");

        LOGGER.info("Receive {} messages from mirrored topic on Target cluster.", MESSAGE_COUNT);
        initialInternalClientTargetJob.createAndWaitForReadiness(initialInternalClientTargetJob.consumerStrimzi().build());
        ClientUtils.waitForClientSuccess(targetConsumerName, NAMESPACE, MESSAGE_COUNT);
        JobUtils.deleteJobWithWait(NAMESPACE, sourceProducerName);
        JobUtils.deleteJobWithWait(NAMESPACE, targetConsumerName);

        LOGGER.info("Send 50 messages to Source cluster");
        internalClientSourceJob = internalClientSourceJob.toBuilder().withMessageCount(50).withMessage("Producer C").build();
        internalClientSourceJob.createAndWaitForReadiness(internalClientSourceJob.producerStrimzi().build());
        ClientUtils.waitForClientSuccess(sourceProducerName, NAMESPACE, 50);
        JobUtils.deleteJobWithWait(NAMESPACE, sourceProducerName);

        LOGGER.info("Wait 1 second as 'sync.group.offsets.interval.seconds=1'. As this is insignificant wait, we're skipping it");
        LOGGER.info("Receive 10 msgs from source cluster");
        internalClientSourceJob = internalClientSourceJob.toBuilder().withMessageCount(10).withAdditionalConfig("max.poll.records=10").build();
        internalClientSourceJob.createAndWaitForReadiness(internalClientSourceJob.consumerStrimzi().build());
        ClientUtils.waitForClientSuccess(sourceConsumerName, NAMESPACE, 10);
        JobUtils.deleteJobWithWait(NAMESPACE, sourceConsumerName);

        LOGGER.info("Wait 1 second as 'sync.group.offsets.interval.seconds=1'. As this is insignificant wait, we're skipping it");

        LOGGER.info("Receive 40 msgs from mirrored topic on Target cluster");
        KafkaBasicExampleClients internalClientTargetJob = initialInternalClientTargetJob.toBuilder().withMessageCount(40).build();
        internalClientTargetJob.createAndWaitForReadiness(internalClientTargetJob.consumerStrimzi().build());
        ClientUtils.waitForClientSuccess(targetConsumerName, NAMESPACE, 40);
        JobUtils.deleteJobWithWait(NAMESPACE, targetConsumerName);

        LOGGER.info("There should be no more messages to read. Try to consume at least 1 message. " +
                "This client job should fail on timeout.");
        initialInternalClientTargetJob.createAndWaitForReadiness(initialInternalClientTargetJob.consumerStrimzi().build());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(targetConsumerName, NAMESPACE, 1));

        LOGGER.info("As it's Active-Active MM2 mode, there should be no more messages to read from Source cluster" +
                " topic. This client job should fail on timeout.");
        initialInternalClientSourceJob.createAndWaitForReadiness(initialInternalClientSourceJob.consumerStrimzi().build());
        assertDoesNotThrow(() -> ClientUtils.waitForClientTimeout(sourceConsumerName, NAMESPACE, 1));
    }

    @BeforeAll
    void setup() {
        kafkaClusterSourceName = clusterName + "-source";
        kafkaClusterTargetName = clusterName + "-target";

        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_SHORT);
    }

    @AfterEach
    void removeResourcesAndTopics() {
        // force deletion of MM2 (all) components, after that remove other KafkaTopics
        // else they will/might be incorrectly recreated by MM2 component (TopicOperator)
        ResourceManager.deleteMethodResources();
        KafkaTopicList kafkaTopicList = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).list();
        kafkaTopicList.getItems().forEach(kafkaTopic -> {
            KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).delete(kafkaTopic);
            LOGGER.info("Topic {} deleted", kafkaTopic.getMetadata().getName());
            KafkaTopicUtils.waitForKafkaTopicDeletion(kafkaTopic.getMetadata().getName());
        });
    }
}
