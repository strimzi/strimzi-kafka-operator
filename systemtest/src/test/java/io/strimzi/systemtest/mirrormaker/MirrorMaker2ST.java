/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.mirrormaker;

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
import io.strimzi.api.kafka.model.status.KafkaMirrorMaker2Status;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SCALABILITY;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
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
    private final int messagesCount = 200;

    private String kafkaClusterSourceName = CLUSTER_NAME + "-source";
    private String kafkaClusterTargetName = CLUSTER_NAME + "-target";

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
        KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1).done();
        // Deploy target kafka
        KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1).done();
        // Deploy Topic
        KafkaTopicResource.topic(kafkaClusterSourceName, topicSourceName, 3).done();

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        final String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(AVAILABILITY_TOPIC_SOURCE_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(kafkaClusterSourceName)
            .withMessageCount(MESSAGE_COUNT)
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
        internalKafkaClient.setTopicName(AVAILABILITY_TOPIC_TARGET_NAME);
        internalKafkaClient.setClusterName(kafkaClusterTargetName);
        internalKafkaClient.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            AVAILABILITY_TOPIC_TARGET_NAME, kafkaClusterTargetName, MESSAGE_COUNT);
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        KafkaMirrorMaker2Resource.kafkaMirrorMaker2(CLUSTER_NAME, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
                .editSpec()
                    .editFirstMirror()
                        .editSourceConnector()
                            .addToConfig("refresh.topics.interval.seconds", "60")
                        .endSourceConnector()
                    .endMirror()
                .endSpec()
                .done();
        LOGGER.info("Looks like the mirrormaker2 cluster my-cluster deployed OK");

        String podName = PodUtils.getPodNameByPrefix(KafkaMirrorMaker2Resources.deploymentName(CLUSTER_NAME));
        String kafkaPodJson = TestUtils.toJsonString(kubeClient().getPod(podName));

        assertThat(kafkaPodJson, hasJsonPath(StUtils.globalVariableJsonPathBuilder(0, "KAFKA_CONNECT_BOOTSTRAP_SERVERS"),
                hasItem(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))));
        assertThat(StUtils.getPropertiesFromJson(0, kafkaPodJson, "KAFKA_CONNECT_CONFIGURATION"), is(expectedConfig));
        testDockerImagesForKafkaMirrorMaker2();

        verifyLabelsOnPods(CLUSTER_NAME, "mirrormaker2", null, "KafkaMirrorMaker2");
        verifyLabelsForService(CLUSTER_NAME, "mirrormaker2-api", "KafkaMirrorMaker2");

        verifyLabelsForConfigMaps(kafkaClusterSourceName, null, kafkaClusterTargetName);
        verifyLabelsForServiceAccounts(kafkaClusterSourceName, null);

        LOGGER.info("Setting topic to {}, cluster to {} and changing consumer group",
            topicSourceName, kafkaClusterSourceName);
        internalKafkaClient.setTopicName(topicSourceName);
        internalKafkaClient.setClusterName(kafkaClusterSourceName);
        internalKafkaClient.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());

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
        internalKafkaClient.setTopicName(topicTargetName);
        internalKafkaClient.setClusterName(kafkaClusterTargetName);
        internalKafkaClient.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());

        LOGGER.info("Consumer in target cluster and topic should receive {} messages", MESSAGE_COUNT);
        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesPlain()
        );

        LOGGER.info("Changing topic to {}", topicSourceNameMirrored);
        internalKafkaClient.setTopicName(topicSourceNameMirrored);
        internalKafkaClient.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());

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
        KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withTls(listenerTls)
                            .withNewTls()
                        .endTls()
                    .endListeners()
                .endKafka()
            .endSpec().done();

        // Deploy target kafka with tls listener and mutual tls auth
        KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withTls(listenerTls)
                            .withNewTls()
                        .endTls()
                    .endListeners()
                .endKafka()
            .endSpec().done();

        // Deploy topic
        KafkaTopicResource.topic(kafkaClusterSourceName, topicSourceName, 3).done();

        // Create Kafka user
        KafkaUser userSource = KafkaUserResource.tlsUser(kafkaClusterSourceName, kafkaUserSourceName).done();

        KafkaUser userTarget = KafkaUserResource.tlsUser(kafkaClusterTargetName, kafkaUserTargetName).done();

        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, userSource, userTarget).done();

        final String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName("my-topic-test-1")
            .withNamespaceName(NAMESPACE)
            .withClusterName(kafkaClusterSourceName)
            .withKafkaUsername(userSource.getMetadata().getName())
            .withMessageCount(messagesCount)
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
        internalKafkaClient.setTopicName("my-topic-test-2");
        internalKafkaClient.setClusterName(kafkaClusterTargetName);
        internalKafkaClient.setKafkaUsername(userTarget.getMetadata().getName());

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

        KafkaMirrorMaker2Resource.kafkaMirrorMaker2(CLUSTER_NAME, kafkaClusterTargetName, kafkaClusterSourceName, 1, true)
            .editSpec()
                .withClusters(sourceClusterWithTlsAuth, targetClusterWithTlsAuth)
                .editFirstMirror()
                    .withNewTopicsPattern(MIRRORMAKER2_TOPIC_NAME + ".*")
                .endMirror()
            .endSpec()
            .done();

        LOGGER.info("Setting topic to {}, cluster to {} and changing user to {}",
            topicSourceName, kafkaClusterSourceName, userSource.getMetadata().getName());
        internalKafkaClient.setTopicName(topicSourceName);
        internalKafkaClient.setClusterName(kafkaClusterSourceName);
        internalKafkaClient.setKafkaUsername(userSource.getMetadata().getName());

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
        internalKafkaClient.setTopicName(topicTargetName);
        internalKafkaClient.setClusterName(kafkaClusterTargetName);
        internalKafkaClient.setKafkaUsername(userTarget.getMetadata().getName());

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
        KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withNewTls().withAuth(new KafkaListenerAuthenticationScramSha512()).endTls()
                    .endListeners()
                .endKafka()
            .endSpec().done();

        // Deploy target kafka with tls listener and SCRAM-SHA authentication
        KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withNewTls().withAuth(new KafkaListenerAuthenticationScramSha512()).endTls()
                    .endListeners()
                .endKafka()
            .endSpec().done();

        // Deploy topic
        KafkaTopicResource.topic(kafkaClusterSourceName, topicSourceName, 3).done();

        // Create Kafka user for source cluster
        KafkaUser userSource = KafkaUserResource.scramShaUser(kafkaClusterSourceName, kafkaUserSource).done();

        // Create Kafka user for target cluster
        KafkaUser userTarget = KafkaUserResource.scramShaUser(kafkaClusterTargetName, kafkaUserTarget).done();

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
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, userSource, userTarget).done();

        final String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(AVAILABILITY_TOPIC_SOURCE_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(kafkaClusterSourceName)
            .withKafkaUsername(userSource.getMetadata().getName())
            .withMessageCount(messagesCount)
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
        internalKafkaClient.setTopicName(AVAILABILITY_TOPIC_TARGET_NAME);
        internalKafkaClient.setClusterName(kafkaClusterTargetName);
        internalKafkaClient.setKafkaUsername(userTarget.getMetadata().getName());
        internalKafkaClient.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());

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

        KafkaMirrorMaker2Resource.kafkaMirrorMaker2(CLUSTER_NAME, kafkaClusterTargetName, kafkaClusterSourceName, 1, true)
            .editSpec()
                .withClusters(targetClusterWithScramSha512Auth, sourceClusterWithScramSha512Auth)
                .editFirstMirror()
                    .withTopicsBlacklistPattern("availability.*")
                .endMirror()
            .endSpec().done();

        LOGGER.info("Setting topic to {}, cluster to {} and changing user to {}",
            topicSourceName, kafkaClusterSourceName, userSource.getMetadata().getName());
        internalKafkaClient.setTopicName(topicSourceName);
        internalKafkaClient.setClusterName(kafkaClusterSourceName);
        internalKafkaClient.setKafkaUsername(userSource.getMetadata().getName());
        internalKafkaClient.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());

        LOGGER.info("Sending messages to - topic {}, cluster {} and message count of {}",
            topicSourceName, kafkaClusterSourceName, messagesCount);
        int sent = internalKafkaClient.sendMessagesTls();

        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesTls()
        );

        LOGGER.info("Changing to target - topic {}, cluster {}, user {}", topicTargetName, kafkaClusterTargetName, userTarget.getMetadata().getName());
        internalKafkaClient.setTopicName(topicTargetName);
        internalKafkaClient.setClusterName(kafkaClusterTargetName);
        internalKafkaClient.setKafkaUsername(userTarget.getMetadata().getName());
        internalKafkaClient.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());

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

        String mirrormaker2Version = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getVersion();
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
        KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1).done();
        // Deploy target kafka
        KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1).done();

        KafkaMirrorMaker2Resource.kafkaMirrorMaker2(CLUSTER_NAME, kafkaClusterTargetName, kafkaClusterSourceName, 1, false).done();

        int scaleTo = 4;
        long mm2ObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getObservedGeneration();
        String mm2GenName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaMirrorMaker2 subresource <-------");
        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient().scaleByName(KafkaMirrorMaker2.RESOURCE_KIND, CLUSTER_NAME, scaleTo);
        DeploymentUtils.waitForDeploymentAndPodsReady(KafkaMirrorMaker2Resources.deploymentName(CLUSTER_NAME), scaleTo);

        LOGGER.info("Check if replicas is set to {}, naming prefix should be same and observed generation higher", scaleTo);
        List<String> mm2Pods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND);
        assertThat(mm2Pods.size(), is(4));
        assertThat(KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getReplicas(), is(4));
        assertThat(KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getReplicas(), is(4));
        /*
        observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
        the observed generation is increased
        */
        assertThat(mm2ObsGen < KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getObservedGeneration(), is(true));
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
        KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1).done();
        // Deploy target kafka
        KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1).done();
        // Deploy Topic for example clients
        KafkaTopicResource.topic(kafkaClusterSourceName, sourceExampleTopic).done();

        KafkaMirrorMaker2Resource.kafkaMirrorMaker2(CLUSTER_NAME, kafkaClusterTargetName, kafkaClusterSourceName, 1, false).done();

        //deploying example clients for checking if mm2 will mirror messages with headers
        KafkaClientsResource.consumerStrimzi(targetConsumerName, KafkaResources.plainBootstrapAddress(kafkaClusterTargetName), targetExampleTopic, MESSAGE_COUNT).done();
        KafkaClientsResource.producerStrimzi(sourceProducerName, KafkaResources.plainBootstrapAddress(kafkaClusterSourceName), sourceExampleTopic, MESSAGE_COUNT)
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
            .done();

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
        KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 1, 1).done();
        // Deploy target kafka
        KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 1, 1).done();

        KafkaMirrorMaker2Resource.kafkaMirrorMaker2(CLUSTER_NAME, kafkaClusterTargetName, kafkaClusterSourceName, 3, false).done();

        long oldObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getObservedGeneration();
        String mm2DepName = KafkaMirrorMaker2Resources.deploymentName(CLUSTER_NAME);
        List<String> mm2Pods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND);
        assertThat(mm2Pods.size(), is(3));

        LOGGER.info("Scaling MirrorMaker2 to zero");
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(CLUSTER_NAME, mm2 -> mm2.getSpec().setReplicas(0));

        PodUtils.waitForPodsReady(kubeClient().getDeploymentSelectors(mm2DepName), 0, true);

        mm2Pods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND);
        KafkaMirrorMaker2Status mm2Status = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        long actualObsGen = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getMetadata().getGeneration();

        assertThat(mm2Pods.size(), is(0));
        assertThat(mm2Status.getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(actualObsGen, is(not(oldObsGen)));
        assertNull(mm2Status.getUrl());
    }

    @BeforeAll
    void setup() throws Exception {
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
