/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.CertSecretSource;
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
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
@Tag(MIRROR_MAKER2)
@Tag(CONNECT_COMPONENTS)
@Tag(INTERNAL_CLIENTS_USED)
class MirrorMaker2ST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(MirrorMaker2ST.class);
    public static final String NAMESPACE = "mirrormaker2-cluster-test";

    private static final String MIRRORMAKER2_TOPIC_NAME = "mirrormaker2-topic-example";
    private final int messagesCount = 200;

    private String kafkaClusterSourceName = CLUSTER_NAME + "-source";
    private String kafkaClusterTargetName = CLUSTER_NAME + "-target";

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    void testMirrorMaker2() {
        String availabilityTopicSourceName = "availability-topic-source-" + rng.nextInt(Integer.MAX_VALUE);
        String availabilityTopicTargetName = "availability-topic-target-" + rng.nextInt(Integer.MAX_VALUE);

        Map<String, Object> expectedConfig = StUtils.loadProperties("group.id=mirrormaker2-cluster\n" +
                "key.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
                "value.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n" +
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
        String topicSourceNameMirrored = kafkaClusterSourceName + "." + availabilityTopicSourceName;

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
            .withTopicName(availabilityTopicSourceName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(kafkaClusterSourceName)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .build();

        // Check brokers availability
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        internalKafkaClient.setTopicName(availabilityTopicTargetName);
        internalKafkaClient.setClusterName(kafkaClusterTargetName);
        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );
        
        KafkaMirrorMaker2Resource.kafkaMirrorMaker2(CLUSTER_NAME, kafkaClusterTargetName, kafkaClusterSourceName, 1, false).done();
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

        internalKafkaClient.setTopicName(topicSourceName);
        internalKafkaClient.setClusterName(kafkaClusterSourceName);
        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        int sent = internalKafkaClient.sendMessagesPlain();

        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesPlain()
        );

        internalKafkaClient.setTopicName(topicTargetName);
        internalKafkaClient.setClusterName(kafkaClusterTargetName);
        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesPlain()
        );

        internalKafkaClient.setTopicName(topicSourceNameMirrored);
        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        // Check that mm2 mirror automatically created topic
        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesPlain()
        );

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
        String availabilityTopicSourceName = "availability-topic-source-" + rng.nextInt(Integer.MAX_VALUE);
        String availabilityTopicTargetName = "availability-topic-target-" + rng.nextInt(Integer.MAX_VALUE);
        String topicSourceNameMirrored = kafkaClusterSourceName + "." + availabilityTopicSourceName;
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
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .build();

        // Check brokers availability
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient.setTopicName("my-topic-test-2");
        internalKafkaClient.setClusterName(kafkaClusterTargetName);
        internalKafkaClient.setKafkaUsername(userTarget.getMetadata().getName());

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

        internalKafkaClient.setTopicName(topicSourceName);
        internalKafkaClient.setClusterName(kafkaClusterSourceName);
        internalKafkaClient.setKafkaUsername(userSource.getMetadata().getName());

        int sent = internalKafkaClient.sendMessagesTls();

        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient.setTopicName(topicTargetName);
        internalKafkaClient.setClusterName(kafkaClusterTargetName);
        internalKafkaClient.setKafkaUsername(userTarget.getMetadata().getName());

        internalKafkaClient.checkProducedAndConsumedMessages(
            sent,
            internalKafkaClient.receiveMessagesTls()
        );

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
        String availabilityTopicSourceName = "availability-topic-source-" + rng.nextInt(Integer.MAX_VALUE);
        String availabilityTopicTargetName = "availability-topic-target-" + rng.nextInt(Integer.MAX_VALUE);
        String topicSourceNameMirrored = kafkaClusterSourceName + "." + availabilityTopicSourceName;
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
        SecretUtils.waitForSecretReady(kafkaUserSource);

        // Create Kafka user for target cluster
        KafkaUser userTarget = KafkaUserResource.scramShaUser(kafkaClusterTargetName, kafkaUserTarget).done();
        SecretUtils.waitForSecretReady(kafkaUserTarget);

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
            .withTopicName(availabilityTopicSourceName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(kafkaClusterSourceName)
            .withKafkaUsername(userSource.getMetadata().getName())
            .withMessageCount(messagesCount)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .build();

        // Check brokers availability
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient.setTopicName(availabilityTopicTargetName);
        internalKafkaClient.setClusterName(kafkaClusterTargetName);
        internalKafkaClient.setKafkaUsername(userTarget.getMetadata().getName());
        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

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

        // Deploy topic
        KafkaTopicResource.topic(kafkaClusterSourceName, topicSourceName, 3).done();

        internalKafkaClient.setTopicName(topicSourceName);
        internalKafkaClient.setClusterName(kafkaClusterSourceName);
        internalKafkaClient.setKafkaUsername(userSource.getMetadata().getName());
        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        int sent = internalKafkaClient.sendMessagesTls();

        internalKafkaClient.setTopicName(topicTargetName);
        internalKafkaClient.setClusterName(kafkaClusterTargetName);
        internalKafkaClient.setKafkaUsername(userTarget.getMetadata().getName());

        TestUtils.waitFor("Waiting for Mirror Maker 2 will copy messages from " + kafkaClusterSourceName + " to " + kafkaClusterTargetName,
            Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, Constants.TIMEOUT_FOR_MIRROR_MAKER_COPY_MESSAGES_BETWEEN_BROKERS,
            () -> {
                internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));
                return sent == internalKafkaClient.receiveMessagesTls();
            });

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
        String mirrormaker2ImageName = PodUtils.getFirstContainerImageNameFromPod(kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, "KafkaMirrorMaker2").
                get(0).getMetadata().getName());

        String mirrormaker2Version = Crds.kafkaMirrorMaker2Operation(kubeClient().getClient()).inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getVersion();
        if (mirrormaker2Version == null) {
            mirrormaker2Version = Environment.ST_KAFKA_VERSION;
        }

        assertThat(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_MIRROR_MAKER_2_IMAGE_MAP)).get(mirrormaker2Version), is(mirrormaker2ImageName));
        LOGGER.info("Docker images verified");
    }


    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        cluster.setNamespace(NAMESPACE);
        KubernetesResource.clusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_SHORT).done();
    }
}