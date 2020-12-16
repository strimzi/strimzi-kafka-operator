/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.user;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.ExecResult;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SCALABILITY;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
class UserST extends AbstractST {

    public static final String NAMESPACE = "user-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(UserST.class);

    @Test
    void testUserWithNameMoreThan64Chars() {
        String userWithLongName = "user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijk"; // 65 character username
        String userWithCorrectName = "user-with-correct-name" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopq"; // 64 character username
        String saslUserWithLongName = "sasl-user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdef"; // 65 character username

        // Create user with correct name
        KafkaUserResource.tlsUser(CLUSTER_NAME, userWithCorrectName).done();

        KafkaUserUtils.waitUntilKafkaUserStatusConditionIsPresent(userWithCorrectName);

        Condition condition = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userWithCorrectName).get().getStatus().getConditions().get(0);

        verifyCRStatusCondition(condition, "True", Ready);

        // Create sasl user with long name, shouldn't fail
        KafkaUserResource.scramShaUser(CLUSTER_NAME, saslUserWithLongName).done();

        KafkaUserResource.kafkaUserWithoutWait(KafkaUserResource.defaultUser(CLUSTER_NAME, userWithLongName)
            .withNewSpec()
                .withNewKafkaUserTlsClientAuthentication()
                .endKafkaUserTlsClientAuthentication()
            .endSpec()
            .build());

        KafkaUserUtils.waitUntilKafkaUserStatusConditionIsPresent(userWithLongName);

        condition = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userWithLongName).get().getStatus().getConditions().get(0);

        verifyCRStatusCondition(condition,
                "only up to 64 characters",
                "InvalidResourceException", "True", NotReady);
    }

    @Test
    @Tag(ACCEPTANCE)
    void testUpdateUser() {
        KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME).done();

        String kafkaUserSecret = TestUtils.toJsonString(kubeClient().getSecret(USER_NAME));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['ca.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.key']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.name", equalTo(USER_NAME)));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));

        KafkaUser kUser = KafkaUserResource.kafkaUserClient().inNamespace(kubeClient().getNamespace()).withName(USER_NAME).get();
        String kafkaUserAsJson = TestUtils.toJsonString(kUser);

        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(USER_NAME)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo(Constants.TLS_LISTENER_DEFAULT_NAME)));

        long observedGeneration = KafkaUserResource.kafkaUserClient().inNamespace(kubeClient().getNamespace()).withName(USER_NAME).get().getStatus().getObservedGeneration();

        KafkaUserResource.replaceUserResource(USER_NAME, ku -> {
            ku.getMetadata().setResourceVersion(null);
            ku.getSpec().setAuthentication(new KafkaUserScramSha512ClientAuthentication());
        });

        KafkaUserUtils.waitForKafkaUserIncreaseObserverGeneration(observedGeneration, USER_NAME);
        KafkaUserUtils.waitForKafkaUserCreation(USER_NAME);

        kafkaUserSecret = TestUtils.toJsonString(kubeClient().getSecret(USER_NAME));
        assertThat(kafkaUserSecret, hasJsonPath("$.data.password", notNullValue()));

        kUser = Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(USER_NAME).get();
        kafkaUserAsJson = TestUtils.toJsonString(kUser);
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(USER_NAME)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo("scram-sha-512")));

        Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).delete(kUser);
        KafkaUserUtils.waitForKafkaUserDeletion(USER_NAME);
    }

    @Tag(SCALABILITY)
    @Test
    void testBigAmountOfScramShaUsers() {
        createBigAmountOfUsers("SCRAM_SHA");
    }

    @Tag(SCALABILITY)
    @Test
    void testBigAmountOfTlsUsers() {
        createBigAmountOfUsers("TLS");
    }

    @Test
    void testTlsUserWithQuotas() {
        testUserWithQuotas(KafkaUserResource.tlsUser(CLUSTER_NAME, "encrypted-arnost").done());
    }

    @Test
    void testScramUserWithQuotas() {
        testUserWithQuotas(KafkaUserResource.scramShaUser(CLUSTER_NAME, "scramed-arnost").done());
    }

    @Test
    void testUserTemplate() {
        String labelKey = "test-label-key";
        String labelValue = "test-label-value";
        String annotationKey = "test-annotation-key";
        String annotationValue = "test-annotation-value";
        KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME)
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewSecret()
                        .editOrNewMetadata()
                            .addToLabels(labelKey, labelValue)
                            .addToAnnotations(annotationKey, annotationValue)
                        .endMetadata()
                    .endSecret()
                .endTemplate()
            .endSpec().done();

        Secret userSecret = kubeClient().getSecret(USER_NAME);
        assertThat(userSecret.getMetadata().getLabels().get(labelKey), is(labelValue));
        assertThat(userSecret.getMetadata().getAnnotations().get(annotationKey), is(annotationValue));
    }

    void testUserWithQuotas(KafkaUser user) {
        String userName = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(user.getMetadata().getName()).get().getStatus().getUsername();

        Integer prodRate = 1111;
        Integer consRate = 2222;
        Integer reqPerc = 42;

        // Create user with correct name
        KafkaUserResource.userWithQuota(user, prodRate, consRate, reqPerc).done();

        String command = "bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type users";
        LOGGER.debug("Command for kafka-configs.sh {}", command);

        ExecResult result = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", command);
        assertThat(result.out().contains("Quota configs for user-principal '" + userName + "' are"), is(true));
        assertThat(result.out().contains("request_percentage=" + reqPerc), is(true));
        assertThat(result.out().contains("producer_byte_rate=" + prodRate), is(true));
        assertThat(result.out().contains("consumer_byte_rate=" + consRate), is(true));

        String zkListCommand = "sh /opt/kafka/bin/zookeeper-shell.sh localhost:12181 <<< 'ls /config/users'";

        TestUtils.waitFor("user " + userName + " will be available in Zookeeper", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            ExecResult zkResult = cmdKubeClient().execInPod(KafkaResources.zookeeperPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", zkListCommand);
            try {
                return zkResult.out().contains(URLEncoder.encode(userName, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("Failed to encode username", e);
            }
        });

        // delete user
        KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(user.getMetadata().getName()).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(user.getMetadata().getName());

        ExecResult resultAfterDelete = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", command);
        assertThat(resultAfterDelete.out(), not(containsString(userName)));

        TestUtils.waitFor("user " + userName + " will be deleted from Zookeeper", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            ExecResult zkResult = cmdKubeClient().execInPod(KafkaResources.zookeeperPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", zkListCommand);
            try {
                return !zkResult.out().contains(URLEncoder.encode(userName, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("Failed to encode username", e);
            }
        });
    }

    @Test
    void testCreatingUsersWithSecretPrefix() {
        String clusterName = "second-cluster";
        String secretPrefix = "top-secret-";
        String tlsUserName = "encrypted-leopold";
        String scramShaUserName = "scramed-leopold";

        KafkaResource.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
                .editEntityOperator()
                    .editUserOperator()
                        .withNewSecretPrefix(secretPrefix)
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .done();

        KafkaTopicResource.topic(clusterName, TOPIC_NAME).done();
        KafkaUser tlsUser = KafkaUserResource.tlsUser(clusterName, tlsUserName).done();
        KafkaUser scramShaUser = KafkaUserResource.scramShaUser(clusterName, scramShaUserName).done();

        LOGGER.info("Deploying KafkaClients pod for TLS listener");
        KafkaClientsResource.deployKafkaClients(true, clusterName + "-tls-" + Constants.KAFKA_CLIENTS, true, Constants.TLS_LISTENER_DEFAULT_NAME, secretPrefix, tlsUser).done();
        String tlsKafkaClientsName = kubeClient().listPodsByPrefixInName(clusterName + "-tls-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        LOGGER.info("Deploying KafkaClients pod for PLAIN listener");
        KafkaClientsResource.deployKafkaClients(false, clusterName + "-plain-" + Constants.KAFKA_CLIENTS, true, Constants.PLAIN_LISTENER_DEFAULT_NAME, secretPrefix, scramShaUser).done();
        String plainKafkaClientsName = kubeClient().listPodsByPrefixInName(clusterName + "-plain-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        Secret tlsSecret = kubeClient().getSecret(secretPrefix + tlsUserName);
        Secret scramShaSecret = kubeClient().getSecret(secretPrefix + scramShaUserName);

        LOGGER.info("Checking if user secrets with secret prefixes exists");
        assertNotNull(tlsSecret);
        assertNotNull(scramShaSecret);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(tlsKafkaClientsName)
            .withNamespaceName(NAMESPACE)
            .withTopicName(TOPIC_NAME)
            .withKafkaUsername(tlsUserName)
            .withSecurityProtocol(SecurityProtocol.SASL_SSL)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecretPrefix(secretPrefix)
            .build();

        LOGGER.info("Checking if TLS user is able to send messages");
        internalKafkaClient.assertSentAndReceivedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withUsingPodName(plainKafkaClientsName)
            .withSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .withKafkaUsername(scramShaUserName)
            .build();

        LOGGER.info("Checking if SCRAM-SHA user is able to send messages");
        internalKafkaClient.assertSentAndReceivedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        LOGGER.info("Checking owner reference - if the secret will be deleted when we delete KafkaUser");

        LOGGER.info("Deleting KafkaUser:{}", tlsUserName);
        KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(tlsUserName).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(tlsUserName);

        LOGGER.info("Deleting KafkaUser:{}", scramShaUserName);
        KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(scramShaUserName).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(scramShaUserName);

        LOGGER.info("Checking if secrets are deleted");
        SecretUtils.waitForSecretDeletion(tlsSecret.getMetadata().getName());
        SecretUtils.waitForSecretDeletion(scramShaSecret.getMetadata().getName());
        assertNull(kubeClient().getSecret(tlsSecret.getMetadata().getName()));
        assertNull(kubeClient().getSecret(scramShaSecret.getMetadata().getName()));
    }

    void createBigAmountOfUsers(String typeOfUser) {

        int numberOfUsers = 100;

        for (int i = 0; i < numberOfUsers; i++) {
            String userName = "alisa" + i;

            if (typeOfUser.equals("TLS")) {
                KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();
            } else {
                KafkaUserResource.scramShaUser(CLUSTER_NAME, userName).done();
            }

            LOGGER.info("Checking status of KafkaUser {}", userName);
            Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userName).get()
                    .getStatus().getConditions().get(0);
            LOGGER.info("KafkaUser condition status: {}", kafkaCondition.getStatus());
            LOGGER.info("KafkaUser condition type: {}", kafkaCondition.getType());
            assertThat(kafkaCondition.getType(), is(Ready.toString()));
            LOGGER.info("KafkaUser {} is in desired state: {}", userName, kafkaCondition.getType());
        }
    }

    private void deployTestSpecificResources() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1).done();
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        deployTestSpecificResources();
    }
}
