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
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.ExecResult;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

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
    private final String userClusterName = "user-cluster-name";

    @ParallelTest
    void testUserWithNameMoreThan64Chars(ExtensionContext extensionContext) {
        String userWithLongName = "user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijk"; // 65 character username
        String userWithCorrectName = "user-with-correct-name" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopq"; // 64 character username
        String saslUserWithLongName = "sasl-user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdef"; // 65 character username

        // Create user with correct name
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(userClusterName, userWithCorrectName).build());

        KafkaUserUtils.waitUntilKafkaUserStatusConditionIsPresent(userWithCorrectName);

        Condition condition = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userWithCorrectName).get().getStatus().getConditions().get(0);

        verifyCRStatusCondition(condition, "True", Ready);

        // Create sasl user with long name, shouldn't fail
        resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(userClusterName, saslUserWithLongName).build());

        resourceManager.createResource(extensionContext, false, KafkaUserTemplates.defaultUser(userClusterName, userWithLongName)
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

    @ParallelTest
    @Tag(ACCEPTANCE)
    void testUpdateUser(ExtensionContext extensionContext) {
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(userClusterName, userName).build());

        String kafkaUserSecret = TestUtils.toJsonString(kubeClient().getSecret(userName));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['ca.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.key']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.name", equalTo(userName)));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));

        KafkaUser kUser = KafkaUserResource.kafkaUserClient().inNamespace(kubeClient().getNamespace()).withName(userName).get();
        String kafkaUserAsJson = TestUtils.toJsonString(kUser);

        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(userName)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo(Constants.TLS_LISTENER_DEFAULT_NAME)));

        long observedGeneration = KafkaUserResource.kafkaUserClient().inNamespace(kubeClient().getNamespace()).withName(userName).get().getStatus().getObservedGeneration();

        KafkaUserResource.replaceUserResource(userName, ku -> {
            ku.getMetadata().setResourceVersion(null);
            ku.getSpec().setAuthentication(new KafkaUserScramSha512ClientAuthentication());
        });

        KafkaUserUtils.waitForKafkaUserIncreaseObserverGeneration(observedGeneration, userName);
        KafkaUserUtils.waitForKafkaUserCreation(userName);

        kafkaUserSecret = TestUtils.toJsonString(kubeClient().getSecret(userName));
        assertThat(kafkaUserSecret, hasJsonPath("$.data.password", notNullValue()));

        kUser = Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(userName).get();
        kafkaUserAsJson = TestUtils.toJsonString(kUser);
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(userName)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo("scram-sha-512")));

        Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).delete(kUser);
        KafkaUserUtils.waitForKafkaUserDeletion(userName);
    }

    @Tag(SCALABILITY)
    @ParallelTest
    void testBigAmountOfScramShaUsers(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        createBigAmountOfUsers(extensionContext, clusterName, "SCRAM_SHA");
    }

    @Tag(SCALABILITY)
    @ParallelTest
    void testBigAmountOfTlsUsers(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        createBigAmountOfUsers(extensionContext, clusterName, "TLS");
    }

    @ParallelTest
    void testTlsUserWithQuotas(ExtensionContext extensionContext) {
        KafkaUser user = KafkaUserTemplates.tlsUser(userClusterName, "encrypted-arnost").build();

        resourceManager.createResource(extensionContext, user);

        testUserWithQuotas(extensionContext, user);
    }

    @ParallelTest
    void testScramUserWithQuotas(ExtensionContext extensionContext) {
        KafkaUser user = KafkaUserTemplates.scramShaUser(userClusterName, "scramed-arnost").build();

        resourceManager.createResource(extensionContext, user);

        testUserWithQuotas(extensionContext, user);
    }

    @ParallelTest
    void testUserTemplate(ExtensionContext extensionContext) {
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        String labelKey = "test-label-key";
        String labelValue = "test-label-value";
        String annotationKey = "test-annotation-key";
        String annotationValue = "test-annotation-value";

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(userClusterName, userName)
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewSecret()
                        .editOrNewMetadata()
                            .addToLabels(labelKey, labelValue)
                            .addToAnnotations(annotationKey, annotationValue)
                        .endMetadata()
                    .endSecret()
                .endTemplate()
            .endSpec()
            .build());

        Secret userSecret = kubeClient().getSecret(userName);
        assertThat(userSecret.getMetadata().getLabels().get(labelKey), is(labelValue));
        assertThat(userSecret.getMetadata().getAnnotations().get(annotationKey), is(annotationValue));
    }

    void testUserWithQuotas(ExtensionContext extensionContext, KafkaUser user) {
        String userName = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(user.getMetadata().getName()).get().getStatus().getUsername();

        Integer prodRate = 1111;
        Integer consRate = 2222;
        Integer reqPerc = 42;

        // Create user with correct name
        resourceManager.createResource(extensionContext, KafkaUserTemplates.userWithQuotas(user, prodRate, consRate, reqPerc).build());

        String command = "bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type users";
        LOGGER.debug("Command for kafka-configs.sh {}", command);

        ExecResult result = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(userClusterName, 0), "/bin/bash", "-c", command);
        assertThat(result.out().contains("Quota configs for user-principal '" + userName + "' are"), is(true));
        assertThat(result.out().contains("request_percentage=" + reqPerc), is(true));
        assertThat(result.out().contains("producer_byte_rate=" + prodRate), is(true));
        assertThat(result.out().contains("consumer_byte_rate=" + consRate), is(true));

        // delete user
        KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(user.getMetadata().getName()).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(user.getMetadata().getName());

        ExecResult resultAfterDelete = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(userClusterName, 0), "/bin/bash", "-c", command);
        assertThat(resultAfterDelete.out(), not(containsString(userName)));
        assertThat(resultAfterDelete.out(), not(containsString("request_percentage")));
        assertThat(resultAfterDelete.out(), not(containsString("producer_byte_rate")));
        assertThat(resultAfterDelete.out(), not(containsString("consumer_byte_rate")));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCreatingUsersWithSecretPrefix(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        String secretPrefix = "top-secret-";
        String tlsUserName = "encrypted-leopold";
        String scramShaUserName = "scramed-leopold";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
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
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        KafkaUser tlsUser = KafkaUserTemplates.tlsUser(clusterName, tlsUserName).build();
        KafkaUser scramShaUser =  KafkaUserTemplates.scramShaUser(clusterName, scramShaUserName).build();

        resourceManager.createResource(extensionContext, tlsUser);
        resourceManager.createResource(extensionContext, scramShaUser);

        LOGGER.info("Deploying KafkaClients pod for TLS listener");
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-tls-" + Constants.KAFKA_CLIENTS, true, Constants.TLS_LISTENER_DEFAULT_NAME, secretPrefix, tlsUser).build());
        String tlsKafkaClientsName = kubeClient().listPodsByPrefixInName(clusterName + "-tls-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        LOGGER.info("Deploying KafkaClients pod for PLAIN listener");
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, clusterName + "-plain-" + Constants.KAFKA_CLIENTS, true, Constants.PLAIN_LISTENER_DEFAULT_NAME, secretPrefix, scramShaUser).build());
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

    synchronized void createBigAmountOfUsers(ExtensionContext extensionContext, String clusterName, String typeOfUser) {

        int numberOfUsers = 100;

        for (int i = 0; i < numberOfUsers; i++) {
            String userName = "alisa" + i + "-" + clusterName;

            if (typeOfUser.equals("TLS")) {
                resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(userClusterName, userName).build());
            } else {
                resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(userClusterName, userName).build());
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

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(userClusterName, 1, 1).build());
    }
}
