/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.user;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.AclOperation;
import io.strimzi.api.kafka.model.AclRule;
import io.strimzi.api.kafka.model.AclRuleBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.clients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.ExecResult;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
@ParallelSuite
class UserST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(UserST.class);

    private final String userClusterName = "user-cluster-name";
    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(UserST.class.getSimpleName()).stream().findFirst().get();

    @ParallelTest
    void testUserWithNameMoreThan64Chars(ExtensionContext extensionContext) {
        String userWithLongName = "user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijk"; // 65 character username
        String userWithCorrectName = "user-with-correct-name" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopq"; // 64 character username
        String saslUserWithLongName = "sasl-user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdef"; // 65 character username

        // Create user with correct name
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(userClusterName, userWithCorrectName)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .build());

        KafkaUserUtils.waitUntilKafkaUserStatusConditionIsPresent(namespace, userWithCorrectName);

        Condition condition = KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(userWithCorrectName).get().getStatus().getConditions().get(0);

        verifyCRStatusCondition(condition, "True", Ready);

        // Create sasl user with long name, shouldn't fail
        resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(userClusterName, saslUserWithLongName)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .build());

        resourceManager.createResource(extensionContext, false, KafkaUserTemplates.defaultUser(userClusterName, userWithLongName)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
                .withNewKafkaUserTlsClientAuthentication()
                .endKafkaUserTlsClientAuthentication()
            .endSpec()
            .build());

        KafkaUserUtils.waitUntilKafkaUserStatusConditionIsPresent(namespace, userWithLongName);

        condition = KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(userWithLongName).get().getStatus().getConditions().get(0);

        verifyCRStatusCondition(condition,
                "only up to 64 characters",
                "InvalidResourceException", "True", NotReady);
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    void testUpdateUser(ExtensionContext extensionContext) {
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(namespace, userClusterName, userName).build());

        String kafkaUserSecret = TestUtils.toJsonString(kubeClient(namespace).getSecret(userName));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['ca.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.key']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.name", equalTo(userName)));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.namespace", equalTo(namespace)));

        KafkaUser kUser = KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(userName).get();
        String kafkaUserAsJson = TestUtils.toJsonString(kUser);

        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(userName)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(namespace)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo(Constants.TLS_LISTENER_DEFAULT_NAME)));

        long observedGeneration = KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(userName).get().getStatus().getObservedGeneration();

        KafkaUserResource.replaceUserResourceInSpecificNamespace(userName, ku -> {
            ku.getMetadata().setResourceVersion(null);
            ku.getSpec().setAuthentication(new KafkaUserScramSha512ClientAuthentication());
        }, namespace);

        KafkaUserUtils.waitForKafkaUserIncreaseObserverGeneration(namespace, observedGeneration, userName);
        KafkaUserUtils.waitForKafkaUserCreation(namespace, userName);

        String anotherKafkaUserSecret = TestUtils.toJsonString(kubeClient(namespace).getSecret(namespace, userName));

        assertThat(anotherKafkaUserSecret, hasJsonPath("$.data.password", notNullValue()));

        kUser = Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(namespace).withName(userName).get();
        kafkaUserAsJson = TestUtils.toJsonString(kUser);
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(userName)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(namespace)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo("scram-sha-512")));

        Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(namespace).delete(kUser);
        KafkaUserUtils.waitForKafkaUserDeletion(userName);
    }

    @Tag(SCALABILITY)
    @IsolatedTest
    @Disabled("UserOperator create user operation timeouts, when creating many kafka users.")
    void testBigAmountOfScramShaUsers(ExtensionContext extensionContext) {
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        createBigAmountOfUsers(extensionContext, userName, "SCRAM_SHA", 100);
    }

    @Tag(SCALABILITY)
    @IsolatedTest
    @Disabled("UserOperator create user operation timeouts, when creating many kafka users.")
    void testAlterBigAmountOfScramShaUsers(ExtensionContext extensionContext) {
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        int numberOfUsers = 100;
        int producerRate = 1000;
        int consumerRate = 1500;
        int requestsPercentage = 42;
        double mutationRate = 3.0;

        createBigAmountOfUsers(extensionContext, userName, "SCRAM_SHA", numberOfUsers);
        alterBigAmountOfUsers(extensionContext, userName, "SCRAM_SHA", numberOfUsers,
                producerRate, consumerRate, requestsPercentage, mutationRate);
    }

    @Tag(SCALABILITY)
    @IsolatedTest
    @Disabled("UserOperator create user operation timeouts, when creating many kafka users.")
    void testBigAmountOfTlsUsers(ExtensionContext extensionContext) {
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        createBigAmountOfUsers(extensionContext, userName, "TLS", 100);
    }

    @Tag(SCALABILITY)
    @IsolatedTest
    @Disabled("UserOperator create user operation timeouts, when creating many kafka users.")
    void testAlterBigAmountOfTlsUsers(ExtensionContext extensionContext) {
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        int numberOfUsers = 100;
        int producerRate = 1000;
        int consumerRate = 1500;
        int requestsPercentage = 42;
        double mutationRate = 3.0;

        createBigAmountOfUsers(extensionContext, userName, "TLS", numberOfUsers);
        alterBigAmountOfUsers(extensionContext, userName, "TLS", numberOfUsers,
                producerRate, consumerRate, requestsPercentage, mutationRate);
    }

    @ParallelTest
    void testTlsUserWithQuotas(ExtensionContext extensionContext) {
        KafkaUser user = KafkaUserTemplates.tlsUser(namespace, userClusterName, "encrypted-arnost").build();

        testUserWithQuotas(extensionContext, user);
    }

    @ParallelTest
    void testTlsExternalUserWithQuotas(ExtensionContext extensionContext) {
        final String kafkaUserName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final KafkaUser tlsExternalUser = KafkaUserTemplates.tlsExternalUser(namespace, userClusterName, kafkaUserName).build();

        testUserWithQuotas(extensionContext, tlsExternalUser);
    }

    @ParallelTest
    void testScramUserWithQuotas(ExtensionContext extensionContext) {
        KafkaUser user = KafkaUserTemplates.scramShaUser(namespace, userClusterName, "scramed-arnost").build();

        testUserWithQuotas(extensionContext, user);
    }

    @ParallelTest
    void testUserTemplate(ExtensionContext extensionContext) {
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        String labelKey = "test-label-key";
        String labelValue = "test-label-value";
        String annotationKey = "test-annotation-key";
        String annotationValue = "test-annotation-value";

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(namespace, userClusterName, userName)
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

        Secret userSecret = kubeClient(namespace).getSecret(userName);
        assertThat(userSecret.getMetadata().getLabels().get(labelKey), is(labelValue));
        assertThat(userSecret.getMetadata().getAnnotations().get(annotationKey), is(annotationValue));
    }

    synchronized void testUserWithQuotas(ExtensionContext extensionContext, KafkaUser user) {
        final Integer prodRate = 1111;
        final Integer consRate = 2222;
        final Integer reqPerc = 42;
        final Double mutRate = 10d;

        // Create user with correct name
        resourceManager.createResource(extensionContext, KafkaUserTemplates.userWithQuotas(user, prodRate, consRate, reqPerc, mutRate)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .build());

        final String userName = KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(user.getMetadata().getName()).get().getStatus().getUsername();

        String command = "bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --user " + userName;
        LOGGER.debug("Command for kafka-configs.sh {}", command);

        ExecResult result = cmdKubeClient(namespace).execInPod(KafkaResources.kafkaPodName(userClusterName, 0), "/bin/bash", "-c", command);
        assertThat(result.out().contains("Quota configs for user-principal '" + userName + "' are"), is(true));
        assertThat(result.out().contains("request_percentage=" + reqPerc), is(true));
        assertThat(result.out().contains("producer_byte_rate=" + prodRate), is(true));
        assertThat(result.out().contains("consumer_byte_rate=" + consRate), is(true));
        assertThat(result.out().contains("controller_mutation_rate=" + mutRate), is(true));

        // delete user
        KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(user.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(namespace, user.getMetadata().getName());

        TestUtils.waitFor("all KafkaUser " + userName + " attributes will be cleaned", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                ExecResult resultAfterDelete = cmdKubeClient(namespace).execInPod(KafkaResources.kafkaPodName(userClusterName, 0), "/bin/bash", "-c", command);

                return
                    !resultAfterDelete.out().contains(userName) &&
                    !resultAfterDelete.out().contains("request_percentage") &&
                    !resultAfterDelete.out().contains("producer_byte_rate") &&
                    !resultAfterDelete.out().contains("consumer_byte_rate") &&
                    !resultAfterDelete.out().contains("controller_mutation_rate");
            });
    }

    @ParallelNamespaceTest
    void testCreatingUsersWithSecretPrefix(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        final String secretPrefix = "top-secret-";
        final String tlsUserName = "encrypted-leopold";
        final String scramShaUserName = "scramed-leopold";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .withNewKafkaListenerAuthenticationScramSha512Auth()
                                .endKafkaListenerAuthenticationScramSha512Auth()
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build())
                .endKafka()
                .editEntityOperator()
                    .editUserOperator()
                        .withSecretPrefix(secretPrefix)
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
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespaceName, true, clusterName + "-tls-" + Constants.KAFKA_CLIENTS, true, Constants.TLS_LISTENER_DEFAULT_NAME, secretPrefix, tlsUser).build());
        String tlsKafkaClientsName = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-tls-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        LOGGER.info("Deploying KafkaClients pod for PLAIN listener");
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(namespaceName, false, clusterName + "-plain-" + Constants.KAFKA_CLIENTS, true, Constants.PLAIN_LISTENER_DEFAULT_NAME, secretPrefix, scramShaUser).build());
        String plainKafkaClientsName = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, clusterName + "-plain-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        Secret tlsSecret = kubeClient(namespaceName).getSecret(secretPrefix + tlsUserName);
        Secret scramShaSecret = kubeClient(namespaceName).getSecret(secretPrefix + scramShaUserName);

        LOGGER.info("Checking if user secrets with secret prefixes exists");
        assertNotNull(tlsSecret);
        assertNotNull(scramShaSecret);

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(tlsKafkaClientsName)
            .withNamespaceName(namespaceName)
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
        KafkaUserResource.kafkaUserClient().inNamespace(namespaceName).withName(tlsUserName).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(namespaceName, tlsUserName);

        LOGGER.info("Deleting KafkaUser:{}", scramShaUserName);
        KafkaUserResource.kafkaUserClient().inNamespace(namespaceName).withName(scramShaUserName).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(namespaceName, scramShaUserName);

        LOGGER.info("Checking if secrets are deleted");
        SecretUtils.waitForSecretDeletion(namespaceName, tlsSecret.getMetadata().getName());
        SecretUtils.waitForSecretDeletion(namespaceName, scramShaSecret.getMetadata().getName());
        assertNull(kubeClient(namespaceName).getSecret(namespaceName, tlsSecret.getMetadata().getName()));
        assertNull(kubeClient(namespaceName).getSecret(namespaceName, scramShaSecret.getMetadata().getName()));
    }

    @ParallelNamespaceTest
    void testTlsExternalUser(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        final AclRule writeRule = new AclRuleBuilder()
            .withNewAclRuleTopicResource()
                .withName(topicName)
            .endAclRuleTopicResource()
            .withOperation(AclOperation.WRITE)
            .build();

        final AclRule describeRule = new AclRuleBuilder()
            .withNewAclRuleTopicResource()
                .withName(topicName)
            .endAclRuleTopicResource()
            .withOperation(AclOperation.DESCRIBE)
            .build();

        // exercise (a) - create Kafka cluster with support for authorization
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                .endKafka()
            .endSpec()
            .build());

        // quotas configuration
        final int prodRate = 1212;
        final int consRate = 2121;
        final int requestPerc = 21;
        final double mutRate = 5d;

        final KafkaUser tlsExternalUserWithQuotasAndAcls = KafkaUserTemplates.tlsExternalUser(namespaceName, clusterName, userName)
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addToAcls(writeRule, describeRule)
                .endKafkaUserAuthorizationSimple()
                .withNewQuotas()
                    .withConsumerByteRate(consRate)
                    .withProducerByteRate(prodRate)
                    .withRequestPercentage(requestPerc)
                    .withControllerMutationRate(mutRate)
                .endQuotas()
            .endSpec()
            .build();

        // exercise (b) - create the KafkaUser with tls external client authentication
        resourceManager.createResource(extensionContext, tlsExternalUserWithQuotasAndAcls);

        // verify (a) - that secrets are not generated and KafkaUser is created
        KafkaUserUtils.waitForKafkaUserReady(namespaceName, userName);
        assertThat(kubeClient().getSecret(namespaceName, userName), nullValue());

        // verify (b) -  if the operator has the right username in the status, that is what it also used in the ACLs and Quotas
        KafkaUser user = KafkaUserResource.kafkaUserClient().inNamespace(namespaceName).withName(userName).get();

        assertThat(user.getStatus().getUsername(), is("CN=" + userName));
    }

    synchronized void createBigAmountOfUsers(ExtensionContext extensionContext, String userName, String typeOfUser, int numberOfUsers) {
        LOGGER.info("Creating {} KafkaUsers", numberOfUsers);
        for (int i = 0; i < numberOfUsers; i++) {
            String userNameWithSuffix = userName + "-" + i;

            if (typeOfUser.equals("TLS")) {
                resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(userClusterName, userNameWithSuffix)
                    .editMetadata()
                        .withNamespace(namespace)
                    .endMetadata()
                    .build());
            } else {
                resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(userClusterName, userNameWithSuffix)
                    .editMetadata()
                        .withNamespace(namespace)
                    .endMetadata()
                    .build());
            }

            LOGGER.debug("Checking status of KafkaUser {}", userNameWithSuffix);
            Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(userNameWithSuffix).get()
                    .getStatus().getConditions().get(0);
            LOGGER.debug("KafkaUser condition status: {}", kafkaCondition.getStatus());
            LOGGER.debug("KafkaUser condition type: {}", kafkaCondition.getType());
            assertThat(kafkaCondition.getType(), is(Ready.toString()));
            LOGGER.debug("KafkaUser {} is in desired state: {}", userNameWithSuffix, kafkaCondition.getType());
        }
    }

    synchronized void alterBigAmountOfUsers(ExtensionContext extensionContext, String userName, String typeOfUser, int numberOfUsers,
                                            int producerRate, int consumerRate, int requestsPercentage, double mutationRate) {
        KafkaUserQuotas kuq = new KafkaUserQuotas();
        kuq.setConsumerByteRate(consumerRate);
        kuq.setProducerByteRate(producerRate);
        kuq.setRequestPercentage(requestsPercentage);
        kuq.setControllerMutationRate(mutationRate);

        LOGGER.info("Updating of existing KafkaUsers");
        for (int i = 0; i < numberOfUsers; i++) {
            String userNameWithSuffix = userName + "-" + i;
            if (typeOfUser.equals("TLS")) {
                resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(userClusterName, userNameWithSuffix)
                    .editMetadata()
                        .withNamespace(namespace)
                    .endMetadata()
                        .editSpec()
                            .withQuotas(kuq)
                        .endSpec()
                    .build());
            } else {
                resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(userClusterName, userNameWithSuffix)
                    .editMetadata()
                        .withNamespace(namespace)
                    .endMetadata()
                        .editSpec()
                            .withQuotas(kuq)
                        .endSpec()
                    .build());
            }

            LOGGER.info("[After update] Checking status of KafkaUser {}", userNameWithSuffix);
            Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(userNameWithSuffix).get()
                    .getStatus().getConditions().get(0);
            LOGGER.debug("KafkaUser condition status: {}", kafkaCondition.getStatus());
            LOGGER.debug("KafkaUser condition type: {}", kafkaCondition.getType());
            assertThat(kafkaCondition.getType(), is(Ready.toString()));
            LOGGER.debug("KafkaUser {} is in desired state: {}", userNameWithSuffix, kafkaCondition.getType());

            KafkaUserQuotas kuqAfter = KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(userNameWithSuffix).get().getSpec().getQuotas();
            LOGGER.debug("Check altered KafkaUser {} new quotas.", userNameWithSuffix);
            assertThat(kuqAfter.getRequestPercentage(), is(requestsPercentage));
            assertThat(kuqAfter.getConsumerByteRate(), is(consumerRate));
            assertThat(kuqAfter.getProducerByteRate(), is(producerRate));
            assertThat(kuqAfter.getControllerMutationRate(), is(mutationRate));
        }
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(userClusterName, 1, 1)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .build());
    }
}
