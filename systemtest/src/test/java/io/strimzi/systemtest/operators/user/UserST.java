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
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
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

    private final String scraperName = userClusterName + "-" + Constants.SCRAPER_NAME;
    private String scraperPodName = "";

    @ParallelTest
    @KRaftNotSupported("Scram-sha is not supported by KRaft mode and is used in this test case")
    void testUserWithNameMoreThan64Chars(ExtensionContext extensionContext) {
        String userWithLongName = "user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijk"; // 65 character username
        String userWithCorrectName = "user-with-correct-name" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopq"; // 64 character username
        String saslUserWithLongName = "sasl-user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdef"; // 65 character username

        // Create user with correct name
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(namespace, userClusterName, userWithCorrectName).build());

        KafkaUserUtils.waitUntilKafkaUserStatusConditionIsPresent(namespace, userWithCorrectName);

        Condition condition = KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(userWithCorrectName).get().getStatus().getConditions().get(0);

        verifyCRStatusCondition(condition, "True", Ready);

        // Create sasl user with long name, shouldn't fail
        resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(namespace, userClusterName, saslUserWithLongName).build());

        resourceManager.createResource(extensionContext, false, KafkaUserTemplates.defaultUser(namespace, userClusterName, userWithLongName)
            .withNewSpec()
                .withNewKafkaUserTlsClientAuthentication()
                .endKafkaUserTlsClientAuthentication()
            .endSpec()
            .build());

        KafkaUserUtils.waitUntilKafkaUserStatusConditionIsPresent(namespace, userWithLongName);

        condition = KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(userWithLongName).get().getStatus().getConditions().get(0);

        verifyCRStatusCondition(condition,
                "only up to 64 characters",
                "ExecutionException", "True", NotReady);
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    @KRaftNotSupported("Scram-sha is not supported by KRaft mode and is used in this test case")
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

        Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(namespace).resource(kUser).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(namespace, userName);
    }

    @ParallelTest
    @KRaftNotSupported("Probably bug in Kafka - https://issues.apache.org/jira/browse/KAFKA-13964")
    void testTlsUserWithQuotas(ExtensionContext extensionContext) {
        KafkaUser user = KafkaUserTemplates.tlsUser(namespace, userClusterName, "encrypted-arnost").build();

        testUserWithQuotas(extensionContext, user);
    }

    @ParallelTest
    @KRaftNotSupported("Probably bug in Kafka - https://issues.apache.org/jira/browse/KAFKA-13964")
    void testTlsExternalUserWithQuotas(ExtensionContext extensionContext) {
        final String kafkaUserName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final KafkaUser tlsExternalUser = KafkaUserTemplates.tlsExternalUser(namespace, userClusterName, kafkaUserName).build();

        testUserWithQuotas(extensionContext, tlsExternalUser);
    }

    @ParallelTest
    @KRaftNotSupported("Scram-sha is not supported by KRaft mode and is used in this test case")
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

        TestUtils.waitFor("all KafkaUser " + userName + " attributes are present", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                String result = KafkaCmdClient.describeUserUsingPodCli(namespace, scraperPodName, KafkaResources.plainBootstrapAddress(userClusterName), userName);
                return result.contains("Quota configs for user-principal '" + userName + "' are") &&
                    result.contains("request_percentage=" + reqPerc) &&
                    result.contains("producer_byte_rate=" + prodRate) &&
                    result.contains("consumer_byte_rate=" + consRate) &&
                    result.contains("controller_mutation_rate=" + mutRate);
            });

        // delete user
        KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(user.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(namespace, user.getMetadata().getName());

        TestUtils.waitFor("all KafkaUser " + userName + " attributes will be cleaned", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                String resultAfterDelete = KafkaCmdClient.describeUserUsingPodCli(namespace, scraperPodName, KafkaResources.plainBootstrapAddress(userClusterName), userName);

                return
                    !resultAfterDelete.contains(userName) &&
                    !resultAfterDelete.contains("request_percentage") &&
                    !resultAfterDelete.contains("producer_byte_rate") &&
                    !resultAfterDelete.contains("consumer_byte_rate") &&
                    !resultAfterDelete.contains("controller_mutation_rate");
            });
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("Scram-sha is not supported by KRaft mode and is used in this test case")
    void testCreatingUsersWithSecretPrefix(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, namespace);

        final String secretPrefix = "top-secret-";
        final String tlsUserName = "encrypted-leopold";
        final String scramShaUserName = "scramed-leopold";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
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

        resourceManager.createResource(extensionContext,
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), tlsUserName).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getClusterName(), scramShaUserName).build()
        );

        Secret tlsSecret = kubeClient().getSecret(testStorage.getNamespaceName(), secretPrefix + tlsUserName);
        Secret scramShaSecret = kubeClient().getSecret(testStorage.getNamespaceName(), secretPrefix + scramShaUserName);

        LOGGER.info("Checking if user secrets with secret prefixes exists");
        assertNotNull(tlsSecret);
        assertNotNull(scramShaSecret);

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUserName(secretPrefix + tlsUserName)
            .build();

        LOGGER.info("Checking if TLS user is able to send messages");
        resourceManager.createResource(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        clients = new KafkaClientsBuilder(clients)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withUserName(secretPrefix + scramShaUserName)
            .build();

        LOGGER.info("Checking if SCRAM-SHA user is able to send messages");
        resourceManager.createResource(extensionContext, clients.producerScramShaPlainStrimzi(), clients.consumerScramShaPlainStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Checking owner reference - if the secret will be deleted when we delete KafkaUser");

        LOGGER.info("Deleting KafkaUser:{}", tlsUserName);
        KafkaUserResource.kafkaUserClient().inNamespace(testStorage.getNamespaceName()).withName(tlsUserName).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(testStorage.getNamespaceName(), tlsUserName);

        LOGGER.info("Deleting KafkaUser:{}", scramShaUserName);
        KafkaUserResource.kafkaUserClient().inNamespace(testStorage.getNamespaceName()).withName(scramShaUserName).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(testStorage.getNamespaceName(), scramShaUserName);

        LOGGER.info("Checking if secrets are deleted");
        SecretUtils.waitForSecretDeletion(testStorage.getNamespaceName(), tlsSecret.getMetadata().getName());
        SecretUtils.waitForSecretDeletion(testStorage.getNamespaceName(), scramShaSecret.getMetadata().getName());
        assertNull(kubeClient().getSecret(testStorage.getNamespaceName(), tlsSecret.getMetadata().getName()));
        assertNull(kubeClient().getSecret(testStorage.getNamespaceName(), scramShaSecret.getMetadata().getName()));
    }

    @ParallelNamespaceTest
    void testTlsExternalUser(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        final AclRule aclRule = new AclRuleBuilder()
            .withNewAclRuleTopicResource()
                .withName(topicName)
            .endAclRuleTopicResource()
            .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE)
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
                    .addToAcls(aclRule)
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

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(userClusterName, 1, 1)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .build(),
            ScraperTemplates.scraperPod(namespace, scraperName).build()
        );

        scraperPodName = kubeClient().listPodsByPrefixInName(namespace, scraperName).get(0).getMetadata().getName();
    }
}
