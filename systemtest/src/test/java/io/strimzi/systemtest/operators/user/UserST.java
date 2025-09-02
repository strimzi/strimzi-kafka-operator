/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.user;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Secret;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimpleBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.user.KafkaUserTlsClientAuthentication;
import io.strimzi.api.kafka.model.user.KafkaUserTlsExternalClientAuthentication;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.api.kafka.model.user.acl.AclResourcePatternType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.TestTags.ACCEPTANCE;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test suite for various Kafka User operations and configurations."),
    beforeTestSteps = {
        @Step(value = "Initialize shared test storage and deploy Kafka cluster with necessary configuration.", expected = "Kafka cluster and scraper pod are deployed and ready for testing.")
    },
    labels = {
        @Label(TestDocsLabels.USER_OPERATOR)
    }
)
class UserST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(UserST.class);

    private TestStorage sharedTestStorage;
    private String scraperPodName = "";

    @TestDoc(
        description = @Desc("Verifies that Kafka users with names longer than 64 characters are rejected, while users with valid names are accepted."),
        steps = {
            @Step(value = "Create Kafka user with valid name (64 characters).", expected = "User is successfully created and becomes ready."),
            @Step(value = "Create SASL user with long name (65 characters).", expected = "SASL user is created successfully since SASL users support longer names."),
            @Step(value = "Attempt to create TLS user with long name (65 characters).", expected = "User creation fails with validation error."),
            @Step(value = "Verify error condition and message.", expected = "Error condition indicates username limitation and provides an appropriate error message.")
        },
        labels = {
            @Label(TestDocsLabels.USER_OPERATOR)
        }
    )
    @ParallelTest
    void testUserWithNameMoreThan64Chars() {
        String userWithLongName = "user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijk"; // 65 character username
        String userWithCorrectName = "user-with-correct-name" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopq"; // 64 character username
        String saslUserWithLongName = "sasl-user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdef"; // 65 character username

        // Create user with correct name
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, userWithCorrectName, sharedTestStorage.getClusterName()).build());
        KafkaUserUtils.waitForKafkaUserReady(Environment.TEST_SUITE_NAMESPACE, userWithCorrectName);

        // Create sasl user with long name, shouldn't fail
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.scramShaUser(Environment.TEST_SUITE_NAMESPACE, saslUserWithLongName, sharedTestStorage.getClusterName()).build());

        KubeResourceManager.get().createResourceWithoutWait(KafkaUserTemplates.defaultUser(Environment.TEST_SUITE_NAMESPACE, userWithLongName, sharedTestStorage.getClusterName())
            .withNewSpec()
                .withNewKafkaUserTlsClientAuthentication()
                .endKafkaUserTlsClientAuthentication()
            .endSpec()
            .build());

        KafkaUserUtils.waitUntilKafkaUserStatusConditionIsPresent(Environment.TEST_SUITE_NAMESPACE, userWithLongName);
        KafkaUserUtils.waitForKafkaUserNotReady(Environment.TEST_SUITE_NAMESPACE, userWithLongName);

        final Condition condition = CrdClients.kafkaUserClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(userWithLongName).get().getStatus().getConditions().get(0);
        assertThat(condition.getMessage(), containsString("only up to 64 characters"));
        assertThat(condition.getReason(), CoreMatchers.is("ExecutionException"));
    }

    @TestDoc(
        description = @Desc("Verifies updating a Kafka user from TLS to SCRAM-SHA-512 authentication and validates user secret contents."),
        steps = {
            @Step(value = "Create TLS Kafka user.", expected = "User is created with TLS authentication and secret contains TLS certificates."),
            @Step(value = "Verify TLS user secret contents.", expected = "Secret contains `ca.crt`, `user.crt`, and `user.key` fields."),
            @Step(value = "Test message sending and receiving with TLS user.", expected = "Messages are successfully sent and received."),
            @Step(value = "Update user authentication to SCRAM-SHA-512.", expected = "User authentication is updated successfully."),
            @Step(value = "Verify SCRAM-SHA-512 user secret contents.", expected = "Secret contains SCRAM-SHA-512 `password` field and TLS certificates are removed."),
            @Step(value = "Test message sending and receiving with SCRAM-SHA-512 user.", expected = "Messages are successfully sent and received with SCRAM-SHA-512 authentication.")
        },
        labels = {
            @Label(TestDocsLabels.USER_OPERATOR)
        }
    )
    @ParallelTest
    @Tag(ACCEPTANCE)
    void testUpdateUser() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaUsername(), sharedTestStorage.getClusterName()).build());

        String kafkaUserSecret = ReadWriteUtils.writeObjectToJsonString(KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getKafkaUsername()).get());
        assertThat(kafkaUserSecret, hasJsonPath("$.data['ca.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.key']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.name", equalTo(testStorage.getKafkaUsername())));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.namespace", equalTo(Environment.TEST_SUITE_NAMESPACE)));

        KafkaUser kUser = CrdClients.kafkaUserClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getKafkaUsername()).get();
        String kafkaUserAsJson = ReadWriteUtils.writeObjectToJsonString(kUser);

        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(testStorage.getKafkaUsername())));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(Environment.TEST_SUITE_NAMESPACE)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo(TestConstants.TLS_LISTENER_DEFAULT_NAME)));

        final long observedGeneration = CrdClients.kafkaUserClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getKafkaUsername()).get().getStatus().getObservedGeneration();

        // Send and receive messages
        KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(sharedTestStorage.getClusterName()))
            .withUsername(testStorage.getKafkaUsername())
            .build();

        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerTlsStrimzi(sharedTestStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(sharedTestStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        KafkaUserUtils.replace(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaUsername(), ku -> {
            ku.getSpec().setAuthentication(new KafkaUserScramSha512ClientAuthentication());
        });

        KafkaUserUtils.waitForKafkaUserIncreaseObserverGeneration(Environment.TEST_SUITE_NAMESPACE, observedGeneration, testStorage.getKafkaUsername());
        KafkaUserUtils.waitForKafkaUserCreation(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaUsername());

        String anotherKafkaUserSecret = ReadWriteUtils.writeObjectToJsonString(KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getKafkaUsername()).get());

        assertThat(anotherKafkaUserSecret, hasJsonPath("$.data.password", notNullValue()));

        kUser = CrdClients.kafkaUserClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getKafkaUsername()).get();
        kafkaUserAsJson = ReadWriteUtils.writeObjectToJsonString(kUser);
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(testStorage.getKafkaUsername())));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(Environment.TEST_SUITE_NAMESPACE)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo("scram-sha-512")));

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withBootstrapAddress(KafkaResources.bootstrapServiceName(sharedTestStorage.getClusterName()) + ":9095")
            .build();

        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerScramShaTlsStrimzi(sharedTestStorage.getClusterName()), kafkaClients.consumerScramShaTlsStrimzi(sharedTestStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @TestDoc(
        description = @Desc("Verifies that TLS authenticated Kafka users can be configured with quotas."),
        steps = {
            @Step(value = "Create TLS user with quota configuration.", expected = "User is created successfully with TLS authentication and quota settings applied.")
        },
        labels = {
            @Label(TestDocsLabels.USER_OPERATOR)
        }
    )
    @ParallelTest
    void testTlsUserWithQuotas() {
        KafkaUser user = KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, "encrypted-arnost", sharedTestStorage.getClusterName()).build();

        testUserWithQuotas(user);
    }

    @TestDoc(
        description = @Desc("Verifies that Kafka users authenticated with external TLS can be configured with quotas."),
        steps = {
            @Step(value = "Create TLS external user with quota configuration.", expected = "User is created successfully with TLS external authentication and quota settings applied.")
        },
        labels = {
            @Label(TestDocsLabels.USER_OPERATOR)
        }
    )
    @ParallelTest
    void testTlsExternalUserWithQuotas() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final KafkaUser tlsExternalUser = KafkaUserTemplates.tlsExternalUser(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaUsername(), sharedTestStorage.getClusterName()).build();

        testUserWithQuotas(tlsExternalUser);
    }

    @TestDoc(
        description = @Desc("Verifies that SCRAM-SHA-512 authenticated Kafka users can be configured with quotas."),
        steps = {
            @Step(value = "Create SCRAM-SHA-512 user with quota configuration.", expected = "User is created successfully with SCRAM-SHA-512 authentication and quota settings applied.")
        },
        labels = {
            @Label(TestDocsLabels.USER_OPERATOR)
        }
    )
    @ParallelTest
    void testScramUserWithQuotas() {
        KafkaUser user = KafkaUserTemplates.scramShaUser(Environment.TEST_SUITE_NAMESPACE, "scramed-arnost", sharedTestStorage.getClusterName()).build();
        testUserWithQuotas(user);
    }
    void testUserWithQuotas(KafkaUser user) {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final Integer prodRate = 1111;
        final Integer consRate = 2222;
        final Integer reqPerc = 42;
        final Double mutRate = 10d;

        // Create user with correct name
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.userWithQuotas(user, prodRate, consRate, reqPerc, mutRate).build());

        final String userName = user.getMetadata().getName();
        final String statusUserName = CrdClients.kafkaUserClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(userName).get().getStatus().getUsername();

        TestUtils.waitFor("all KafkaUser " + userName + " attributes are present", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                String result = KafkaCmdClient.describeUserUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName()), statusUserName);
                return result.contains("Quota configs for user-principal '" + statusUserName + "' are") &&
                    result.contains("request_percentage=" + reqPerc) &&
                    result.contains("producer_byte_rate=" + prodRate) &&
                    result.contains("consumer_byte_rate=" + consRate) &&
                    result.contains("controller_mutation_rate=" + mutRate);
            });

        final KafkaClients kafkaClients = ClientUtils.getInstantScramShaClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(sharedTestStorage.getClusterName()))
            .withUsername(userName)
            .build();

        if (user.getSpec().getAuthentication() instanceof KafkaUserScramSha512ClientAuthentication) {
            kafkaClients.setBootstrapAddress(KafkaResources.bootstrapServiceName(sharedTestStorage.getClusterName()) + ":9095");

            KubeResourceManager.get().createResourceWithWait(kafkaClients.producerScramShaTlsStrimzi(sharedTestStorage.getClusterName()),
                                                                     kafkaClients.consumerScramShaTlsStrimzi(sharedTestStorage.getClusterName()));

        } else if (user.getSpec().getAuthentication() instanceof KafkaUserTlsClientAuthentication) {
            KubeResourceManager.get().createResourceWithWait(kafkaClients.producerTlsStrimzi(sharedTestStorage.getClusterName()),
                                                                     kafkaClients.consumerTlsStrimzi(sharedTestStorage.getClusterName()));

        } else if (user.getSpec().getAuthentication() instanceof KafkaUserTlsExternalClientAuthentication) {
            SecretUtils.createExternalTlsUserSecret(testStorage.getNamespaceName(), userName, sharedTestStorage.getClusterName());

            KubeResourceManager.get().createResourceWithWait(kafkaClients.producerTlsStrimzi(sharedTestStorage.getClusterName()),
                                                                     kafkaClients.consumerTlsStrimzi(sharedTestStorage.getClusterName()));
        }

        ClientUtils.waitForInstantClientSuccess(testStorage);

        // delete user
        CrdClients.kafkaUserClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(userName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(Environment.TEST_SUITE_NAMESPACE, userName);

        TestUtils.waitFor("all attributes of KafkaUser: " + statusUserName + " to be cleaned", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                String resultAfterDelete = KafkaCmdClient.describeUserUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName()), statusUserName);

                return
                    !resultAfterDelete.contains(statusUserName) &&
                    !resultAfterDelete.contains("request_percentage") &&
                    !resultAfterDelete.contains("producer_byte_rate") &&
                    !resultAfterDelete.contains("consumer_byte_rate") &&
                    !resultAfterDelete.contains("controller_mutation_rate");
            });
    }

    @TestDoc(
        description = @Desc("Verifies creating Kafka users with custom secret prefixes for organizing user secrets."),
        steps = {
            @Step(value = "Configure cluster operator with custom secret prefix.", expected = "Cluster operator is reconfigured with the specified secret prefix."),
            @Step(value = "Create TLS and SCRAM-SHA-512 users.", expected = "Users are created successfully with TLS and SCRAM-SHA-512 authentication."),
            @Step(value = "Verify user secrets are created with proper prefix.", expected = "User secrets are created with the configured prefix in their names."),
            @Step(value = "Test message sending and receiving.", expected = "Messages are successfully sent and received using both authentication methods."),
            @Step(value = "Update users and verify secret updates.", expected = "User updates are reflected in the prefixed secrets."),
            @Step(value = "Delete users and verify cleanup.", expected = "User deletion removes the prefixed secrets properly.")
        },
        labels = {
            @Label(TestDocsLabels.USER_OPERATOR)
        }
    )
    @ParallelNamespaceTest
    void testCreatingUsersWithSecretPrefix() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final String secretPrefix = "top-secret-";
        final String tlsUserName = "encrypted-leopold";
        final String scramShaUserName = "scramed-leopold";

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .withNewKafkaListenerAuthenticationScramSha512Auth()
                                .endKafkaListenerAuthenticationScramSha512Auth()
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
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

        KubeResourceManager.get().createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), tlsUserName, testStorage.getClusterName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), scramShaUserName, testStorage.getClusterName()).build()
        );

        Secret tlsSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(secretPrefix + tlsUserName).get();
        Secret scramShaSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(secretPrefix + scramShaUserName).get();

        LOGGER.info("Checking for existing user Secrets with prefix: {}", secretPrefix);
        assertNotNull(tlsSecret);
        assertNotNull(scramShaSecret);

        KafkaClients clients = ClientUtils.getInstantTlsClientBuilder(testStorage)
            .withUsername(secretPrefix + tlsUserName)
            .build();

        LOGGER.info("Checking if TLS user is able to send messages");
        KubeResourceManager.get().createResourceWithWait(clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);


        clients = ClientUtils.getInstantScramShaOverPlainClientBuilder(testStorage)
            .withUsername(secretPrefix + scramShaUserName)
            .build();

        LOGGER.info("Checking if SCRAM-SHA-512 user is able to send messages");
        KubeResourceManager.get().createResourceWithWait(clients.producerScramShaPlainStrimzi(), clients.consumerScramShaPlainStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Checking owner reference - if the Secret will be deleted when we delete KafkaUser");

        LOGGER.info("Deleting KafkaUser: {}/{}", testStorage.getNamespaceName(), tlsUserName);
        CrdClients.kafkaUserClient().inNamespace(testStorage.getNamespaceName()).withName(tlsUserName).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(testStorage.getNamespaceName(), tlsUserName);

        LOGGER.info("Deleting KafkaUser: {}/{}", testStorage.getNamespaceName(), scramShaUserName);
        CrdClients.kafkaUserClient().inNamespace(testStorage.getNamespaceName()).withName(scramShaUserName).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(testStorage.getNamespaceName(), scramShaUserName);

        LOGGER.info("Checking if Secrets are deleted");
        SecretUtils.waitForSecretDeletion(testStorage.getNamespaceName(), tlsSecret.getMetadata().getName());
        SecretUtils.waitForSecretDeletion(testStorage.getNamespaceName(), scramShaSecret.getMetadata().getName());
        assertNull(KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(tlsSecret.getMetadata().getName()).get());
        assertNull(KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(scramShaSecret.getMetadata().getName()).get());
    }

    @TestDoc(
        description = @Desc("Verifies TLS external user authentication with custom certificates and ACL authorization."),
        steps = {
            @Step(value = "Deploy Kafka cluster with TLS authentication and Simple authorization.", expected = "Kafka cluster is deployed with TLS listener and Simple ACL authorization enabled."),
            @Step(value = "Create TLS external user with ACL permissions.", expected = "TLS external user is created with specified ACL rules for topic access."),
            @Step(value = "Create custom external TLS secret for user.", expected = "External TLS secret is created with custom certificates."),
            @Step(value = "Test message sending and receiving with TLS external user.", expected = "Messages are successfully sent and received using external TLS certificates.")
        },
        labels = {
            @Label(TestDocsLabels.USER_OPERATOR)
        }
    )
    @ParallelNamespaceTest
    void testTlsExternalUser() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String consumerGroupName = ClientUtils.generateRandomConsumerGroup();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
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

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName()).build());

        final KafkaUser tlsExternalUserWithQuotasAndAcls = KafkaUserTemplates.tlsExternalUser(testStorage.getNamespaceName(), testStorage.getKafkaUsername(), testStorage.getClusterName())
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withPatternType(AclResourcePatternType.LITERAL)
                            .withName(testStorage.getTopicName())
                        .endAclRuleTopicResource()
                        .withOperations(AclOperation.READ, AclOperation.WRITE, AclOperation.DESCRIBE, AclOperation.CREATE)
                    .endAcl()
                    .addNewAcl()
                        .withNewAclRuleGroupResource()
                            .withPatternType(AclResourcePatternType.LITERAL)
                            .withName(consumerGroupName)
                        .endAclRuleGroupResource()
                        .withOperations(AclOperation.READ)
                    .endAcl()
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build();
        KubeResourceManager.get().createResourceWithWait(tlsExternalUserWithQuotasAndAcls);

        // For clients of authentication type tls-external, operator should not create a secret
        KafkaUserUtils.waitForKafkaUserReady(testStorage.getNamespaceName(), testStorage.getKafkaUsername());
        assertThat(KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getKafkaUsername()).get(), nullValue());
        assertThat(CrdClients.kafkaUserClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getKafkaUsername()).get().getStatus().getUsername(), is("CN=" + testStorage.getKafkaUsername()));

        SecretUtils.createExternalTlsUserSecret(testStorage.getNamespaceName(), testStorage.getKafkaUsername(), testStorage.getClusterName());

        KafkaClients kafkaClients = ClientUtils.getInstantTlsClientBuilder(testStorage)
            .withUsername(testStorage.getKafkaUsername())
            .withConsumerGroup(consumerGroupName)
            .build();

        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()), kafkaClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        KafkaUserUtils.replace(testStorage.getNamespaceName(), testStorage.getKafkaUsername(), user -> {
            user.getSpec().setAuthorization(new KafkaUserAuthorizationSimpleBuilder()
                    .addNewAcl()
                        .withNewAclRuleTopicResource()
                            .withPatternType(AclResourcePatternType.LITERAL)
                            .withName(testStorage.getTopicName())
                        .endAclRuleTopicResource()
                        .withOperations(AclOperation.READ, AclOperation.DESCRIBE)
                    .endAcl()
                    .build());
        }
        );

        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerTlsStrimzi(testStorage.getClusterName()));

        PodUtils.waitUntilMessageIsInPodLogs(testStorage.getNamespaceName(),
            PodUtils.getPodNameByPrefix(testStorage.getNamespaceName(), testStorage.getProducerName()), "authorization failed");

        ClientUtils.waitForInstantProducerClientTimeout(testStorage);
    }

    @BeforeAll
    void setup() {
        sharedTestStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(sharedTestStorage.getNamespaceName(), sharedTestStorage.getBrokerPoolName(), sharedTestStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(sharedTestStorage.getNamespaceName(), sharedTestStorage.getControllerPoolName(), sharedTestStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName(), 1)
            .editSpec()
                .editKafka()
                    .addToListeners(new GenericKafkaListenerBuilder()
                                .withName("scramshatls")
                                .withPort(9095)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationScramSha512Auth()
                                .endKafkaListenerAuthenticationScramSha512Auth()
                                .build())
                .endKafka()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getScraperName()).build()
        );

        scraperPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getScraperName()).get(0).getMetadata().getName();
    }
}
