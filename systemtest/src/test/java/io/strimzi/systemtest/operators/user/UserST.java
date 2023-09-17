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
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
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
import io.strimzi.systemtest.utils.TestKafkaVersion;
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
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
class UserST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(UserST.class);

    private final String userClusterName = "user-cluster-name";

    private final String scraperName = userClusterName + "-" + Constants.SCRAPER_NAME;
    private String scraperPodName = "";

    @ParallelTest
    void testUserWithNameMoreThan64Chars(ExtensionContext extensionContext) {
        String userWithLongName = "user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijk"; // 65 character username
        String userWithCorrectName = "user-with-correct-name" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopq"; // 64 character username
        String saslUserWithLongName = "sasl-user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdef"; // 65 character username

        // Create user with correct name
        resourceManager.createResourceWithWait(extensionContext, KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, userClusterName, userWithCorrectName).build());

        KafkaUserUtils.waitUntilKafkaUserStatusConditionIsPresent(Environment.TEST_SUITE_NAMESPACE, userWithCorrectName);

        Condition condition = KafkaUserResource.kafkaUserClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(userWithCorrectName).get().getStatus().getConditions().get(0);

        verifyCRStatusCondition(condition, "True", Ready);

        // Create sasl user with long name, shouldn't fail
        resourceManager.createResourceWithWait(extensionContext, KafkaUserTemplates.scramShaUser(Environment.TEST_SUITE_NAMESPACE, userClusterName, saslUserWithLongName).build());

        resourceManager.createResourceWithoutWait(extensionContext, KafkaUserTemplates.defaultUser(Environment.TEST_SUITE_NAMESPACE, userClusterName, userWithLongName)
            .withNewSpec()
                .withNewKafkaUserTlsClientAuthentication()
                .endKafkaUserTlsClientAuthentication()
            .endSpec()
            .build());

        KafkaUserUtils.waitUntilKafkaUserStatusConditionIsPresent(Environment.TEST_SUITE_NAMESPACE, userWithLongName);

        condition = KafkaUserResource.kafkaUserClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(userWithLongName).get().getStatus().getConditions().get(0);

        verifyCRStatusCondition(condition,
                "only up to 64 characters",
                "ExecutionException", "True", NotReady);
    }

    @ParallelTest
    @Tag(ACCEPTANCE)
    void testUpdateUser(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        String userName = testStorage.getKafkaUsername();

        resourceManager.createResourceWithWait(extensionContext, KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, userClusterName, userName).build());

        String kafkaUserSecret = TestUtils.toJsonString(kubeClient(Environment.TEST_SUITE_NAMESPACE).getSecret(userName));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['ca.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.key']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.name", equalTo(userName)));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.namespace", equalTo(Environment.TEST_SUITE_NAMESPACE)));

        KafkaUser kUser = KafkaUserResource.kafkaUserClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(userName).get();
        String kafkaUserAsJson = TestUtils.toJsonString(kUser);

        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(userName)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(Environment.TEST_SUITE_NAMESPACE)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo(Constants.TLS_LISTENER_DEFAULT_NAME)));

        long observedGeneration = KafkaUserResource.kafkaUserClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(userName).get().getStatus().getObservedGeneration();

        KafkaUserResource.replaceUserResourceInSpecificNamespace(userName, ku -> {
            ku.getMetadata().setResourceVersion(null);
            ku.getSpec().setAuthentication(new KafkaUserScramSha512ClientAuthentication());
        }, Environment.TEST_SUITE_NAMESPACE);

        KafkaUserUtils.waitForKafkaUserIncreaseObserverGeneration(Environment.TEST_SUITE_NAMESPACE, observedGeneration, userName);
        KafkaUserUtils.waitForKafkaUserCreation(Environment.TEST_SUITE_NAMESPACE, userName);

        String anotherKafkaUserSecret = TestUtils.toJsonString(kubeClient(Environment.TEST_SUITE_NAMESPACE).getSecret(Environment.TEST_SUITE_NAMESPACE, userName));

        assertThat(anotherKafkaUserSecret, hasJsonPath("$.data.password", notNullValue()));

        kUser = Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(userName).get();
        kafkaUserAsJson = TestUtils.toJsonString(kUser);
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(userName)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(Environment.TEST_SUITE_NAMESPACE)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo("scram-sha-512")));

        Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(Environment.TEST_SUITE_NAMESPACE).resource(kUser).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(Environment.TEST_SUITE_NAMESPACE, userName);
    }

    @ParallelTest
    void testTlsUserWithQuotas(ExtensionContext extensionContext) {
        // skip test if KRaft mode is enabled and Kafka version is lower than 3.5.0 - https://github.com/strimzi/strimzi-kafka-operator/issues/8806
        assumeFalse(Environment.isKRaftModeEnabled() && TestKafkaVersion.compareDottedVersions("3.5.0", Environment.ST_KAFKA_VERSION) == 1);

        KafkaUser user = KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, userClusterName, "encrypted-arnost").build();

        testUserWithQuotas(extensionContext, user);
    }

    @ParallelTest
    void testTlsExternalUserWithQuotas(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        // skip test if KRaft mode is enabled and Kafka version is lower than 3.5.0 - https://github.com/strimzi/strimzi-kafka-operator/issues/8806
        assumeFalse(Environment.isKRaftModeEnabled() && TestKafkaVersion.compareDottedVersions("3.5.0", Environment.ST_KAFKA_VERSION) == 1);

        final String kafkaUserName = testStorage.getKafkaUsername();
        final KafkaUser tlsExternalUser = KafkaUserTemplates.tlsExternalUser(Environment.TEST_SUITE_NAMESPACE, userClusterName, kafkaUserName).build();

        testUserWithQuotas(extensionContext, tlsExternalUser);
    }

    @ParallelTest
    void testScramUserWithQuotas(ExtensionContext extensionContext) {
        KafkaUser user = KafkaUserTemplates.scramShaUser(Environment.TEST_SUITE_NAMESPACE, userClusterName, "scramed-arnost").build();

        testUserWithQuotas(extensionContext, user);
    }

    @ParallelTest
    void testUserTemplate(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        String userName = testStorage.getKafkaUsername();

        String labelKey = "test-label-key";
        String labelValue = "test-label-value";
        String annotationKey = "test-annotation-key";
        String annotationValue = "test-annotation-value";

        resourceManager.createResourceWithWait(extensionContext, KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, userClusterName, userName)
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

        Secret userSecret = kubeClient(Environment.TEST_SUITE_NAMESPACE).getSecret(userName);
        assertThat(userSecret.getMetadata().getLabels().get(labelKey), is(labelValue));
        assertThat(userSecret.getMetadata().getAnnotations().get(annotationKey), is(annotationValue));
    }

    synchronized void testUserWithQuotas(ExtensionContext extensionContext, KafkaUser user) {
        final Integer prodRate = 1111;
        final Integer consRate = 2222;
        final Integer reqPerc = 42;
        final Double mutRate = 10d;

        // Create user with correct name
        resourceManager.createResourceWithWait(extensionContext, KafkaUserTemplates.userWithQuotas(user, prodRate, consRate, reqPerc, mutRate)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        final String userName = KafkaUserResource.kafkaUserClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(user.getMetadata().getName()).get().getStatus().getUsername();

        TestUtils.waitFor("all KafkaUser " + userName + " attributes are present", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                String result = KafkaCmdClient.describeUserUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(userClusterName), userName);
                return result.contains("Quota configs for user-principal '" + userName + "' are") &&
                    result.contains("request_percentage=" + reqPerc) &&
                    result.contains("producer_byte_rate=" + prodRate) &&
                    result.contains("consumer_byte_rate=" + consRate) &&
                    result.contains("controller_mutation_rate=" + mutRate);
            });

        // delete user
        KafkaUserResource.kafkaUserClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(user.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(Environment.TEST_SUITE_NAMESPACE, user.getMetadata().getName());

        TestUtils.waitFor("all attributes of KafkaUser: " + userName + " to be cleaned", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                String resultAfterDelete = KafkaCmdClient.describeUserUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(userClusterName), userName);

                return
                    !resultAfterDelete.contains(userName) &&
                    !resultAfterDelete.contains("request_percentage") &&
                    !resultAfterDelete.contains("producer_byte_rate") &&
                    !resultAfterDelete.contains("consumer_byte_rate") &&
                    !resultAfterDelete.contains("controller_mutation_rate");
            });
    }

    @ParallelNamespaceTest
    void testCreatingUsersWithSecretPrefix(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Environment.TEST_SUITE_NAMESPACE);

        final String secretPrefix = "top-secret-";
        final String tlsUserName = "encrypted-leopold";
        final String scramShaUserName = "scramed-leopold";

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
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

        resourceManager.createResourceWithWait(extensionContext,
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), tlsUserName).build(),
            KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getClusterName(), scramShaUserName).build()
        );

        Secret tlsSecret = kubeClient().getSecret(testStorage.getNamespaceName(), secretPrefix + tlsUserName);
        Secret scramShaSecret = kubeClient().getSecret(testStorage.getNamespaceName(), secretPrefix + scramShaUserName);

        LOGGER.info("Checking for existing user Secrets with prefix: {}", secretPrefix);
        assertNotNull(tlsSecret);
        assertNotNull(scramShaSecret);

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(secretPrefix + tlsUserName)
            .build();

        LOGGER.info("Checking if TLS user is able to send messages");
        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        clients = new KafkaClientsBuilder(clients)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withUsername(secretPrefix + scramShaUserName)
            .build();

        LOGGER.info("Checking if SCRAM-SHA user is able to send messages");
        resourceManager.createResourceWithWait(extensionContext, clients.producerScramShaPlainStrimzi(), clients.consumerScramShaPlainStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Checking owner reference - if the Secret will be deleted when we delete KafkaUser");

        LOGGER.info("Deleting KafkaUser: {}/{}", testStorage.getNamespaceName(), tlsUserName);
        KafkaUserResource.kafkaUserClient().inNamespace(testStorage.getNamespaceName()).withName(tlsUserName).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(testStorage.getNamespaceName(), tlsUserName);

        LOGGER.info("Deleting KafkaUser: {}/{}", testStorage.getNamespaceName(), scramShaUserName);
        KafkaUserResource.kafkaUserClient().inNamespace(testStorage.getNamespaceName()).withName(scramShaUserName).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(testStorage.getNamespaceName(), scramShaUserName);

        LOGGER.info("Checking if Secrets are deleted");
        SecretUtils.waitForSecretDeletion(testStorage.getNamespaceName(), tlsSecret.getMetadata().getName());
        SecretUtils.waitForSecretDeletion(testStorage.getNamespaceName(), scramShaSecret.getMetadata().getName());
        assertNull(kubeClient().getSecret(testStorage.getNamespaceName(), tlsSecret.getMetadata().getName()));
        assertNull(kubeClient().getSecret(testStorage.getNamespaceName(), scramShaSecret.getMetadata().getName()));
    }

    @ParallelNamespaceTest
    void testTlsExternalUser(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, extensionContext);
        final String clusterName = testStorage.getClusterName();
        final String userName = testStorage.getKafkaUsername();
        final String topicName = testStorage.getTopicName();

        final AclRule aclRule = new AclRuleBuilder()
            .withNewAclRuleTopicResource()
                .withName(topicName)
            .endAclRuleTopicResource()
            .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE)
            .build();

        // exercise (a) - create Kafka cluster with support for authorization
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
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
        resourceManager.createResourceWithWait(extensionContext, tlsExternalUserWithQuotasAndAcls);

        // verify (a) - that secrets are not generated and KafkaUser is created
        KafkaUserUtils.waitForKafkaUserReady(namespaceName, userName);
        assertThat(kubeClient().getSecret(namespaceName, userName), nullValue());

        // verify (b) -  if the operator has the right username in the status, that is what it also used in the ACLs and Quotas
        KafkaUser user = KafkaUserResource.kafkaUserClient().inNamespace(namespaceName).withName(userName).get();

        assertThat(user.getStatus().getUsername(), is("CN=" + userName));
    }

    /**
     * @description This test case checks that KafkaUser custom resource is managed correctly, even in presence of more than just one User Operator.
     *
     * @steps
     *  1. - In addition to already existing Kafka Cluster (A), deploy another Kafka Cluster (B) in the same Namespace
     *     - Kafka (B) is deployed
     *  2. - Create KafkaUser custom resource with metadata due to which it will be listened to by User Operator belonging to the Kafka Cluster B
     *     - Secret with label referencing Kafka Cluster B is created, and Kafka resource user corresponding to created KafkaUser custom resource exists only in Kafka Cluster B
     *
     * @usecase
     *  - labels
     *  - user-operator
     *  - users
     *  - secrets
     */
    @ParallelTest
    void testUOListeningOnlyUsersInSameCluster(ExtensionContext extensionContext) {
        // skip test if KRaft mode is enabled and Kafka version is lower than 3.5.0 - https://github.com/strimzi/strimzi-kafka-operator/issues/8806
        assumeFalse(Environment.isKRaftModeEnabled() && TestKafkaVersion.compareDottedVersions("3.5.0", Environment.ST_KAFKA_VERSION) == 1);

        final TestStorage testStorage = new TestStorage(extensionContext, Environment.TEST_SUITE_NAMESPACE);
        final String userListeningClusterName = "user-listening-cluster";
        final String userIgnoringClusterName = userClusterName; // pre-created shared Kafka will be used as the secondary cluster (its UO ignoring KafkaUser with diff label)

        LOGGER.info("Creating new Kafka: {}/{}, whose TO will listen to soon to be created KafkaUser CR", Environment.TEST_SUITE_NAMESPACE, userListeningClusterName);
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(userListeningClusterName, 3, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        LOGGER.info("Creating new KafkaUser: {}/{}", Environment.TEST_SUITE_NAMESPACE, testStorage.getUsername());
        final KafkaUser user = KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, userListeningClusterName, testStorage.getUsername()).build();
        resourceManager.createResourceWithWait(extensionContext, KafkaUserTemplates.userWithQuotas(user, 123, 123, 12, 10d)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        LOGGER.info("Verifying KafkaUser: {}/{} is created in Kafka: {}/{} ", Environment.TEST_SUITE_NAMESPACE, testStorage.getUsername(), Environment.TEST_SUITE_NAMESPACE, userListeningClusterName);
        String entityOperatorPodName = kubeClient().listPodNamesInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, Labels.STRIMZI_NAME_LABEL, KafkaResources.entityOperatorDeploymentName(userListeningClusterName)).get(0);
        String uOLogs = kubeClient().logsInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, entityOperatorPodName, "user-operator");
        assertThat(uOLogs, containsString("KafkaUser " + testStorage.getUsername() + " in namespace " + Environment.TEST_SUITE_NAMESPACE + " was ADDED"));

        LOGGER.info("Verifying KafkaUser: {}/{} is not created in Kafka: {}/{}", Environment.TEST_SUITE_NAMESPACE, testStorage.getUsername(), Environment.TEST_SUITE_NAMESPACE, userIgnoringClusterName);
        entityOperatorPodName = kubeClient().listPodNamesInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, Labels.STRIMZI_NAME_LABEL, KafkaResources.entityOperatorDeploymentName(userIgnoringClusterName)).get(0);
        uOLogs = kubeClient().logsInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, entityOperatorPodName, "user-operator");
        assertThat(uOLogs, not(containsString("KafkaUser " + testStorage.getUsername() + " in namespace " + Environment.TEST_SUITE_NAMESPACE + " was ADDED")));

        LOGGER.info("Verifying KafkaUser: {}/{} has label: {}={} corresponding to Kafka cluster it belongs to", Environment.TEST_SUITE_NAMESPACE, testStorage.getUsername(), Labels.STRIMZI_CLUSTER_LABEL, userListeningClusterName);
        String kafkaUserResource = cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).getResourceAsYaml("kafkauser", testStorage.getUsername());
        assertThat(kafkaUserResource, containsString(Labels.STRIMZI_CLUSTER_LABEL + ": " + userListeningClusterName));

        SecretUtils.waitForSpecificLabelKeyValue(testStorage.getUsername(), Environment.TEST_SUITE_NAMESPACE, Labels.STRIMZI_CLUSTER_LABEL, userListeningClusterName);

        KafkaUserUtils.waitForKafkaUserMappingIntoKafkaResource(Environment.TEST_SUITE_NAMESPACE, testStorage.getUsername(), userListeningClusterName, scraperPodName);

        LOGGER.info("Verifying KafkaUser: {}/{} is not present in Kafka ecosystem in Kafka/{}", Environment.TEST_SUITE_NAMESPACE, testStorage.getUsername(), userIgnoringClusterName);
        String getUserResult = KafkaCmdClient.describeUserUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(userIgnoringClusterName), "CN=" + testStorage.getUsername());
        assertThat("KafkaUser CR is not mapped as Kafka resource user", getUserResult, not(containsString(testStorage.getUsername())));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(userClusterName, 1, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build(),
            ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, scraperName).build()
        );

        scraperPodName = kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, scraperName).get(0).getMetadata().getName();
    }
}
