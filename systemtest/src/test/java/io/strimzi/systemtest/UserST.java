/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.PERFORMANCE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
@Tag(ACCEPTANCE)
class UserST extends AbstractST {

    public static final String NAMESPACE = "user-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(UserST.class);

    @Test
    void testUserWithNameMoreThan64Chars() {
        String userWithLongName = "user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijk"; // 65 character username
        String userWithCorrectName = "user-with-correct-name" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopq"; // 64 character username
        String saslUserWithLongName = "sasl-user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdef"; // 65 character username

        // Create user with correct name
        testMethodResources().tlsUser(CLUSTER_NAME, userWithCorrectName).done();
        StUtils.waitForSecretReady(userWithCorrectName);

        String messageUserWasAdded = "KafkaUser " + userWithCorrectName + " in namespace " + NAMESPACE + " was ADDED";
        String errorMessage = "InvalidResourceException: Users with TLS client authentication can have a username (name of the KafkaUser custom resource) only up to 64 characters long.";

        // Checking UO logs
        String entityOperatorPodName = kubeClient().listPods("strimzi.io/name", KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)).get(0).getMetadata().getName();
        String uOlogs = kubeClient().logs(entityOperatorPodName, "user-operator");
        assertThat(uOlogs, containsString(messageUserWasAdded));
        assertThat(uOlogs, not(containsString(errorMessage)));

        // Create sasl user with long name
        testMethodResources().scramShaUser(CLUSTER_NAME, saslUserWithLongName).done();
        StUtils.waitForSecretReady(saslUserWithLongName);

        // Checking UO logs
        uOlogs = kubeClient().logs(entityOperatorPodName, "user-operator");
        messageUserWasAdded = "KafkaUser " + saslUserWithLongName + " in namespace " + NAMESPACE + " was ADDED";
        assertThat(uOlogs, containsString(messageUserWasAdded));
        assertThat(uOlogs, not(containsString(errorMessage)));

        // Create user with long name
        testMethodResources().tlsUser(CLUSTER_NAME, userWithLongName).done();
        // Checking UO logs
        StUtils.waitForKafkaUserCreationError(userWithLongName, entityOperatorPodName);
    }

    @Test
    void testUpdateUser() {
        String kafkaUser = "test-user";

        KafkaUser user = testMethodResources().tlsUser(CLUSTER_NAME, kafkaUser).done();
        StUtils.waitForSecretReady(kafkaUser);

        String kafkaUserSecret = TestUtils.toJsonString(kubeClient().getSecret(kafkaUser));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['ca.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.key']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.name", equalTo(kafkaUser)));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));

        KafkaUser kUser = Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(kafkaUser).get();
        String kafkaUserAsJson = TestUtils.toJsonString(kUser);

        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(kafkaUser)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo("tls")));

        kafkaUser = "new-" + kafkaUser;

        testMethodResources().user(user)
            .editMetadata()
                .withResourceVersion(null)
                .withName(kafkaUser)
            .endMetadata()
            .editSpec()
                .withNewKafkaUserScramSha512ClientAuthentication()
                .endKafkaUserScramSha512ClientAuthentication()
            .endSpec().done();

        StUtils.waitForSecretReady(kafkaUser);
        kafkaUserSecret = TestUtils.toJsonString(kubeClient().getSecret(kafkaUser));
        assertThat(kafkaUserSecret, hasJsonPath("$.data.password", notNullValue()));

        kUser = Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(kafkaUser).get();
        kafkaUserAsJson = TestUtils.toJsonString(kUser);
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(kafkaUser)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo("scram-sha-512")));

        Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).delete(kUser);
        StUtils.waitForKafkaUserDeletion(kafkaUser);
    }

    @Tag(PERFORMANCE)
    @Test
    void testBigAmountOfUsers(){
        int numberOfUsers = 100;

        for(int i = 0; i < numberOfUsers; i++){
            String userName = "alisa" + i;
            LOGGER.info("Creating user with name {}", userName);
            testMethodResources().tlsUser(CLUSTER_NAME, userName).done();
            StUtils.waitForSecretReady(userName);
            LOGGER.info("Checking status of deployed Kafka User {}", userName);
            Condition kafkaCondition = testMethodResources().kafkaUser().inNamespace(NAMESPACE).withName(userName).get()
                    .getStatus().getConditions().get(0);
            LOGGER.info("Kafka User Status: {}", kafkaCondition.getStatus());
            LOGGER.info("Kafka User Type: {}", kafkaCondition.getType());
            assertEquals("Ready", kafkaCondition.getType());
            LOGGER.info("Kafka User {} is in desired state: {}", userName, kafkaCondition.getType());
        }
    }

    @BeforeEach
    void createTestResources() {
        createTestMethodResources();
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources().clusterOperator(NAMESPACE).done();

        testClassResources().kafka(testClassResources().defaultKafka(CLUSTER_NAME, 3)
                .editSpec()
                .editKafka()
                .editListeners()
                .editTls()
                .withNewKafkaListenerAuthenticationTlsAuth()
                .endKafkaListenerAuthenticationTlsAuth()
                .endTls()
                .endListeners()
                .withNewKafkaAuthorizationSimple()
                .endKafkaAuthorizationSimple()
                .endKafka()
                .endSpec().build()).done();
    }

    @Override
    protected void tearDownEnvironmentAfterEach() throws Exception {
        deleteTestMethodResources();
    }
}
