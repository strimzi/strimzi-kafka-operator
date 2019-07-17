/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaUser;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
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

        testMethodResources().kafka(testMethodResources().defaultKafka(CLUSTER_NAME, 1, 1)
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

        // Create user with correct name
        testMethodResources().tlsUser(CLUSTER_NAME, userWithCorrectName).done();
        StUtils.waitForSecretReady(userWithCorrectName);

        String messageUserWasAdded = "KafkaUser " + userWithCorrectName + " in namespace " + NAMESPACE + " was ADDED";
        String errorMessage = "InvalidResourceException: Users with TLS client authentication can have a username (name of the KafkaUser custom resource) only up to 64 characters long.";

        // Checking UO logs
        String entityOperatorPodName = cmdKubeClient().listResourcesByLabel("pod", "strimzi.io/name=my-cluster-entity-operator").get(0);
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
        final String messageUserWasNotAdded = "KafkaUser(" + NAMESPACE + "/" + userWithLongName + "): createOrUpdate failed";
        TestUtils.waitFor("User operator has expected error", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                String logs = kubeClient().logs(entityOperatorPodName, "user-operator");
                return logs.contains(errorMessage) && logs.contains(messageUserWasNotAdded);
            });
    }

    @Test
    void testUpdateUser() {
        String kafkaUser = "test-user";

        testMethodResources().kafka(testMethodResources().defaultKafka(CLUSTER_NAME, 3)
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
        getTestClassResources().clusterOperator(NAMESPACE).done();
    }

    @Override
    protected void tearDownEnvironmentAfterEach() throws Exception {
        deleteTestMethodResources();
        waitForDeletion(Constants.TIMEOUT_TEARDOWN);
    }
}
