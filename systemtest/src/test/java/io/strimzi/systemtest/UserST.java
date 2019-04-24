/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.model.KafkaUser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
class UserST extends AbstractST {

    public static final String NAMESPACE = "user-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(UserST.class);

    @Test
    void testUpdateUser() {
        LOGGER.info("Running testUpdateUser in namespace {}", NAMESPACE);
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
        KUBE_CLIENT.waitForResourceCreation("secret", kafkaUser);

        String kafkaUserSecret = KUBE_CLIENT.getResourceAsJson("secret", kafkaUser);
        assertThat(kafkaUserSecret, hasJsonPath("$.data['ca.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.key']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.name", equalTo(kafkaUser)));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));

        String kafkaUserAsJson = KUBE_CLIENT.getResourceAsJson("KafkaUser", kafkaUser);

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

        kafkaUserSecret = KUBE_CLIENT.getResourceAsJson("secret", kafkaUser);
        assertThat(kafkaUserSecret, hasJsonPath("$.data.password", notNullValue()));

        kafkaUserAsJson = KUBE_CLIENT.getResourceAsJson("KafkaUser", kafkaUser);
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(kafkaUser)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo("scram-sha-512")));

        KUBE_CLIENT.deleteByName("KafkaUser", kafkaUser);
        KUBE_CLIENT.waitForResourceDeletion("KafkaUser", kafkaUser);
    }

    @BeforeEach
    void createTestResources() {
        createResources();
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating testMethodResources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources.clusterOperator(NAMESPACE).done();
    }

    @Override
    void tearDownEnvironmentAfterEach() throws Exception {
        deleteTestMethodResources();
        waitForDeletion(TIMEOUT_TEARDOWN, NAMESPACE);
    }
}
