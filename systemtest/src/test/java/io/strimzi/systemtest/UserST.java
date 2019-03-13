/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.kafkaclients.verifiable.VerifiableConsumer;
import io.strimzi.systemtest.kafkaclients.verifiable.VerifiableProducer;
import io.strimzi.test.extensions.StrimziExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@ExtendWith(StrimziExtension.class)
class UserST extends MessagingBaseST {

    public static final String NAMESPACE = "user-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(UserST.class);

    @Test
    @Tag(REGRESSION)
    void testUpdateUser() throws InterruptedException, ExecutionException, TimeoutException {
        LOGGER.info("Running testUpdateUser in namespace {}", NAMESPACE);
        String kafkaUser = "test-user";

        resources().kafka(resources().defaultKafka(CLUSTER_NAME, 3)
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

//        availabilityTest(new VerifiableProducer(), new VerifiableConsumer(), 50);

        KafkaUser user = resources().tlsUser(CLUSTER_NAME, kafkaUser).done();
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

        resources().user(user)
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

    @Test
    @Tag(REGRESSION)
    void testMessaging() throws InterruptedException, ExecutionException, TimeoutException {
        resources().kafka(resources().defaultKafka(CLUSTER_NAME, 3).build()).done();

        availabilityTest(new VerifiableProducer(), new VerifiableConsumer(), 5000, 60000, CLUSTER_NAME);
    }

    @BeforeEach
    void createTestResources() throws Exception {
        createResources();
        resources.createServiceResource(Resources.KAFKA_CLIENTS, Environment.INGRESS_DEFAULT_PORT, NAMESPACE).done();
        resources.createIngress(Resources.KAFKA_CLIENTS, Environment.INGRESS_DEFAULT_PORT, ENVIRONMENT.getKubernetesApiUrl(), NAMESPACE).done();
        resources.deployKafkaClients(CLUSTER_NAME).done();
    }

    @AfterEach
    void deleteTestResources() throws Exception {
        deleteResources();
        waitForDeletion(TEARDOWN_GLOBAL_WAIT, NAMESPACE);
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources.clusterOperator(NAMESPACE).done();
    }

    @AfterAll
    void teardownEnvironment() {
        testClassResources.deleteResources();
        teardownEnvForOperator();
    }
}
