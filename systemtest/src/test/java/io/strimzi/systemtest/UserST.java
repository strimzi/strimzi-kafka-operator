/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.libClient.KafkaClient;
import io.strimzi.systemtest.utils.AvailabilityVerifier;
import io.strimzi.test.TestUtils;
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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.strimzi.test.TestUtils.waitFor;
import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@ExtendWith(StrimziExtension.class)
class UserST extends AbstractST {

    public static final String NAMESPACE = "user-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(UserST.class);

    @Test
    @Tag(REGRESSION)
    void testUpdateUser() {
        LOGGER.info("Running testUpdateUser in namespace {}", NAMESPACE);
        String kafkaUser = "test-user";

        resources().kafka(resources().defaultKafka(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                        .endKafkaListenerExternalLoadBalancer()
                        .editTls()
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                        .endTls()
                        .endListeners()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                .endKafka()
            .endSpec().build()).done();

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
    void test() throws InterruptedException, ExecutionException, TimeoutException {
        LOGGER.info("Running testUpdateUser in namespace {}", NAMESPACE);
        String kafkaUser = "test-user";
        int messageCount = 50;

        resources().kafka(resources().defaultKafka(CLUSTER_NAME, 3)
                .editSpec()
                .editKafka()
                .editListeners()
                .withNewKafkaListenerExternalLoadBalancer()
                    .withTls(false)
                .endKafkaListenerExternalLoadBalancer()
//                .editTls()
//                .withNewKafkaListenerAuthenticationTlsAuth()
//                .endKafkaListenerAuthenticationTlsAuth()
//                .endTls()
                .endListeners()
//                .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
                .endSpec().build()).done();

        resources().topic(CLUSTER_NAME, "my-topic-1").done();

        KafkaUser user = resources().tlsUser(CLUSTER_NAME, kafkaUser).done();
        waitFor("Wait for secrets became available", GLOBAL_POLL_INTERVAL, 180000,
                () -> CLIENT.secrets().inNamespace(NAMESPACE).withName(kafkaUser).get() != null,
                () -> LOGGER.error("Couldn't find user secret {}", CLIENT.secrets().inNamespace(NAMESPACE).list().getItems()));

        LOGGER.info("before");
        KafkaClient testClient = new KafkaClient();
        Future producer = testClient.sendMessages("my-topic-1", NAMESPACE, CLUSTER_NAME, messageCount);

        Future consumer = testClient.receiveMessages("my-topic-1", NAMESPACE, CLUSTER_NAME, messageCount);

        LOGGER.info("after");

        Thread.sleep(60000);
        LOGGER.info("after sleep");

        assertThat("Producer producer all messages", producer.get(1, TimeUnit.MINUTES), is(messageCount));
        assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), is(messageCount));

//        AvailabilityVerifier mp = waitForInitialAvailability(kafkaUser);

        KUBE_CLIENT.deleteByName("KafkaUser", kafkaUser);
        KUBE_CLIENT.waitForResourceDeletion("KafkaUser", kafkaUser);
    }

    @BeforeEach
    void createTestResources() {
        createResources();
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

    @Override
    void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
    }
}
