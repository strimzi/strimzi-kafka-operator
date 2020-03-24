/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.ExecResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SCALABILITY;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
class UserST extends BaseST {

    public static final String NAMESPACE = "user-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(UserST.class);

    @Test
    void testUserWithNameMoreThan64Chars() {
        String userWithLongName = "user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijk"; // 65 character username
        String userWithCorrectName = "user-with-correct-name" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopq"; // 64 character username
        String saslUserWithLongName = "sasl-user" + "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdef"; // 65 character username

        // Create user with correct name
        KafkaUserResource.tlsUser(CLUSTER_NAME, userWithCorrectName).done();

        KafkaUserUtils.waitUntilKafkaUserStatusConditionIsPresent(userWithCorrectName);

        Condition condition = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userWithCorrectName).get().getStatus().getConditions().get(0);

        verifyCRStatusCondition(condition,
                "True",
                "Ready");

        // Create sasl user with long name, shouldn't fail
        KafkaUserResource.scramShaUser(CLUSTER_NAME, saslUserWithLongName).done();
        KafkaUserUtils.waitForKafkaUserCreation(saslUserWithLongName);

        KafkaUserResource.kafkaUserWithoutWait(KafkaUserResource.defaultUser(CLUSTER_NAME, userWithLongName)
            .withNewSpec()
                .withNewKafkaUserTlsClientAuthentication()
                .endKafkaUserTlsClientAuthentication()
            .endSpec()
            .build());

        KafkaUserUtils.waitUntilKafkaUserStatusConditionIsPresent(userWithLongName);

        condition = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userWithLongName).get().getStatus().getConditions().get(0);

        verifyCRStatusCondition(condition,
                "only up to 64 characters",
                "InvalidResourceException",
                "True",
                "NotReady");
    }

    @Test
    @Tag(ACCEPTANCE)
    void testUpdateUser() {
        KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME).done();

        String kafkaUserSecret = TestUtils.toJsonString(kubeClient().getSecret(USER_NAME));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['ca.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.crt']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.data['user.key']", notNullValue()));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.name", equalTo(USER_NAME)));
        assertThat(kafkaUserSecret, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));

        KafkaUser kUser = KafkaUserResource.kafkaUserClient().inNamespace(kubeClient().getNamespace()).withName(USER_NAME).get();
        String kafkaUserAsJson = TestUtils.toJsonString(kUser);

        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(USER_NAME)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo("tls")));

        long observedGeneration = KafkaUserResource.kafkaUserClient().inNamespace(kubeClient().getNamespace()).withName(USER_NAME).get().getStatus().getObservedGeneration();

        KafkaUserResource.replaceUserResource(USER_NAME, ku -> {
            ku.getMetadata().setResourceVersion(null);
            ku.getSpec().setAuthentication(new KafkaUserScramSha512ClientAuthentication());
        });

        KafkaUserUtils.waitForKafkaUserIncreaseObserverGeneration(observedGeneration, USER_NAME);
        KafkaUserUtils.waitForKafkaUserCreation(USER_NAME);

        kafkaUserSecret = TestUtils.toJsonString(kubeClient().getSecret(USER_NAME));
        assertThat(kafkaUserSecret, hasJsonPath("$.data.password", notNullValue()));

        kUser = Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(USER_NAME).get();
        kafkaUserAsJson = TestUtils.toJsonString(kUser);
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(USER_NAME)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));
        assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo("scram-sha-512")));

        Crds.kafkaUserOperation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).delete(kUser);
        KafkaUserUtils.waitForKafkaUserDeletion(USER_NAME);
    }

    @Tag(SCALABILITY)
    @Test
    void testBigAmountOfScramShaUsers() {
        createBigAmountOfUsers("SCRAM_SHA");
    }

    @Tag(SCALABILITY)
    @Test
    void testBigAmountOfTlsUsers() {
        createBigAmountOfUsers("TLS");
    }

    @Test
    void testTlsUserWithQuotas() {
        testUserWithQuotas(KafkaUserResource.tlsUser(CLUSTER_NAME, "encrypted-arnost").done());
    }

    @Test
    void testScramUserWithQuotas() {
        testUserWithQuotas(KafkaUserResource.scramShaUser(CLUSTER_NAME, "scramed-arnost").done());
    }

    void testUserWithQuotas(KafkaUser user) {
        String userName = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(user.getMetadata().getName()).get().getStatus().getUsername();

        Integer prodRate = 1111;
        Integer consRate = 2222;
        Integer reqPerc = 42;

        // Create user with correct name
        KafkaUserResource.userWithQuota(user, prodRate, consRate, reqPerc).done();

        String command = "sh bin/kafka-configs.sh --zookeeper localhost:2181 --describe --entity-type users";
        LOGGER.debug("Command for kafka-configs.sh {}", command);

        ExecResult result = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", command);
        assertThat(result.out().contains("Configs for user-principal '" + userName + "' are"), is(true));
        assertThat(result.out().contains("request_percentage=" + reqPerc), is(true));
        assertThat(result.out().contains("producer_byte_rate=" + prodRate), is(true));
        assertThat(result.out().contains("consumer_byte_rate=" + consRate), is(true));

        String zkListCommand = "sh /opt/kafka/bin/zookeeper-shell.sh localhost:2181 <<< 'ls /config/users'";

        TestUtils.waitFor("user " + userName + " will be available in Zookeeper", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            ExecResult zkResult = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", zkListCommand);
            try {
                return zkResult.out().contains(URLEncoder.encode(userName, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("Failed to encode username", e);
            }
        });

        // delete user
        KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(user.getMetadata().getName()).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(user.getMetadata().getName());

        ExecResult resultAfterDelete = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", command);
        assertThat(resultAfterDelete.out(), emptyString());

        TestUtils.waitFor("user " + userName + " will be deleted from Zookeeper", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            ExecResult zkResult = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c", zkListCommand);
            try {
                return !zkResult.out().contains(URLEncoder.encode(userName, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("Failed to encode username", e);
            }
        });
    }

    void createBigAmountOfUsers(String typeOfUser) {

        int numberOfUsers = 100;

        for (int i = 0; i < numberOfUsers; i++) {
            String userName = "alisa" + i;
            LOGGER.info("Creating user with name {}", userName);

            if (typeOfUser.equals("TLS")) {
                KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();
            } else {
                KafkaUserResource.scramShaUser(CLUSTER_NAME, userName).done();
            }

            LOGGER.info("Checking status of deployed Kafka User {}", userName);
            Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userName).get()
                    .getStatus().getConditions().get(0);
            LOGGER.info("Kafka User condition status: {}", kafkaCondition.getStatus());
            LOGGER.info("Kafka User condition type: {}", kafkaCondition.getType());
            assertThat(kafkaCondition.getType(), is("Ready"));
            LOGGER.info("Kafka User {} is in desired state: {}", userName, kafkaCondition.getType());
        }
    }

    private void deployTestSpecificResources() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1).done();
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();
        deployTestSpecificResources();
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) throws InterruptedException {
        super.recreateTestEnv(coNamespace, bindingsNamespaces);
        deployTestSpecificResources();
    }
}
