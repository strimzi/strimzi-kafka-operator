/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.test.ClusterOperator;
import io.strimzi.test.JUnitGroup;
import io.strimzi.test.Namespace;
import io.strimzi.test.StrimziRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@RunWith(StrimziRunner.class)
@Namespace(UserST.NAMESPACE)
@ClusterOperator
public class UserST extends AbstractST {

    public static final String NAMESPACE = "user-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(UserST.class);

    @Test
    @JUnitGroup(name = "regression")
    public void testUpdateUser() {
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
                    .withNewKafkaAuthorizationSimpleAuthorization()
                    .endKafkaAuthorizationSimpleAuthorization()
                .endKafka()
            .endSpec().build()).done();

        KafkaUser user = resources().tlsUser(kafkaUser).done();
        kubeClient.waitForResourceCreation("secret", kafkaUser);

        String kafkaUserSecret = kubeClient.getResourceAsJson("secret", kafkaUser);
        Assert.assertThat(kafkaUserSecret, hasJsonPath("$.data['ca.crt']", notNullValue()));
        Assert.assertThat(kafkaUserSecret, hasJsonPath("$.data['user.crt']", notNullValue()));
        Assert.assertThat(kafkaUserSecret, hasJsonPath("$.data['user.key']", notNullValue()));
        Assert.assertThat(kafkaUserSecret, hasJsonPath("$.metadata.name", equalTo(kafkaUser)));
        Assert.assertThat(kafkaUserSecret, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));

        String kafkaUserAsJson = kubeClient.getResourceAsJson("KafkaUser", kafkaUser);

        Assert.assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(kafkaUser)));
        Assert.assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));
        Assert.assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo("tls")));

        kafkaUser = "new-" + kafkaUser;

        resources().user(user)
            .editMetadata()
                .withResourceVersion(null)
                .withName(kafkaUser)
            .endMetadata()
            .editSpec()
                .withNewKafkaUserScramSha512ClientAuthenticationAuthentication()
                .endKafkaUserScramSha512ClientAuthenticationAuthentication()
            .endSpec().done();

        kafkaUserSecret = kubeClient.getResourceAsJson("secret", kafkaUser);
        Assert.assertThat(kafkaUserSecret, hasJsonPath("$.data.password", notNullValue()));

        kafkaUserAsJson = kubeClient.getResourceAsJson("KafkaUser", kafkaUser);
        Assert.assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.name", equalTo(kafkaUser)));
        Assert.assertThat(kafkaUserAsJson, hasJsonPath("$.metadata.namespace", equalTo(NAMESPACE)));
        Assert.assertThat(kafkaUserAsJson, hasJsonPath("$.spec.authentication.type", equalTo("scram-sha-512")));

        kubeClient.deleteByName("KafkaUser", kafkaUser);
        kubeClient.waitForResourceDeletion("KafkaUser", kafkaUser);
    }
}
