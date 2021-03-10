/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.custom;


import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import kafka.security.authorizer.AclAuthorizer;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Tag(INTERNAL_CLIENTS_USED)
public class CustomAuthorizerST extends AbstractST {
    static final String NAMESPACE = "custom-authorizer-test";
    static final String CLUSTER_NAME = "custom-authorizer";
    static final String ADMIN = "sre-admin";

    @BeforeAll
    public void setup() {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        Map<String, Object> config = new HashMap<>();
        config.put("prop1", "value1");
        config.put("prop2", "value2");

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationCustom()
                      .withAuthorizerClass(AclAuthorizer.class.getName())
                      .withConfigProperties(config)
                      .withSuperUsers(ADMIN)
                    .endKafkaAuthorizationCustom()
                .endKafka()
            .endSpec()
            .build());

        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).build());
        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).build());
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testAuthorization() {
        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(USER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        // AclAuthorizer without user config will not allow any permission
        internalKafkaClient.sendMessagesPlain();
        int received = internalKafkaClient.receiveMessagesPlain();
        assertThat(received, is(0));
    }
}
