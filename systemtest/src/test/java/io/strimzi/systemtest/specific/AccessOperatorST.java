/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.kafka.access.model.KafkaAccessBuilder;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.access.SetupAccessOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.TestTags.REGRESSION;

@Tag(REGRESSION)
public class AccessOperatorST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(DrainCleanerST.class);

    @IsolatedTest
    void testAccessOperator() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String kafkaAccessName = testStorage.getClusterName() + "-access";

        LOGGER.info("Deploying Kafka Access Operator");
        SetupAccessOperator.install(testStorage.getNamespaceName());

        LOGGER.info("Deploying Kafka cluster and creating KafkaUser");
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .editKafka()
                        .withListeners(
                            new GenericKafkaListenerBuilder()
                                .withType(KafkaListenerType.INTERNAL)
                                .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                                .withPort(9092)
                                .withTls(false)
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withType(KafkaListenerType.INTERNAL)
                                .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build()
                        )
                    .endKafka()
                .endSpec()
                .build()
        );

        resourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        LOGGER.info("Creating KafkaAccess for collecting information about KafkaUser: {} for Kafka: {}", testStorage.getUsername(), testStorage.getClusterName());
        resourceManager.createResourceWithWait(new KafkaAccessBuilder()
            .editOrNewMetadata()
                .withName(kafkaAccessName)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editOrNewSpec()
                .withNewKafka()
                    .withName(testStorage.getClusterName())
                    .withNamespace(testStorage.getNamespaceName())
                    .withListener(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                .endKafka()
            .endSpec()
            .build()
        );

        LOGGER.info("asd");
    }

    @BeforeAll
    void setup() {
        clusterOperator = clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();
    }
}
