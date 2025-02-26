/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.common.Util;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.access.SetupAccessOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

@Tag(REGRESSION)
public class AccessOperatorST extends AbstractST {
    private static final String ACCESS_EXAMPLE_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/kafka-access/kafka-access.yaml";
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
        // TODO: once the `api` module of the Kafka Access Operator is released publicly, we should use the KafkaAccessBuilder instead of having a file like this
        try {
            final String kafkaAccessYaml =  Files.readString(Paths.get(ACCESS_EXAMPLE_PATH))
                .replace("CLUSTER_NAME", testStorage.getClusterName())
                .replace("NAMESPACE_NAME", testStorage.getNamespaceName());

            cmdKubeClient(testStorage.getNamespaceName()).applyContent(kafkaAccessYaml);
        } catch (IOException e) {
            throw new RuntimeException("Failed to update the Postgres deployment YAML", e);
        }

        SecretUtils.waitForSecretReady(testStorage.getNamespaceName(), kafkaAccessName, () -> { });

        Secret accessSecret = kubeClient().getSecret(testStorage.getNamespaceName(), kafkaAccessName);
        String bootstrapServer = Util.decodeFromBase64(accessSecret.getData().get("bootstrapServers"));

        List<EnvVar> tlsEnvVarsForKafkaAccess = List.of(
            new EnvVarBuilder()
                .withName("CA_CRT")
                .withNewValueFrom()
                    .withNewSecretKeyRef()
                        .withName(kafkaAccessName)
                        .withKey("ssl.truststore.crt")
                    .endSecretKeyRef()
                .endValueFrom()
                .build(),
            new EnvVarBuilder()
                .withName("USER_CRT")
                .withNewValueFrom()
                    .withNewSecretKeyRef()
                        .withName(kafkaAccessName)
                        .withKey("ssl.keystore.crt")
                    .endSecretKeyRef()
                .endValueFrom()
                .build(),
            new EnvVarBuilder()
                .withName("USER_KEY")
                .withNewValueFrom()
                    .withNewSecretKeyRef()
                        .withName(kafkaAccessName)
                        .withKey("ssl.keystore.key")
                    .endSecretKeyRef()
                .endValueFrom()
                .build()
        );

        final KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withBootstrapAddress(bootstrapServer)
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(TestConstants.MESSAGE_COUNT)
            .withTopicName(testStorage.getTopicName())
            .build();

        LOGGER.info("Deploying Kafka clients with configured TLS using the KafkaAccess' secret");
        resourceManager.createResourceWithWait(
            kafkaClients.producerTlsStrimziWithTlsEnvVars(tlsEnvVarsForKafkaAccess),
            kafkaClients.consumerTlsStrimziWithTlsEnvVars(tlsEnvVarsForKafkaAccess)
        );

        LOGGER.info("Verifying successful message transmission");
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @BeforeAll
    void setup() {
        clusterOperator = clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();
    }
}
