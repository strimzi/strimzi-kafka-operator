/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.TestDoc;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.kafka.access.model.KafkaAccessBuilder;
import io.strimzi.operator.common.Util;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.List;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

/**
 * Class for testing the usage of Kafka Access Operator together with operators and Kafka/KafkaUser CRs in real environment.
 */
@Tag(REGRESSION)
public class AccessOperatorST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(AccessOperatorST.class);

    @IsolatedTest
    @TestDoc(
        description = @Desc(
            "The `testAccessOperator` test verifies the functionality of Kafka Access Operator together with Kafka and KafkaUser CRs in a real environment. " +
            "It also verifies that with the credentials and information about the Kafka cluster, the Kafka clients are able to connect and do the message transmission"
        ),
        steps = {
            @Step(value = "Deploy Kafka Access Operator using the installation files from packaging folder.", expected = "Kafka Access Operator is successfully deployed."),
            @Step(value = "Deploy and create NodePools, Kafka with configured plain and TLS listeners, TLS KafkaUser, and KafkaTopic where we will produce the messages.", expected = "All of the resources are successfully deployed/created."),
            @Step(value = "Create KafkaAccess resource pointing to our Kafka cluster (and TLS listener) and TLS KafkaUser; Wait for KafkaAccess' Secret creation.", expected = "Both KafkaAccess resource and KafkaAccess' Secret is created."),
            @Step(value = "Collect KafkaAccess' Secret and its data.", expected = "Data successfully collected."),
            @Step(value = "Use the data from KafkaAccess Secret in producer and consumer clients, do message transmission.", expected = "With the data provided by KAO, both clients can connect to Kafka and do the message transmission.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA_ACCESS),
        }
    )
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
                .withNewUser()
                    .withName(testStorage.getUsername())
                    .withNamespace(testStorage.getNamespaceName())
                    .withApiGroup(KafkaUser.RESOURCE_GROUP)
                    .withKind(KafkaUser.RESOURCE_KIND)
                .endUser()
            .endSpec()
            .build()
        );

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
