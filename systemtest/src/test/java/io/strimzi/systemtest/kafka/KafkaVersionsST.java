/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static io.strimzi.systemtest.Constants.KAFKA_SMOKE;

@ParallelSuite
@Tag(KAFKA_SMOKE)
public class KafkaVersionsST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(KafkaVersionsST.class);

    @ParameterizedTest(name = "Kafka version: {0}.version()")
    @MethodSource("io.strimzi.systemtest.utils.TestKafkaVersion#getSupportedKafkaVersions")
    void testKafkaWithVersion(TestKafkaVersion testKafkaVersion, ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        LOGGER.info("Deploying Kafka with version: {}", testKafkaVersion.version());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
            .editOrNewSpec()
                .editOrNewKafka()
                    .withVersion(testKafkaVersion.version())
                    .addToConfig("inter.broker.protocol.version", testKafkaVersion.protocolVersion())
                    .addToConfig("log.message.format.version", testKafkaVersion.messageVersion())
                .endKafka()
            .endSpec()
            .build()
        );

        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(testStorage).build());

        LOGGER.info("Sending and receiving messages via PLAIN");

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );

        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Sending and receiving messages via TLS");

        kafkaClients = new KafkaClientsBuilder(kafkaClients)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withUserName(testStorage.getUserName())
            .build();

        resourceManager.createResource(extensionContext,
            kafkaClients.producerTlsStrimzi(testStorage.getClusterName()),
            kafkaClients.consumerTlsStrimzi(testStorage.getClusterName())
        );

        ClientUtils.waitForClientsSuccess(testStorage);
    }
}
