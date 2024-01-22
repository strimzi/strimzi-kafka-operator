/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.migration;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

public class MigrationST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MigrationST.class);
    private static final String CONTINUOUS_SUFFIX = "-continuous";

    @IsolatedTest
    void testMigrationFromZkToKRaft(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext);

        String midStepTopicName = testStorage.getTopicName() + "-midstep";
        String kraftTopicName = testStorage.getTopicName() + "-kraft";

        String continuousTopicName = testStorage.getTopicName() + CONTINUOUS_SUFFIX;
        String continuousProducerName = testStorage.getProducerName() + CONTINUOUS_SUFFIX;
        String continuousConsumerName = testStorage.getConsumerName() + CONTINUOUS_SUFFIX;
        int continuousMessageCount = 500;

        LabelSelector brokerSelector = KafkaNodePoolResource.getLabelSelector(testStorage.getBrokerPoolName(), ProcessRoles.BROKER);
        LabelSelector controllerSelector = KafkaNodePoolResource.getLabelSelector(testStorage.getControllerPoolName(), ProcessRoles.CONTROLLER);

        String clientsAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";

        KafkaClients immediateClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        KafkaClients continuousClients = new KafkaClientsBuilder()
            .withProducerName(continuousProducerName)
            .withConsumerName(continuousConsumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(continuousTopicName)
            .withMessageCount(continuousMessageCount)
            .withDelayMs(1000)
            .withAdditionalConfig(clientsAdditionConfiguration)
            .build();

        LOGGER.info("Deploying Kafka resource with Broker NodePool");

        // create Kafka resource with ZK and Broker NodePool
        resourceManager.createResourceWithWait(extensionContext,
            KafkaNodePoolTemplates.kafkaNodePoolWithBrokerRoleAndPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "disabled")
                .endMetadata()
                .build());

        Map<String, String> brokerPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), brokerSelector);

        // at this moment, everything should be ready, so we should ideally create some topics and send + receive the messages (to have some data present in Kafka + metadata about topics in ZK)
        LOGGER.info("Creating two topics for immediate and continuous message transmission");
        resourceManager.createResourceWithWait(extensionContext,
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), continuousTopicName, testStorage.getNamespaceName()).build());

        // sanity check that kafkaMetadataState shows ZooKeeper
        String currentKafkaMetadataState = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getKafkaMetadataState();
        assertThat(currentKafkaMetadataState, is("ZooKeeper"));

        // start continuous clients and do the immediate message transmission
        resourceManager.createResourceWithWait(extensionContext,
            continuousClients.producerStrimzi(),
            continuousClients.consumerStrimzi(),
            immediateClients.producerTlsStrimzi(testStorage.getClusterName()),
            immediateClients.consumerTlsStrimzi(testStorage.getClusterName())
        );

        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());

        // starting the migration
        LOGGER.info("Starting the migration process: 1.creating controller NodePool");
        // the controller pods will not be up and running, because we are using the ZK nodes as controllers, they will be created once the migration starts
        resourceManager.createResourceWithoutWait(extensionContext,
            KafkaNodePoolTemplates.kafkaNodePoolWithControllerRoleAndPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build());

        LOGGER.info("1. applying the {} annotation with value: {}", Annotations.ANNO_STRIMZI_IO_KRAFT, "migration");
        KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration");

        LOGGER.info("2. waiting for controller Pods to be up and running");
        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, true);
        Map<String, String> controllerPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), controllerSelector);

        LOGGER.info("3. waiting for first rolling update of broker Pods");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerPodsSnapshot);

        LOGGER.info("4. waiting for second rolling update of broker Pods");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerPodsSnapshot);

        currentKafkaMetadataState = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getKafkaMetadataState();
        assertThat(currentKafkaMetadataState, is("KRaftPostMigration"));

        LOGGER.info("5. creating KafkaTopic: {} and checking if the metadata are in both ZK and KRaft", midStepTopicName);
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), midStepTopicName, testStorage.getNamespaceName()).build());

        LOGGER.info("5a. checking if metadata about KafkaTopic: {} are in ZK", midStepTopicName);

        // TODO: write check here

        LOGGER.info("5b. checking if metadata about KafkaTopic: {} are in KRaft controller", midStepTopicName);

        // TODO: write check here

        LOGGER.info("5c. checking if we are able to do a message transmission on KafkaTopic: {}", midStepTopicName);

        immediateClients = new KafkaClientsBuilder(immediateClients)
            .withTopicName(midStepTopicName)
            .build();

        resourceManager.createResourceWithWait(extensionContext,
            immediateClients.producerTlsStrimzi(testStorage.getClusterName()),
            immediateClients.consumerTlsStrimzi(testStorage.getClusterName())
        );

        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());

        LOGGER.info("5. finishing migration - applying {}: enabled annotation, controllers should be rolled", Annotations.ANNO_STRIMZI_IO_KRAFT);
        KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled");

        controllerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, controllerPodsSnapshot);

        currentKafkaMetadataState = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getKafkaMetadataState();
        assertThat(currentKafkaMetadataState, is("KRaft"));

        LOGGER.info("6. removing LMFV and IBPV from Kafka config -> brokers and controllers should be rolled");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().getConfig().remove("log.message.format.version");
            kafka.getSpec().getKafka().getConfig().remove("inter.broker.protocol.version");
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerPodsSnapshot);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, controllerPodsSnapshot);

        LOGGER.info("7. creating KafkaTopic: {} and checking if the metadata are only in KRaft", kraftTopicName);
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), kraftTopicName, testStorage.getNamespaceName()).build());

        LOGGER.info("7a. checking if metadata about KafkaTopic: {} are not in ZK", kraftTopicName);

        // TODO: write check here

        LOGGER.info("7b. checking if metadata about KafkaTopic: {} are in KRaft controller", kraftTopicName);

        // TODO: write check here

        LOGGER.info("7c. checking if we are able to do a message transmission on KafkaTopic: {}", kraftTopicName);

        immediateClients = new KafkaClientsBuilder(immediateClients)
            .withTopicName(kraftTopicName)
            .build();

        resourceManager.createResourceWithWait(extensionContext,
            immediateClients.producerTlsStrimzi(testStorage.getClusterName()),
            immediateClients.consumerTlsStrimzi(testStorage.getClusterName())
        );

        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());

        LOGGER.info("8. migration is completed, waiting for continuous clients to finish");
        ClientUtils.waitForClientsSuccess(continuousProducerName, continuousConsumerName, testStorage.getNamespaceName(), continuousMessageCount);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();
    }
}
