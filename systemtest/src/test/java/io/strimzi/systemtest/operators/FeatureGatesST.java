/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.annotations.IsolatedTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.CO_NAMESPACE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Feature Gates should give us additional options on
 * how to control and mature different behaviors in the operators.
 * https://github.com/strimzi/proposals/blob/main/022-feature-gates.md
 */
@Tag(REGRESSION)
public class FeatureGatesST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(FeatureGatesST.class);

    /**
     * UseKRaft feature gate
     */
    @IsolatedTest("Feature Gates test for enabled UseKRaft gate")
    @Tag(INTERNAL_CLIENTS_USED)
    public void testKRaftMode(ExtensionContext extensionContext) {
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());

        final TestStorage testStorage = new TestStorage(extensionContext);

        final String clusterName = testStorage.getClusterName();
        final String producerName = testStorage.getProducerName();
        final String consumerName = testStorage.getConsumerName();
        final String topicName = testStorage.getTopicName();

        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        int messageCount = 180;
        List<EnvVar> testEnvVars = new ArrayList<>();
        int kafkaReplicas = 3;

        testEnvVars.add(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, "+UseKRaft", null));

        this.clusterOperator = this.clusterOperator.defaultInstallation(extensionContext)
            .withNamespace(Constants.CO_NAMESPACE)
            .withBindingsNamespaces(Arrays.asList(Constants.CO_NAMESPACE, Environment.TEST_SUITE_NAMESPACE))
            .withExtraEnvVars(testEnvVars)
            .createInstallation()
            .runInstallation();

        Kafka kafka = KafkaTemplates.kafkaPersistent(clusterName, kafkaReplicas)
            .editOrNewMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .editKafka()
                .withListeners(
                        new GenericKafkaListenerBuilder()
                                .withType(KafkaListenerType.INTERNAL)
                                .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                                .withPort(9092)
                                .withTls(false)
                                .build(),
                        new GenericKafkaListenerBuilder()
                                .withType(KafkaListenerType.INTERNAL)
                                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build()
                )
                .endKafka()
            .endSpec()
            .build();
        kafka.getSpec().getEntityOperator().setTopicOperator(null); // The builder cannot disable the EO. It has to be done this way.

        KafkaNodePool kafkaNodePool = KafkaNodePoolResource.convertKafkaResourceToKafkaNodePool(kafka);
        kafkaNodePool = new KafkaNodePoolBuilder(kafkaNodePool)
            .editOrNewMetadata()
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, testStorage.getClusterName())
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editOrNewSpec()
                .addToRoles(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(extensionContext,
            kafkaNodePool,
            kafka
        );

        resourceManager.createResourceWithWait(extensionContext,
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        LOGGER.info("Trying to send some messages to Kafka in next few minutes");

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withUsername(testStorage.getUsername())
            .withTopicName(topicName)
            .withMessageCount(messageCount)
            .withDelayMs(500)
            .withNamespaceName(testStorage.getNamespaceName())
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerTlsStrimzi(clusterName));
        resourceManager.createResourceWithWait(extensionContext, kafkaClients.consumerTlsStrimzi(clusterName));

        // Check that there is no ZooKeeper
        Map<String, String> zkPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), zkSelector);
        assertThat("No ZooKeeper Pods should exist", zkPods.size(), is(0));

        // Roll Kafka
        LOGGER.info("Forcing rolling update of Kafka via read-only configuration change");
        Map<String, String> kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), kafkaSelector);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getKafka().getConfig().put("log.retention.hours", 72), testStorage.getNamespaceName());

        LOGGER.info("Waiting for the next reconciliation to happen");
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), kafkaSelector, kafkaReplicas, kafkaPods);

        LOGGER.info("Waiting for clients to finish sending/receiving messages");
        ClientUtils.waitForClientsSuccess(producerName, consumerName, testStorage.getNamespaceName(), MESSAGE_COUNT);
    }

    /**
     * @description This test case verifies basic working of Kafka Cluster managed by Cluster Operator with kafkaNodePool feature gate enabled.
     *
     * @steps
     *  1. - Deploy Kafka with annotated to enable management by KafkaNodePool, and KafkaNodePool targeting given Kafka Cluster.
     *     - Kafka is deployed, KafkaNodePool custom resource is targeting Kafka Cluster as expected.
     *  2. - Produce and consume messages in given Kafka Cluster.
     *     - Clients can produce and consume messages.
     *  3. - Trigger manual Rolling Update.
     *     - Rolling update is triggered and completed shortly after.
     *
     * @usecase
     *  - kafka-node-pool
     */
    @IsolatedTest
    void testKafkaNodePoolFeatureGate(ExtensionContext extensionContext) {
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());

        final TestStorage testStorage = new TestStorage(extensionContext, CO_NAMESPACE);

        List<EnvVar> coEnvVars = new ArrayList<>();
        coEnvVars.add(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, "+KafkaNodePools", null));
        
        clusterOperator = this.clusterOperator.defaultInstallation(extensionContext)
            .withExtraEnvVars(coEnvVars)
            .createInstallation()
            .runInstallation();

        LOGGER.info("Deploying Kafka Cluster: {}/{} controlled by KafkaNodePool: {}", testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getKafkaNodePoolName());
        Kafka kafkaCr = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 1)
            .editOrNewMetadata()
                .withNamespace(testStorage.getNamespaceName())
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
            .endMetadata()
            .build();

        KafkaNodePool kafkaNodePoolCr =  KafkaNodePoolTemplates.defaultKafkaNodePool(testStorage.getNamespaceName(), testStorage.getKafkaNodePoolName(), testStorage.getClusterName(), 3)
            .editOrNewMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editOrNewSpec()
                .addToRoles(ProcessRoles.BROKER)
                .withStorage(kafkaCr.getSpec().getKafka().getStorage())
                .withJvmOptions(kafkaCr.getSpec().getKafka().getJvmOptions())
                .withResources(kafkaCr.getSpec().getKafka().getResources())
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaNodePoolCr);
        resourceManager.createResourceWithWait(extensionContext, kafkaCr);

        // setup clients
        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withDelayMs(500)
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .build();

        LOGGER.info("Producing and Consuming messages with clients: {}, {} in Namespace {}", testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName());
        resourceManager.createResourceWithWait(extensionContext,
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        // snapshot Kafka Pods before triggering manual rolling update.
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(testStorage.getClusterName(), KafkaResources.kafkaStatefulSetName(testStorage.getClusterName()));
        Map<String, String> kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), kafkaSelector);

        LOGGER.info("Annotating {} of Kafka Cluster: {}/{} with manual rolling update annotation", StrimziPodSet.RESOURCE_KIND, testStorage.getNamespaceName(), testStorage.getClusterName());
        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), testStorage.getClusterName() + "-" + testStorage.getKafkaNodePoolName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), kafkaSelector, 3, kafkaPods);

    }
}
