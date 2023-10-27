/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.rollingupdate;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.template.KafkaClusterTemplate;
import io.strimzi.api.kafka.model.template.KafkaClusterTemplateBuilder;
import io.strimzi.api.kafka.model.template.PodTemplate;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.KRaftWithoutUTONotSupported;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.ROLLING_UPDATE;
import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils.waitForPodsReady;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
@Tag(ROLLING_UPDATE)
public class KafkaRollerST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaRollerST.class);

    @ParallelNamespaceTest
    @KRaftNotSupported
    void testKafkaDoesNotRollsWhenTopicIsUnderReplicated(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        Instant startTime = Instant.now();

        final int initialBrokerReplicaCount = 3;
        final int scaledUpBrokerReplicaCount = 4;

        final String topicNameWith3Replicas = testStorage.getTopicName() + "-3";
        final String topicNameWith4Replicas = testStorage.getTopicName() + "-4";

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), initialBrokerReplicaCount).build());

        LOGGER.info("Verify expected number of replicas '{}' is present in in Kafka Cluster: {}/{}", initialBrokerReplicaCount, testStorage.getNamespaceName(), testStorage.getClusterName());
        final int observedReplicas = kubeClient(testStorage.getNamespaceName()).listPods(testStorage.getKafkaSelector()).size();
        assertEquals(initialBrokerReplicaCount, observedReplicas);

        LOGGER.info("Create kafkaTopic: {}/{} with replica on each (of 3) broker", testStorage.getNamespaceName(), topicNameWith3Replicas);
        KafkaTopic kafkaTopicWith3Replicas = KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 1, 3, 3, testStorage.getNamespaceName()).build();
        resourceManager.createResourceWithWait(extensionContext, kafkaTopicWith3Replicas);

        // setup clients
        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(topicNameWith3Replicas)
            .withMessageCount(testStorage.getMessageCount())
            .withNamespaceName(testStorage.getNamespaceName())
            .build();

        // producing and consuming data when there are 3 brokers ensures that 'consumer_offests' topic will have all of its replicas only across first 3 brokers
        LOGGER.info("Producing and Consuming messages with clients: {}, {} in Namespace {}", testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName());
        resourceManager.createResourceWithWait(extensionContext,
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Scale Kafka up from 3 to 4 brokers");
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(KafkaResource.getNodePoolName(testStorage.getClusterName()), knp -> knp.getSpec().setReplicas(scaledUpBrokerReplicaCount), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getKafka().setReplicas(scaledUpBrokerReplicaCount), testStorage.getNamespaceName());
        }
        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), scaledUpBrokerReplicaCount);

        LOGGER.info("Create kafkaTopic: {}/{} with replica on each broker", testStorage.getNamespaceName(), topicNameWith4Replicas);
        KafkaTopic kafkaTopicWith4Replicas = KafkaTopicTemplates.topic(testStorage.getClusterName(), topicNameWith4Replicas, 1, 4, 4, testStorage.getNamespaceName()).build();
        resourceManager.createResourceWithWait(extensionContext, kafkaTopicWith4Replicas);

        //Test that the new pod does not have errors or failures in events
        String uid = kubeClient(testStorage.getNamespaceName()).getPodUid(KafkaResource.getKafkaPodName(testStorage.getClusterName(),  3));
        List<Event> events = kubeClient(testStorage.getNamespaceName()).listEventsByResourceUid(uid);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(topicNameWith4Replicas)
            .build();

        LOGGER.info("Producing and Consuming messages with clients: {}, {} in Namespace {}", testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName());
        resourceManager.createResourceWithWait(extensionContext,
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Scaling down to {}", initialBrokerReplicaCount);
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(KafkaResource.getNodePoolName(testStorage.getClusterName()), knp -> knp.getSpec().setReplicas(initialBrokerReplicaCount), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getKafka().setReplicas(initialBrokerReplicaCount), testStorage.getNamespaceName());
        }

        LOGGER.info("Waiting for warning regarding preventing Kafka from scaling down when the broker to be scaled down have some partitions");
        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(testStorage.getClusterName(), testStorage.getNamespaceName(), "Cannot scale down broker.*");
        waitForPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), scaledUpBrokerReplicaCount, false);

        LOGGER.info("Remove Topic, thereby remove all partitions located on broker to be scaled down");
        resourceManager.deleteResource(kafkaTopicWith4Replicas);
        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), initialBrokerReplicaCount);

        //Test that CO doesn't have any exceptions in log
        Instant endTime = Instant.now();
        long duration = Duration.between(startTime, endTime).toSeconds();
        assertNoCoErrorsLogged(testStorage.getNamespaceName(), duration);
    }

    @ParallelNamespaceTest
    @KRaftWithoutUTONotSupported
    void testKafkaTopicRFLowerThanMinInSyncReplicas(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, extensionContext);
        final String clusterName = testStorage.getClusterName();
        final String topicName = testStorage.getTopicName();
        final String kafkaName = KafkaResources.kafkaStatefulSetName(clusterName);
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, kafkaName);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3).build());
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName, 1, 1, namespaceName).build());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);

        LOGGER.info("Setting KafkaTopic's min.insync.replicas to be higher than replication factor");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().getConfig().replace("min.insync.replicas", 2), namespaceName);

        // rolling update for kafka
        LOGGER.info("Annotate Kafka {} {} with manual rolling update annotation", StrimziPodSet.RESOURCE_KIND, kafkaName);

        // set annotation to trigger Kafka rolling update
        StrimziPodSetUtils.annotateStrimziPodSet(namespaceName, KafkaResource.getStrimziPodSetName(clusterName), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));

        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, 3, kafkaPods);
        assertThat(PodUtils.podSnapshot(namespaceName, kafkaSelector), is(not(kafkaPods)));
    }

    @ParallelNamespaceTest
    void testKafkaPodCrashLooping(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, extensionContext);
        final String clusterName = testStorage.getClusterName();

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withNewJvmOptions()
                        .withXx(Collections.emptyMap())
                    .endJvmOptions()
                .endKafka()
            .endSpec()
            .build());

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(KafkaResource.getNodePoolName(clusterName), knp ->
                knp.getSpec().getJvmOptions().setXx(Collections.singletonMap("UseParNewGC", "true")), namespaceName);
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka ->
                kafka.getSpec().getKafka().getJvmOptions().setXx(Collections.singletonMap("UseParNewGC", "true")), namespaceName);
        }

        KafkaUtils.waitForKafkaNotReady(namespaceName, clusterName);

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(KafkaResource.getNodePoolName(clusterName), knp ->
                knp.getSpec().getJvmOptions().setXx(Collections.emptyMap()), namespaceName);
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka ->
                kafka.getSpec().getKafka().getJvmOptions().setXx(Collections.emptyMap()), namespaceName);
        }

        // kafka should get back ready in some reasonable time frame.
        // Current timeout for wait is set to 14 minutes, which should be enough.
        // No additional checks are needed, because in case of wait failure, the test will not continue.
        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);
    }

    @ParallelNamespaceTest
    void testKafkaPodImagePullBackOff(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, extensionContext);
        final String clusterName = testStorage.getClusterName();
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3).build());

        String kafkaImage = kubeClient(namespaceName).listPods(kafkaSelector).get(0).getSpec().getContainers().get(0).getImage();

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setImage("quay.io/strimzi/kafka:not-existent-tag");
            kafka.getSpec().getZookeeper().setImage(kafkaImage);
        }, namespaceName);

        KafkaUtils.waitForKafkaNotReady(namespaceName, clusterName);

        assertTrue(checkIfExactlyOneKafkaPodIsNotReady(namespaceName, clusterName));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> kafka.getSpec().getKafka().setImage(kafkaImage), namespaceName);

        // kafka should get back ready in some reasonable time frame.
        // Current timeout for wait is set to 14 minutes, which should be enough.
        // No additional checks are needed, because in case of wait failure, the test will not continue.
        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);
    }

    @ParallelNamespaceTest
    void testKafkaPodPendingDueToRack(ExtensionContext extensionContext) {
        // Testing this scenario
        // 1. deploy Kafka with wrong pod template (looking for nonexistent node) kafka pods should not exist
        // 2. wait for Kafka not ready, kafka pods should be in the pending state
        // 3. fix the Kafka CR, kafka pods should be in the pending state
        // 4. wait for Kafka ready, kafka pods should NOT be in the pending state
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, extensionContext);
        final String clusterName = testStorage.getClusterName();

        NodeSelectorRequirement nsr = new NodeSelectorRequirementBuilder()
                .withKey("dedicated_test")
                .withOperator("In")
                .withValues("Kafka")
                .build();

        NodeSelectorTerm nst = new NodeSelectorTermBuilder()
                .withMatchExpressions(nsr)
                .build();

        Affinity affinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(nst)
                    .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        PodTemplate pt = new PodTemplate();
        pt.setAffinity(affinity);

        KafkaClusterTemplate kct = new KafkaClusterTemplateBuilder()
                .withPod(pt)
                .build();

        resourceManager.createResourceWithoutWait(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withTemplate(kct)
                .endKafka()
            .endSpec()
            .build());

        // pods are stable in the Pending state
        PodUtils.waitUntilPodStabilityReplicasCount(namespaceName, KafkaResource.getStrimziPodSetName(clusterName), 3);

        LOGGER.info("Removing requirement for the affinity");
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(KafkaResource.getNodePoolName(clusterName), knp ->
                knp.getSpec().getTemplate().getPod().setAffinity(null), namespaceName);
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka ->
                kafka.getSpec().getKafka().getTemplate().getPod().setAffinity(null), namespaceName);
        }

        // kafka should get back ready in some reasonable time frame
        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);
        KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaUtils.waitForKafkaDeletion(namespaceName, clusterName);
    }

    boolean checkIfExactlyOneKafkaPodIsNotReady(String namespaceName, String clusterName) {
        List<Pod> kafkaPods = kubeClient(namespaceName).listPodsByPrefixInName(KafkaResource.getStrimziPodSetName(clusterName));
        int runningKafkaPods = (int) kafkaPods.stream().filter(pod -> pod.getStatus().getPhase().equals("Running")).count();

        return runningKafkaPods == (kafkaPods.size() - 1);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator.defaultInstallation(extensionContext)
            .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_MEDIUM)
            .createInstallation()
            .runInstallation();
    }
}
