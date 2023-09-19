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
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.template.KafkaClusterTemplate;
import io.strimzi.api.kafka.model.template.KafkaClusterTemplateBuilder;
import io.strimzi.api.kafka.model.template.PodTemplate;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.KRaftWithoutUTONotSupported;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
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

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.ROLLING_UPDATE;
import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
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
    @KRaftWithoutUTONotSupported
    void testKafkaRollsWhenTopicIsUnderReplicated(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, extensionContext);
        final String clusterName = testStorage.getClusterName();
        final String topicName = testStorage.getTopicName();
        final String kafkaStsName = KafkaResources.kafkaStatefulSetName(clusterName);
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, kafkaStsName);

        Instant startTime = Instant.now();

        // We need to start with 3 replicas / brokers,
        // so that KafkaStreamsTopicStore topic gets set/distributed on this first 3 [0, 1, 2],
        // since this topic has replication-factor 3 and minISR 2.

        // We have disabled the broker scale down check for now since the test fails at the moment
        // due to partition replicas being present on the broker during scale down. We can enable this check
        // once the issue is resolved
        // https://github.com/strimzi/strimzi-kafka-operator/issues/9134
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3)
                .editMetadata()
                    .addToAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_SKIP_BROKER_SCALEDOWN_CHECK, "true"))
                .endMetadata()
                .editSpec()
                .editKafka()
                    .addToConfig("auto.create.topics.enable", "false")
                .endKafka()
            .endSpec()
            .build());

        LOGGER.info("Running kafkaScaleUpScaleDown {}", clusterName);
        final int initialReplicas = kubeClient(namespaceName).listPods(kafkaSelector).size();
        assertEquals(3, initialReplicas);

        // Now that KafkaStreamsTopicStore topic is set on the first 3 brokers, lets spin-up another one.
        int scaledUpReplicas = 4;

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(KafkaResource.getNodePoolName(clusterName), knp -> knp.getSpec().setReplicas(scaledUpReplicas), namespaceName);
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getKafka().setReplicas(scaledUpReplicas), namespaceName);
        }

        RollingUpdateUtils.waitForComponentScaleUpOrDown(namespaceName, kafkaSelector, scaledUpReplicas);

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName, 4, 4, 4, namespaceName).build());

        //Test that the new pod does not have errors or failures in events
        String uid = kubeClient(namespaceName).getPodUid(KafkaResource.getKafkaPodName(clusterName,  3));
        List<Event> events = kubeClient(namespaceName).listEventsByResourceUid(uid);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));

        // TODO scaling down of Kafka Cluster (with 4 replicas which has KafkaTopic with 4 replicas) can be forbidden in a future due to would-be Topic under-replication (https://github.com/strimzi/strimzi-kafka-operator/pull/9042)
        // scale down
        final int scaledDownReplicas = 3;
        LOGGER.info("Scaling down to {}", scaledDownReplicas);

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(KafkaResource.getNodePoolName(clusterName), knp -> knp.getSpec().setReplicas(scaledDownReplicas), namespaceName);
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getKafka().setReplicas(scaledDownReplicas), namespaceName);
        }

        Map<String, String> kafkaPods = RollingUpdateUtils.waitForComponentScaleUpOrDown(namespaceName, kafkaSelector, scaledDownReplicas);

        PodUtils.verifyThatRunningPodsAreStable(namespaceName, clusterName);

        // set annotation to trigger Kafka rolling update
        StrimziPodSetUtils.annotateStrimziPodSet(namespaceName, KafkaResource.getStrimziPodSetName(clusterName), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));

        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, scaledDownReplicas, kafkaPods);
        //Test that CO doesn't have any exceptions in log
        Instant endTime = Instant.now();
        long duration = Duration.between(startTime, endTime).toSeconds();
        assertNoCoErrorsLogged(namespaceName, duration);
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
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_MEDIUM)
            .createInstallation()
            .runInstallation();
    }
}
