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
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.template.KafkaClusterTemplate;
import io.strimzi.api.kafka.model.template.KafkaClusterTemplateBuilder;
import io.strimzi.api.kafka.model.template.PodTemplate;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.BeforeAllOnce;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.ROLLING_UPDATE;
import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
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
@IsolatedSuite
public class KafkaRollerIsolatedST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaRollerIsolatedST.class);

    @ParallelNamespaceTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testKafkaRollsWhenTopicIsUnderReplicated(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String kafkaStsName = KafkaResources.kafkaStatefulSetName(clusterName);
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, kafkaStsName);

        Instant startTime = Instant.now();

        // We need to start with 3 replicas / brokers,
        // so that KafkaStreamsTopicStore topic gets set/distributed on this first 3 [0, 1, 2],
        // since this topic has replication-factor 3 and minISR 2.
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3)
            .editSpec()
                .editKafka()
                    .addToConfig("auto.create.topics.enable", "false")
                .endKafka()
            .endSpec()
            .build());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);

        LOGGER.info("Running kafkaScaleUpScaleDown {}", clusterName);
        final int initialReplicas = kubeClient(namespaceName).listPods(kafkaSelector).size();
        assertEquals(3, initialReplicas);

        // Now that KafkaStreamsTopicStore topic is set on the first 3 brokers, lets spin-up another one.
        int scaledUpReplicas = 4;
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getKafka().setReplicas(scaledUpReplicas), namespaceName);

        kafkaPods = RollingUpdateUtils.waitForComponentScaleUpOrDown(namespaceName, kafkaSelector, scaledUpReplicas, kafkaPods);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName, 4, 4, 4).build());

        //Test that the new pod does not have errors or failures in events
        String uid = kubeClient(namespaceName).getPodUid(KafkaResources.kafkaPodName(clusterName,  3));
        List<Event> events = kubeClient(namespaceName).listEventsByResourceUid(uid);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));

        // scale down
        final int scaledDownReplicas = 3;
        LOGGER.info("Scaling down to {}", scaledDownReplicas);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getKafka().setReplicas(scaledDownReplicas), namespaceName);

        kafkaPods = RollingUpdateUtils.waitForComponentScaleUpOrDown(namespaceName, kafkaSelector, scaledDownReplicas, kafkaPods);

        PodUtils.verifyThatRunningPodsAreStable(namespaceName, clusterName);

        // set annotation to trigger Kafka rolling update
        StUtils.annotateStatefulSetOrStrimziPodSet(namespaceName, kafkaStsName, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));

        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, scaledDownReplicas, kafkaPods);
        //Test that CO doesn't have any exceptions in log
        Instant endTime = Instant.now();
        long duration = Duration.between(startTime, endTime).toSeconds();
        assertNoCoErrorsLogged(namespaceName, duration);
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testKafkaTopicRFLowerThanMinInSyncReplicas(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String kafkaName = KafkaResources.kafkaStatefulSetName(clusterName);
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, kafkaName);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName, 1, 1).build());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);

        LOGGER.info("Setting KafkaTopic's min.insync.replicas to be higher than replication factor");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().getConfig().replace("min.insync.replicas", 2), namespaceName);

        // rolling update for kafka
        LOGGER.info("Annotate Kafka {} {} with manual rolling update annotation",
            Environment.isStrimziPodSetEnabled() ? StrimziPodSet.RESOURCE_KIND : Constants.STATEFUL_SET, kafkaName);

        // set annotation to trigger Kafka rolling update
        StUtils.annotateStatefulSetOrStrimziPodSet(namespaceName, kafkaName, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));

        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, 3, kafkaPods);
        assertThat(PodUtils.podSnapshot(namespaceName, kafkaSelector), is(not(kafkaPods)));
    }

    @ParallelNamespaceTest
    void testKafkaPodCrashLooping(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withNewJvmOptions()
                        .withXx(Collections.emptyMap())
                    .endJvmOptions()
                .endKafka()
            .endSpec()
            .build());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka ->
                kafka.getSpec().getKafka().getJvmOptions().setXx(Collections.singletonMap("UseParNewGC", "true")), namespaceName);

        KafkaUtils.waitForKafkaNotReady(namespaceName, clusterName);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka ->
                kafka.getSpec().getKafka().getJvmOptions().setXx(Collections.emptyMap()), namespaceName);

        // kafka should get back ready in some reasonable time frame.
        // Current timeout for wait is set to 14 minutes, which should be enough.
        // No additional checks are needed, because in case of wait failure, the test will not continue.
        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);
    }

    @ParallelNamespaceTest
    void testKafkaPodImagePullBackOff(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3).build());

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
    public void testKafkaPodPending(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        ResourceRequirements rr = new ResourceRequirementsBuilder()
            .withRequests(Collections.emptyMap())
            .build();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withResources(rr)
                .endKafka()
            .endSpec()
            .build());

        Map<String, Quantity> requests = new HashMap<>(2);
        requests.put("cpu", new Quantity("123456"));
        requests.put("memory", new Quantity("128Mi"));
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka ->
                kafka.getSpec().getKafka().getResources().setRequests(requests), namespaceName);

        KafkaUtils.waitForKafkaNotReady(namespaceName, clusterName);

        assertTrue(checkIfExactlyOneKafkaPodIsNotReady(namespaceName, clusterName));

        requests.put("cpu", new Quantity("100m"));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka ->
                kafka.getSpec().getKafka().getResources().setRequests(requests), namespaceName);

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
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

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

        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withTemplate(kct)
                .endKafka()
            .endSpec()
            .build());

        // pods are stable in the Pending state
        PodUtils.waitUntilPodStabilityReplicasCount(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName), 3);

        LOGGER.info("Removing requirement for the affinity");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka ->
                kafka.getSpec().getKafka().getTemplate().getPod().setAffinity(null), namespaceName);

        // kafka should get back ready in some reasonable time frame
        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);
        KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaUtils.waitForKafkaDeletion(namespaceName, clusterName);
    }

    boolean checkIfExactlyOneKafkaPodIsNotReady(String namespaceName, String clusterName) {
        List<Pod> kafkaPods = kubeClient(namespaceName).listPodsByPrefixInName(KafkaResources.kafkaStatefulSetName(clusterName));
        int runningKafkaPods = (int) kafkaPods.stream().filter(pod -> pod.getStatus().getPhase().equals("Running")).count();

        return runningKafkaPods == (kafkaPods.size() - 1);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
            .withNamespace(INFRA_NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_MEDIUM)
            .createInstallation()
            .runInstallation();
    }
}
