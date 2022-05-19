/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.NodeAffinity;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodAffinityTerm;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.systemtest.AbstractST;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.REGRESSION;

import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

@Tag(REGRESSION)
@ParallelSuite
class RackAwarenessST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(RackAwarenessST.class);
    private static final String TOPOLOGY_KEY = "kubernetes.io/hostname";

    @ParallelNamespaceTest
    void testKafkaRackAwareness(ExtensionContext extensionContext) {
        Assumptions.assumeFalse(Environment.isNamespaceRbacScope());

        TestStorage storage = storageMap.get(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(storage.getClusterName(), 1, 1)
                .editSpec()
                    .editKafka()
                        .withNewRack(TOPOLOGY_KEY)
                        .addToConfig("replica.selector.class", "org.apache.kafka.common.replica.RackAwareReplicaSelector")
                    .endKafka()
                .endSpec()
                .build());

        LOGGER.info("Kafka cluster deployed successfully");
        String ssName = KafkaResources.kafkaStatefulSetName(storage.getClusterName());
        String podName = PodUtils.getPodNameByPrefix(storage.getNamespaceName(), ssName);
        Pod pod = kubeClient().getPod(storage.getNamespaceName(), podName);

        // check that spec matches the actual pod configuration
        Affinity specAffinity = StUtils.getStatefulSetOrStrimziPodSetAffinity(storage.getNamespaceName(), KafkaResources.kafkaStatefulSetName(storage.getClusterName()));
        NodeSelectorRequirement specNodeRequirement = specAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        NodeAffinity podAffinity = pod.getSpec().getAffinity().getNodeAffinity();
        NodeSelectorRequirement podNodeRequirement = podAffinity.getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        assertThat(podNodeRequirement, is(specNodeRequirement));
        assertThat(specNodeRequirement.getKey(), is(TOPOLOGY_KEY));
        assertThat(specNodeRequirement.getOperator(), is("Exists"));

        PodAffinityTerm specPodAntiAffinityTerm = specAffinity.getPodAntiAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().get(0).getPodAffinityTerm();
        PodAffinityTerm podAntiAffinityTerm = pod.getSpec().getAffinity().getPodAntiAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().get(0).getPodAffinityTerm();
        assertThat(podAntiAffinityTerm, is(specPodAntiAffinityTerm));
        assertThat(specPodAntiAffinityTerm.getTopologyKey(), is(TOPOLOGY_KEY));
        assertThat(specPodAntiAffinityTerm.getLabelSelector().getMatchLabels(), hasEntry("strimzi.io/cluster", storage.getClusterName()));
        assertThat(specPodAntiAffinityTerm.getLabelSelector().getMatchLabels(), hasEntry("strimzi.io/name", KafkaResources.kafkaStatefulSetName(storage.getClusterName())));

        // check Kafka rack awareness configuration
        String podNodeName = pod.getSpec().getNodeName();
        String hostname = podNodeName.contains(".") ? podNodeName.substring(0, podNodeName.indexOf(".")) : podNodeName;

        String rackIdOut = cmdKubeClient(storage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(storage.getClusterName(), 0),
                "/bin/bash", "-c", "cat /opt/kafka/init/rack.id").out().trim();
        String brokerRackOut = cmdKubeClient(storage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(storage.getClusterName(), 0),
                "/bin/bash", "-c", "cat /tmp/strimzi.properties | grep broker.rack").out().trim();
        assertThat(rackIdOut.trim(), is(hostname));
        assertThat(brokerRackOut.contains("broker.rack=" + hostname), is(true));
    }

    @Tag(CONNECT)
    @ParallelNamespaceTest
    void testConnectRackAwareness(ExtensionContext extensionContext) {
        Assumptions.assumeFalse(Environment.isNamespaceRbacScope());

        TestStorage storage = storageMap.get(extensionContext);
        String invalidTopologyKey = "invalid-topology-key";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(storage.getClusterName(), 1, 1).build());

        LOGGER.info("Deploy KafkaConnect with an invalid topology key: {}", invalidTopologyKey);
        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnect(extensionContext, storage.getClusterName(), 1)
                .editSpec()
                    .withNewRack(invalidTopologyKey)
                .endSpec()
                .build());

        LOGGER.info("Check that KafkaConnect pod is unschedulable");
        KafkaConnectUtils.waitForConnectPodCondition("Unschedulable", storage.getNamespaceName(), storage.getClusterName(), 30_000);

        LOGGER.info("Fix KafkaConnect with a valid topology key: {}", TOPOLOGY_KEY);
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(storage.getClusterName(), kc -> kc.getSpec().setRack(new Rack(TOPOLOGY_KEY)), storage.getNamespaceName());
        KafkaConnectUtils.waitForConnectReady(storage.getNamespaceName(), storage.getClusterName());

        LOGGER.info("KafkaConnect cluster deployed successfully");
        String deployName = KafkaConnectResources.deploymentName(storage.getClusterName());
        String podName = PodUtils.getPodNameByPrefix(storage.getNamespaceName(), deployName);
        Pod pod = kubeClient().getPod(storage.getNamespaceName(), podName);

        // check that spec matches the actual pod configuration
        Affinity specAffinity = kubeClient().getDeployment(storage.getNamespaceName(), deployName).getSpec().getTemplate().getSpec().getAffinity();
        NodeSelectorRequirement specNodeRequirement = specAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        NodeAffinity podAffinity = pod.getSpec().getAffinity().getNodeAffinity();
        NodeSelectorRequirement podNodeRequirement = podAffinity.getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        assertThat(podNodeRequirement, is(specNodeRequirement));
        assertThat(podNodeRequirement.getKey(), is(TOPOLOGY_KEY));
        assertThat(podNodeRequirement.getOperator(), is("Exists"));

        // check Kafka client rack awareness configuration
        String podNodeName = pod.getSpec().getNodeName();
        String hostname = podNodeName.contains(".") ? podNodeName.substring(0, podNodeName.indexOf(".")) : podNodeName;
        String commandOut = cmdKubeClient(storage.getNamespaceName()).execInPod(podName,
                "/bin/bash", "-c", "cat /tmp/strimzi-connect.properties | grep consumer.client.rack").out().trim();
        assertThat(commandOut.equals("consumer.client.rack=" + hostname), is(true));
    }

    @Tag(MIRROR_MAKER2)
    @ParallelNamespaceTest
    void testMirrorMaker2RackAwareness(ExtensionContext extensionContext) {
        Assumptions.assumeFalse(Environment.isNamespaceRbacScope());

        TestStorage storage = storageMap.get(extensionContext);
        String kafkaClusterSourceName = storage.getClusterName() + "-source";
        String kafkaClusterTargetName = storage.getClusterName() + "-target";

        resourceManager.createResource(extensionContext,
                KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        resourceManager.createResource(extensionContext,
                KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        resourceManager.createResource(extensionContext,
                KafkaMirrorMaker2Templates.kafkaMirrorMaker2(storage.getClusterName(), kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
                        .editSpec()
                            .withNewRack(TOPOLOGY_KEY)
                        .endSpec()
                        .build());

        LOGGER.info("MirrorMaker2 cluster deployed successfully");
        String deployName = KafkaMirrorMaker2Resources.deploymentName(storage.getClusterName());
        String podName = PodUtils.getPodNameByPrefix(storage.getNamespaceName(), deployName);
        Pod pod = kubeClient().getPod(storage.getNamespaceName(), podName);

        // check that spec matches the actual pod configuration
        Affinity specAffinity = kubeClient().getDeployment(storage.getNamespaceName(), deployName).getSpec().getTemplate().getSpec().getAffinity();
        NodeSelectorRequirement specNodeRequirement = specAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        NodeAffinity podAffinity = pod.getSpec().getAffinity().getNodeAffinity();
        NodeSelectorRequirement podNodeRequirement = podAffinity.getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        assertThat(podNodeRequirement, is(specNodeRequirement));
        assertThat(podNodeRequirement.getKey(), is(TOPOLOGY_KEY));
        assertThat(podNodeRequirement.getOperator(), is("Exists"));

        // check Kafka client rack awareness configuration
        String podNodeName = pod.getSpec().getNodeName();
        String hostname = podNodeName.contains(".") ? podNodeName.substring(0, podNodeName.indexOf(".")) : podNodeName;
        String commandOut = cmdKubeClient(storage.getNamespaceName()).execInPod(podName, "/bin/bash", "-c", "cat /tmp/strimzi-connect.properties | grep consumer.client.rack").out().trim();
        assertThat(commandOut.equals("consumer.client.rack=" + hostname), is(true));
    }

    @BeforeEach
    void createTestResources(ExtensionContext extensionContext) {
        storageMap.put(extensionContext, new TestStorage(extensionContext));
    }
}
