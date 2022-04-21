/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.mirrormaker;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.NodeAffinity;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.systemtest.AbstractST;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.RACK_AWARENESS;
import static io.strimzi.systemtest.Constants.REGRESSION;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

@Tag(REGRESSION)
@Tag(MIRROR_MAKER2)
@Tag(CONNECT_COMPONENTS)
@Tag(INTERNAL_CLIENTS_USED)
@Tag(RACK_AWARENESS)
@ParallelSuite
class MirrorMaker2RackST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(MirrorMaker2RackST.class);

    @ParallelNamespaceTest
    void testMirrorMaker2RackAwareness(ExtensionContext extensionContext) {
        assumeFalse(Environment.isNamespaceRbacScope());

        TestStorage storage = new TestStorage(extensionContext);
        String namespace = storage.getNamespaceName();
        String clusterName = storage.getClusterName();
        String kafkaClusterSourceName = clusterName + "-source";
        String kafkaClusterTargetName = clusterName + "-target";
        String topologyKey = "kubernetes.io/hostname";

        resourceManager.createResource(extensionContext,
                KafkaTemplates.kafkaEphemeral(kafkaClusterSourceName, 1, 1).build());
        resourceManager.createResource(extensionContext,
                KafkaTemplates.kafkaEphemeral(kafkaClusterTargetName, 1, 1).build());

        resourceManager.createResource(extensionContext,
                KafkaMirrorMaker2Templates.kafkaMirrorMaker2(clusterName, kafkaClusterTargetName, kafkaClusterSourceName, 1, false)
                    .editSpec()
                        .withNewRack(topologyKey)
                    .endSpec()
                    .build());

        LOGGER.info("MirrorMaker2 cluster deployed successfully");
        String deployName = KafkaMirrorMaker2Resources.deploymentName(clusterName);
        String podName = PodUtils.getPodNameByPrefix(namespace, deployName);
        Pod pod = PodUtils.getPodByName(namespace, podName);

        // check that spec matches the actual pod configuration
        Affinity specAffinity = kubeClient().getDeployment(deployName).getSpec().getTemplate().getSpec().getAffinity();
        NodeSelectorRequirement specNodeRequirement = specAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        NodeAffinity podAffinity = pod.getSpec().getAffinity().getNodeAffinity();
        NodeSelectorRequirement podNodeRequirement = podAffinity.getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms().get(0).getMatchExpressions().get(0);
        assertThat(podNodeRequirement, is(specNodeRequirement));
        assertThat(podNodeRequirement.getKey(), is(topologyKey));
        assertThat(podNodeRequirement.getOperator(), is("Exists"));

        // check Kafka client rack awareness configuration
        String podNodeName = pod.getSpec().getNodeName();
        String hostname = podNodeName.contains(".") ? podNodeName.substring(0, podNodeName.indexOf(".")) : podNodeName;
        String commandOut = cmdKubeClient().execInPod(podName, "/bin/bash", "-c", "cat /tmp/strimzi-connect.properties | grep consumer.client.rack").out().trim();
        assertThat(commandOut.equals("consumer.client.rack=" + hostname), is(true));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        clusterOperator.unInstall();
        clusterOperator = clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}
