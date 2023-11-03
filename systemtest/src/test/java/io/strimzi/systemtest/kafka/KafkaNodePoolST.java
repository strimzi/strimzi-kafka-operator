/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;


import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import java.util.Arrays;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils.waitForPodsReady;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Tag(REGRESSION)
public class KafkaNodePoolST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaNodePoolST.class);

    /**
     * @description This test case verifies KafkaNodePools scaling up and down with correct NodePool IDs, with different NodePools.
     * @param extensionContext
     */
    @ParallelNamespaceTest
    void testKafkaNodePoolBrokerIdsManagementUsingAnnotations(ExtensionContext extensionContext) {

        final TestStorage testStorage = new TestStorage(extensionContext);
        String nodePoolNameA = testStorage.getKafkaNodePoolName() + "-a";
        String nodePoolNameB = testStorage.getKafkaNodePoolName() + "-b";

        // We have disabled the broker scale down check for now since the test fails at the moment
        // due to partition replicas being present on the broker during scale down. We can enable this check
        // once the issue is resolved
        // https://github.com/strimzi/strimzi-kafka-operator/issues/9134
        Kafka kafka = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1, 1)
            .editOrNewMetadata()
                .addToAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled", Annotations.ANNO_STRIMZI_IO_SKIP_BROKER_SCALEDOWN_CHECK, "true"))
            .endMetadata()
            .build();

        LOGGER.info("Testing deployment of NodePools with pre-configured annotation: {} is creating Brokers with correct IDs", Annotations.ANNO_STRIMZI_IO_NODE_POOLS);
        // Deploy NodePool A with only 1 replica and give it annotation with 1 ID
        KafkaNodePool poolA =  KafkaNodePoolTemplates.kafkaNodePoolWithBrokerRole(testStorage.getNamespaceName(), nodePoolNameA, testStorage.getClusterName(), 1)
            .editOrNewMetadata()
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[5]"))
            .endMetadata()
            .editOrNewSpec()
            .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
                .withStorage(kafka.getSpec().getKafka().getStorage())
                .withJvmOptions(kafka.getSpec().getKafka().getJvmOptions())
                .withResources(kafka.getSpec().getKafka().getResources())
            .endSpec()
            .build();

        // Deploy NodePool B with 2 replicas and give it annotation with only  1 ID
        KafkaNodePool poolB =  KafkaNodePoolTemplates.kafkaNodePoolWithBrokerRole(testStorage.getNamespaceName(), nodePoolNameB, testStorage.getClusterName(), 2)
            .editOrNewMetadata()
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[6]"))
            .endMetadata()
            .editOrNewSpec()
                .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
                .withStorage(kafka.getSpec().getKafka().getStorage())
                .withJvmOptions(kafka.getSpec().getKafka().getJvmOptions())
                .withResources(kafka.getSpec().getKafka().getResources())
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(extensionContext, poolA, poolB, kafka);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameA), 1);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameB), 2);

        LOGGER.info("Verifying NodePools contain correct IDs");
        assertThat("NodePool: " + nodePoolNameA + " does not contain expected nodeIds: [5]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameA).get(0).equals(5));
        assertThat("NodePool: " + nodePoolNameB + " does not contain expected nodeIds: [0, 6]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameB).equals(Arrays.asList(0, 6)));

        LOGGER.info("Testing annotation with upscaling NodePool A (more replicas than specified IDs) " +
                    "and downscaling NodePool B (more IDs than needed to be scaled down. This redundant ID is not present)");
        // Annotate NodePool A for scale up with fewer IDs than needed -> this should cause addition of non-used ID starting from [0] in ASC order, which is in this case [1]
        KafkaNodePoolUtils.setKafkaNodePoolAnnotation(testStorage.getNamespaceName(), nodePoolNameA, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS,  "[20-21]"));
        // Annotate NodePool B for scale down with more IDs than needed - > this should not matter as ID [99] is not present so only ID [6] is removed
        KafkaNodePoolUtils.setKafkaNodePoolAnnotation(testStorage.getNamespaceName(), nodePoolNameB, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, "[6, 99]"));
        // Scale NodePool A up + NodePool B down
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), nodePoolNameA, 4);
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), nodePoolNameB, 1);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameA), 4);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameB), 1);

        LOGGER.info("Verifying NodePools contain correct IDs");
        assertThat("NodePool: " + nodePoolNameA + " does not contain expected nodeIds: [1, 5, 20, 21]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameA).equals(Arrays.asList(1, 5, 20, 21)));
        assertThat("NodePool: " + nodePoolNameB + " does not contain expected nodeIds: [0]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameB).get(0).equals(0));

        // 3. Case (A-missing ID for downscale, B-already used ID for upscale)
        LOGGER.info("Testing annotation with downscaling NodePool A (fewer IDs than needed) and NodePool B (already used ID)");
        // Annotate NodePool A for scale down with fewer IDs than needed, this should cause removal of IDs in DESC order after the annotated ID is deleted
        KafkaNodePoolUtils.setKafkaNodePoolAnnotation(testStorage.getNamespaceName(), nodePoolNameA, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS,  "[20]"));
        // Annotate NodePool B for scale up with ID [1] already in use
        KafkaNodePoolUtils.setKafkaNodePoolAnnotation(testStorage.getNamespaceName(), nodePoolNameB, Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[1]"));
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), nodePoolNameA, 2);
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), nodePoolNameB, 6);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameA), 2);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameB), 6);

        LOGGER.info("Verifying NodePools contain correct IDs");
        assertThat("NodePool: " + nodePoolNameA + " does not contain expected nodeIds: [1, 5]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameA).equals(Arrays.asList(1, 5)));
        assertThat("NodePool: " + nodePoolNameB + " does not contain expected nodeIds: [0, 2, 3, 4, 6, 7]",
            KafkaNodePoolUtils.getCurrentKafkaNodePoolIds(testStorage.getNamespaceName(), nodePoolNameB).equals(Arrays.asList(0, 2, 3, 4, 6, 7)));
    }

    @ParallelNamespaceTest
    void testKNPConfigurationPropagationAndModification(ExtensionContext extensionContext) {
        // unless kraft is enabled there is no point testing controllers as only allowed role without KRaft is 'Broker'
//        assumeTrue(Environment.isKRaftModeEnabled());

        final TestStorage testStorage = new TestStorage(extensionContext);
        final String nodePoolNameA = testStorage.getKafkaNodePoolName() + "-a";   // node pool (to be) with all roles,
        final String nodePoolNameB = testStorage.getKafkaNodePoolName() + "-b";   // node pool (to be) with broker role
        final String nodePoolNameC = testStorage.getKafkaNodePoolName() + "-c";   // node pool (to be) with controller role

        final LabelSelector nodePoolALabelSelector = KafkaNodePoolResource.getLabelSelector(testStorage.getClusterName(), nodePoolNameA);
        final LabelSelector nodePoolBLabelSelector = KafkaNodePoolResource.getLabelSelector(testStorage.getClusterName(), nodePoolNameB);
        final LabelSelector nodePoolCLabelSelector = KafkaNodePoolResource.getLabelSelector(testStorage.getClusterName(), nodePoolNameC);

        // config for jvm and resource limit present in Kafka CR
        final String kafkaJvmXmxConfig = "1024m";
        final String kafkaLimitMemoryQuantityConfig = "1024Mi";
        final String kafkaLimitMemoryQuantityConfigModified = "1100Mi";

        // overriding configurations in some of kafkaNodePools CRs
        final String knpBJvmXmxConfig = "2048m";
        final String knpBAndCLimitMemoryQuantityConfig = "2048Mi";

        LOGGER.info("Deploy Kafka: {}/{} with explicit JVM and resource.limit specification", testStorage.getNamespaceName(), testStorage.getClusterName());
        Kafka kafka = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 2, 1)
            .editOrNewMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withResources(new ResourceRequirementsBuilder()
                        .addToLimits("memory", new Quantity(kafkaLimitMemoryQuantityConfig))
                    .build())
                    .withNewJvmOptions()
                        .withXmx(kafkaJvmXmxConfig)
                    .endJvmOptions()
                .endKafka()
            .endSpec()
            .build();

        // KNP with all roles and without any other optional config. overriding config. provided in Kafka
        KafkaNodePool poolA =  KafkaNodePoolTemplates.defaultKafkaNodePool(testStorage.getNamespaceName(), nodePoolNameA, testStorage.getClusterName(), 2)
            .editOrNewSpec()
                .withRoles(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)
                .withStorage(kafka.getSpec().getKafka().getStorage())   // storage is mandatory spec, otherwise it not important part of the test
            .endSpec()
            .build();

        // KNP with broker role with its own config for jvm and resource.limit overriding provided config in Kafka
        KafkaNodePool poolB =  KafkaNodePoolTemplates.defaultKafkaNodePool(testStorage.getNamespaceName(), nodePoolNameB, testStorage.getClusterName(), 1)
            .editOrNewSpec()
                .withRoles(ProcessRoles.BROKER)
                .withStorage(kafka.getSpec().getKafka().getStorage()) // mandatory
                .withNewJvmOptions()
                    .withXmx(knpBJvmXmxConfig) // overriding jvm configuration
                .endJvmOptions()
                .withResources(new ResourceRequirementsBuilder()
                    .addToLimits("memory", new Quantity(knpBAndCLimitMemoryQuantityConfig)) // overriding resource.limit
                .build())
            .endSpec()
            .build();

        // KNP with broker role with its own config for jvm and resource.limit overriding provided config in Kafka
        KafkaNodePool poolC =  KafkaNodePoolTemplates.kafkaNodePoolWithBrokerRole(testStorage.getNamespaceName(), nodePoolNameC, testStorage.getClusterName(), 1)
            .editOrNewSpec()
                .withRoles(ProcessRoles.CONTROLLER)
                .withStorage(kafka.getSpec().getKafka().getStorage()) // mandatory
                .withResources(new ResourceRequirementsBuilder()
                    .addToLimits("memory", new Quantity(knpBAndCLimitMemoryQuantityConfig)) // overriding resource.limit
                .build())
            .endSpec()
            .build();

        LOGGER.info("Verify correct config. propagation from Kafka to Kafka Node Pool Pods");

        resourceManager.createResourceWithWait(extensionContext, poolA, poolB, poolC, kafka);
        waitForPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 4, true);

        // assert jvm and limits config (written in kafka) is propagated into KNP A Pods
        verifyResourcesConfigurationPropagatedToKnpPods(testStorage, nodePoolNameA, kafkaJvmXmxConfig, kafkaLimitMemoryQuantityConfig);

        // assert jvm and limits config (written in kafka) is overwritten by respective config present in KNP B Pods
        verifyResourcesConfigurationPropagatedToKnpPods(testStorage, nodePoolNameB, knpBJvmXmxConfig, knpBAndCLimitMemoryQuantityConfig);

        // assert jvm config (written in kafka) is propagated into KNP C Pods while limit configuration not, as it is present in KNP C.
        verifyResourcesConfigurationPropagatedToKnpPods(testStorage, nodePoolNameC, kafkaJvmXmxConfig, knpBAndCLimitMemoryQuantityConfig);

        LOGGER.info("Verify correct config. propagation when modifying kafka CR");

        final Map<String, String> nodePoolAPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), nodePoolALabelSelector);
        final Map<String, String> nodePoolBPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), nodePoolBLabelSelector);

        // modify resource.limit in kafka CR
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
            k.getSpec().getKafka().getResources().getLimits().replace("memory", new Quantity(kafkaLimitMemoryQuantityConfigModified));
        }, testStorage.getNamespaceName());

        // (as KNP A is the only one that inherit it from Kafka it is the only one which needs to be updated by this change
        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), nodePoolBLabelSelector, nodePoolBPodsSnapshot);
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), nodePoolALabelSelector, 2, nodePoolAPodsSnapshot);
        //waitForPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), scaledUpBrokerReplicaCount, false);

        // assert that new changed configuration is present in pods under Kafka Node Pool A
        verifyResourcesConfigurationPropagatedToKnpPods(testStorage, nodePoolNameA, kafkaJvmXmxConfig, kafkaLimitMemoryQuantityConfigModified);

        LOGGER.info("Modifying configuration relevant or irrelevant based on role of given KafkaNodePools");

        final Pod podFromKnpC = PodUtils.getPodsByPrefixInNameWithDynamicWait(testStorage.getNamespaceName(), KafkaNodePoolUtils.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameC)).get(0);
        final String podCName = podFromKnpC.getMetadata().getName();
        final String knpCConfigHashAnnotationValue = podFromKnpC.getMetadata().getAnnotations().get(Constants.NODE_BROKER_CONFIG_HASH_ANNOTATION);
        final String knpCStrimziPodSetName = KafkaNodePoolUtils.getStrimziPodSetName(testStorage.getClusterName(), nodePoolNameC);

        // replacing config which is not relevant to control (also it is part of dynamic config, therefore should not trigger Rolling Update)
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getKafka().getConfig().put("compression.type", "gzip"), testStorage.getNamespaceName());

        StrimziPodSetUtils.waitForPrevailedPodAnnotationKeyValuePairs(testStorage.getNamespaceName(), Constants.NODE_BROKER_CONFIG_HASH_ANNOTATION, knpCConfigHashAnnotationValue, knpCStrimziPodSetName, podCName);

        // replacing config relevant only to controllers
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getKafka().getConfig().put("max.connections", "456"), testStorage.getNamespaceName());

        StrimziPodSetUtils.waitUntilPodAnnotationChange(testStorage.getNamespaceName(), podCName, knpCStrimziPodSetName, Constants.NODE_BROKER_CONFIG_HASH_ANNOTATION, knpCConfigHashAnnotationValue);
    }

    /**
     * Verifies that the specified configurations are correctly propagated to the Kafka Node Pool (KNP) pods.
     *
     * <p>This method checks the memory limit and JVM heap options of the first pod belonging to the given node pool.
     *
     * @param testStorage        the storage object containing test-related information.
     * @param nodePoolName       the name of the KNP to verify.
     * @param expectedJvmXmxLimit the expected JVM '-Xmx' heap size limit.
     * @param expectedMemoryLimit the expected memory limit ('resource.limits.memory') for the pod.
     *
     * @throws AssertionError if configurations obtained from Pods do not match the expected values.
     */
    private void verifyResourcesConfigurationPropagatedToKnpPods(TestStorage testStorage, String nodePoolName, String expectedJvmXmxLimit, String expectedMemoryLimit) {
        Pod nodePoolPod = PodUtils.getPodsByPrefixInNameWithDynamicWait(testStorage.getNamespaceName(), testStorage.getClusterName() + "-" + nodePoolName).get(0);
        Container container = nodePoolPod.getSpec().getContainers().stream().findFirst().get();
        Quantity  obtainedMemory = container.getResources().getLimits().get("memory");
        EnvVar obtainedJvmHeapEnvVariable = container.getEnv().stream().filter(e -> e.getName().equals("KAFKA_HEAP_OPTS")).findAny().get();

        assertThat(obtainedMemory, is(new Quantity(expectedMemoryLimit)));
        assertThat("NodePool: does not contain expected JVM configuration", obtainedJvmHeapEnvVariable.getValue().contains(expectedJvmXmxLimit));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());
        assumeTrue(Environment.isKRaftModeEnabled());

        List<EnvVar> coEnvVars = new ArrayList<>();
        coEnvVars.add(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, "+UseKRaft,+KafkaNodePools,+UnidirectionalTopicOperator", null));

        this.clusterOperator = this.clusterOperator.defaultInstallation(extensionContext)
            .withExtraEnvVars(coEnvVars)
            .createInstallation()
            .runInstallation();
    }
}