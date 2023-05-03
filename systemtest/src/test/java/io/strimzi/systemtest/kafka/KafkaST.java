/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Container;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.SystemProperty;
import io.strimzi.api.kafka.model.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.ConfigMapUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@Tag(REGRESSION)
@SuppressWarnings("checkstyle:ClassFanOutComplexity")
@ParallelSuite
class KafkaST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaST.class);
    private static final String OPENSHIFT_CLUSTER_NAME = "openshift-my-cluster";

    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(KafkaST.class.getSimpleName()).stream().findFirst().get();

    @ParallelNamespaceTest
    @KRaftNotSupported("EntityOperator is not supported by KRaft mode and is used in this test class")
    void testJvmAndResources(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

        ArrayList<SystemProperty> javaSystemProps = new ArrayList<>();
        javaSystemProps.add(new SystemPropertyBuilder().withName("javax.net.debug")
                .withValue("verbose").build());

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
            .editSpec()
                .editKafka()
                    .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("1.5Gi"))
                            .addToLimits("cpu", new Quantity("1"))
                            .addToRequests("memory", new Quantity("1Gi"))
                            .addToRequests("cpu", new Quantity("50m"))
                            .build())
                    .withNewJvmOptions()
                        .withXmx("1g")
                        .withXms("512m")
                        .withXx(jvmOptionsXX)
                    .endJvmOptions()
                .endKafka()
                .editZookeeper()
                    .withResources(
                        new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("1G"))
                            .addToLimits("cpu", new Quantity("0.5"))
                            .addToRequests("memory", new Quantity("0.5G"))
                            .addToRequests("cpu", new Quantity("25m"))
                            .build())
                    .withNewJvmOptions()
                        .withXmx("1G")
                        .withXms("512M")
                        .withXx(jvmOptionsXX)
                    .endJvmOptions()
                .endZookeeper()
                .withNewEntityOperator()
                    .withNewTopicOperator()
                        .withResources(
                            new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("1024Mi"))
                                .addToLimits("cpu", new Quantity("500m"))
                                .addToRequests("memory", new Quantity("384Mi"))
                                .addToRequests("cpu", new Quantity("0.025"))
                                .build())
                        .withNewJvmOptions()
                            .withXmx("2G")
                            .withXms("1024M")
                            .withJavaSystemProperties(javaSystemProps)
                        .endJvmOptions()
                    .endTopicOperator()
                    .withNewUserOperator()
                        .withResources(
                            new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("512M"))
                                .addToLimits("cpu", new Quantity("300m"))
                                .addToRequests("memory", new Quantity("256M"))
                                .addToRequests("cpu", new Quantity("30m"))
                                .build())
                        .withNewJvmOptions()
                            .withXmx("1G")
                            .withXms("512M")
                            .withJavaSystemProperties(javaSystemProps)
                        .endJvmOptions()
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .build());

        // Make snapshots for Kafka cluster to meke sure that there is no rolling update after CO reconciliation
        final String zkStsName = KafkaResources.zookeeperStatefulSetName(clusterName);
        final String kafkaStsName = kafkaStatefulSetName(clusterName);
        final String eoDepName = KafkaResources.entityOperatorDeploymentName(clusterName);
        final Map<String, String> zkPods = PodUtils.podSnapshot(namespaceName, zkSelector);
        final Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);
        final Map<String, String> eoPods = DeploymentUtils.depSnapshot(namespaceName, eoDepName);

        assertResources(namespaceName, KafkaResources.kafkaPodName(clusterName, 0), "kafka",
                "1536Mi", "1", "1Gi", "50m");
        assertExpectedJavaOpts(namespaceName, KafkaResources.kafkaPodName(clusterName, 0), "kafka",
                "-Xmx1g", "-Xms512m", "-XX:+UseG1GC");

        assertResources(namespaceName, KafkaResources.zookeeperPodName(clusterName, 0), "zookeeper",
                "1G", "500m", "500M", "25m");
        assertExpectedJavaOpts(namespaceName, KafkaResources.zookeeperPodName(clusterName, 0), "zookeeper",
                "-Xmx1G", "-Xms512M", "-XX:+UseG1GC");

        Optional<Pod> pod = kubeClient(namespaceName).listPods(namespaceName)
                .stream().filter(p -> p.getMetadata().getName().startsWith(KafkaResources.entityOperatorDeploymentName(clusterName)))
                .findFirst();
        assertThat("EO pod does not exist", pod.isPresent(), is(true));

        assertResources(namespaceName, pod.get().getMetadata().getName(), "topic-operator",
                "1Gi", "500m", "384Mi", "25m");
        assertResources(namespaceName, pod.get().getMetadata().getName(), "user-operator",
                "512M", "300m", "256M", "30m");
        assertExpectedJavaOpts(namespaceName, pod.get().getMetadata().getName(), "topic-operator",
                "-Xmx2G", "-Xms1024M", null);
        assertExpectedJavaOpts(namespaceName, pod.get().getMetadata().getName(), "user-operator",
                "-Xmx1G", "-Xms512M", null);

        String eoPod = eoPods.keySet().toArray()[0].toString();
        kubeClient(namespaceName).getPod(namespaceName, eoPod).getSpec().getContainers().forEach(container -> {
            if (!container.getName().equals("tls-sidecar")) {
                LOGGER.info("Check if -D java options are present in {}", container.getName());

                String javaSystemProp = container.getEnv().stream().filter(envVar ->
                    envVar.getName().equals("STRIMZI_JAVA_SYSTEM_PROPERTIES")).findFirst().orElseThrow().getValue();
                String javaOpts = container.getEnv().stream().filter(envVar ->
                    envVar.getName().equals("STRIMZI_JAVA_OPTS")).findFirst().orElseThrow().getValue();

                assertThat(javaSystemProp, is("-Djavax.net.debug=verbose"));

                if (container.getName().equals("topic-operator")) {
                    assertThat(javaOpts, is("-Xms1024M -Xmx2G"));
                }

                if (container.getName().equals("user-operator")) {
                    assertThat(javaOpts, is("-Xms512M -Xmx1G"));
                }
            }
        });

        LOGGER.info("Checking no rolling update for Kafka cluster");
        RollingUpdateUtils.waitForNoRollingUpdate(namespaceName, zkSelector, zkPods);
        RollingUpdateUtils.waitForNoRollingUpdate(namespaceName, kafkaSelector, kafkaPods);
        DeploymentUtils.waitForNoRollingUpdate(namespaceName, eoDepName, eoPods);
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testRemoveTopicOperatorFromEntityOperator(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        LOGGER.info("Deploying Kafka cluster {}", clusterName);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        String eoPodName = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName))
            .get(0).getMetadata().getName();

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getEntityOperator().setTopicOperator(null), namespaceName);
        //Waiting when EO pod will be recreated without TO
        PodUtils.deletePodWithWait(namespaceName, eoPodName);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);
        PodUtils.waitUntilPodContainersCount(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);

        //Checking that TO was removed
        kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), not(containsString("topic-operator")));
            });
        });

        eoPodName = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName))
                .get(0).getMetadata().getName();

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getEntityOperator().setTopicOperator(new EntityTopicOperatorSpec()), namespaceName);
        //Waiting when EO pod will be recreated with TO
        PodUtils.deletePodWithWait(namespaceName, eoPodName);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);

        //Checking that TO was created
        kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), anyOf(
                    containsString("topic-operator"),
                    containsString("user-operator"),
                    containsString("tls-sidecar"))
                );
            });
        });
    }

    /**
     * @description This test case verifies the correct deployment of Entity Operator, i.e., including both User Operator and Topic Operator.
     * Entity Operator is firstly modified to exclude User Operator, afterwards it is modified to default configuration, which includes User Operator.
     *
     * @steps
     *  1. - Deploy Kafka with default configuration.
     *     - Kafka is deployed, entity operator comprises both TopicOperator and User Operator.
     *  2. - Remove User Operator from the Kafka Custom Resource specification.
     *     - Entity Operator is redeployed without the User Operator, no other action take place inside the given Kafka cluster.
     *  3. - Modify configuration of the Kafka Custom Resource by specifying default Entity Operator.
     *     - Entity Operator is redeployed, comprising User Operator as well.
     *
     * @usecase
     *  - entity-operator
     *  - topic-operator
     *  - user-operator
     */
    @ParallelNamespaceTest
    void testRemoveComponentsFromEntityOperator(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        LOGGER.info("Deploying Kafka cluster {}", clusterName);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        LOGGER.info("Remove User Operator from Entity Operator");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getEntityOperator().setUserOperator(null), namespaceName);

        if (!Environment.isKRaftModeEnabled()) {
            //Waiting when EO pod will be recreated without UO
            DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);
            PodUtils.waitUntilPodContainersCount(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 2);

            //Checking that UO was removed
            kubeClient().listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName)).forEach(pod -> {
                pod.getSpec().getContainers().forEach(container -> {
                    assertThat(container.getName(), not(containsString("user-operator")));
                });
            });
        } else {
            DeploymentUtils.waitForDeploymentDeletion(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName));
        }

        LOGGER.info("Recreate User Operator");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getEntityOperator().setUserOperator(new EntityUserOperatorSpec()), namespaceName);
        //Waiting when EO pod will be recreated with UO
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);

        int expectedEOContainerCount = Environment.isKRaftModeEnabled() ? 1 : 3;
        PodUtils.waitUntilPodContainersCount(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), expectedEOContainerCount);

        LOGGER.info("Verify that Entity operator and all its component are correctly recreated");
        // names of containers present in EO pod
        List<String> entityOperatorContainerNames = kubeClient().listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName))
                .get(0).getSpec().getContainers()
                .stream()
                .map(Container::getName)
                .toList();

        assertThat("user-operator container is not present in EO", entityOperatorContainerNames.stream().anyMatch(name -> name.contains("user-operator")));

        // kraft does not support Topic Operator, therefore removal and recreation of User Operator is all to be tested with kraft enabled, rest of test is without kraft
        if (!Environment.isKRaftModeEnabled()) {
            assertThat("tls-sidecar container is not present in EO", entityOperatorContainerNames.stream().anyMatch(name -> name.contains("tls-sidecar")));
            assertThat("topic-operator container is not present in EO", entityOperatorContainerNames.stream().anyMatch(name -> name.contains("topic-operator")));

            LOGGER.info("Remove Topic Operator from Entity Operator");
            KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getEntityOperator().setTopicOperator(null), namespaceName);
            DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);
            PodUtils.waitUntilPodContainersCount(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);

            //Checking that TO was removed
            LOGGER.info("Verify that Topic Operator container is no longer present in Entity Operator Pod");
            kubeClient().listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName)).forEach(pod -> {
                pod.getSpec().getContainers().forEach(container -> {
                    assertThat(container.getName(), not(containsString("topic-operator")));
                });
            });

            LOGGER.info("Remove User Operator, after removed Topic Operator");
            KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> {
                k.getSpec().getEntityOperator().setUserOperator(null);
            }, namespaceName);

            // both TO and UO are unset, which means EO should not be deployed
            LOGGER.info("Wait for deletion of Entity Operator Pod");
            PodUtils.waitUntilPodStabilityReplicasCount(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 0);
        }
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testTopicWithoutLabels(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String scraperName = mapWithScraperNames.get(extensionContext.getDisplayName());

        // Negative scenario: creating topic without any labels and make sure that TO can't handle this topic
        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaEphemeral(clusterName, 3).build(),
            ScraperTemplates.scraperPod(namespaceName, scraperName).build()
        );

        final String scraperPodName =  kubeClient().listPodsByPrefixInName(namespaceName, scraperName).get(0).getMetadata().getName();

        // Creating topic without any label
        resourceManager.createResource(extensionContext, false, KafkaTopicTemplates.topic(clusterName, "topic-without-labels", 1, 1, 1)
            .editMetadata()
                .withLabels(null)
            .endMetadata()
            .build()
        );

        // Checking that resource was created
        assertThat(cmdKubeClient(namespaceName).list("kafkatopic"), hasItems("topic-without-labels"));
        // Checking that TO didn't handle new topic and zk pods don't contain new topic
        assertThat(KafkaCmdClient.listTopicsUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName)), not(hasItems("topic-without-labels")));

        // Checking TO logs
        String tOPodName = cmdKubeClient(namespaceName).listResourcesByLabel("pod", Labels.STRIMZI_NAME_LABEL + "=" + clusterName + "-entity-operator").get(0);
        String tOlogs = kubeClient(namespaceName).logsInSpecificNamespace(namespaceName, tOPodName, "topic-operator");
        assertThat(tOlogs, not(containsString("Created topic 'topic-without-labels'")));

        //Deleting topic
        cmdKubeClient(namespaceName).deleteByName("kafkatopic", "topic-without-labels");
        KafkaTopicUtils.waitForKafkaTopicDeletion(namespaceName,  "topic-without-labels");

        //Checking all topics were deleted
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName));
        assertThat(topics, not(hasItems("topic-without-labels")));
    }

    /**
     * @description This test case verifies that Kafka with persistent storage, and JBOD storage, property 'delete claim' of JBOD storage.
     *
     * @steps
     *  1. - Deploy Kafka with persistent storage and JBOD storage with 2 volumes, both of these are configured to delete their Persistent Volume Claims on Kafka cluster un-provision.
     *     - Kafka is deployed, volumes are labeled and linked to Pods correctly.
     *  2. - Verify that labels in Persistent Volume Claims are set correctly.
     *     - Persistent Volume Claims do contain expected labels and values.
     *  2. - Modify Kafka Custom Resource, specifically 'delete claim' property of its first Kafka Volume.
     *     - Kafka CR is successfully modified, annotation of according Persistent Volume Claim is changed afterwards by Cluster Operator.
     *  3. - Delete Kafka cluster.
     *     - Kafka cluster and its components are deleted, including Persistent Volume Claim of Volume with 'delete claim' property set to true.
     *  4. - Verify remaining Persistent Volume Claims.
     *     - Persistent Volume Claim referenced by volume of formerly deleted Kafka Custom Resource with property 'delete claim' set to true is still present.
     *
     * @usecase
     *  - JBOD
     *  - PVC
     *  - volume
     *  - annotations
     */
    @ParallelNamespaceTest
    @KRaftNotSupported("JBOD is not supported by KRaft mode and is used in this test case.")
    void testKafkaJBODDeleteClaimsTrueFalse(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final int kafkaReplicas = 2;
        final String diskSizeGi = "10";

        //Volume Storages (original and modified)
        PersistentClaimStorage idZeroVolumeOriginal = new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize(diskSizeGi + "Gi").build();
        PersistentClaimStorage idOneVolumeOriginal = new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize(diskSizeGi + "Gi").build();
        PersistentClaimStorage idZeroVolumeModified = new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(0).withSize(diskSizeGi + "Gi").build();

        JbodStorage jbodStorage = new JbodStorageBuilder().withVolumes(idZeroVolumeOriginal, idOneVolumeOriginal).build();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaJBOD(clusterName, kafkaReplicas, jbodStorage).build());
        // kafka cluster already deployed
        verifyVolumeNamesAndLabels(namespaceName, clusterName, kafkaReplicas, 2, diskSizeGi);

        //change value of first PVC to delete its claim once Kafka is deleted.
        LOGGER.info("Update Volume with id=0 in Kafka CR by setting 'Delete Claim' property to false.");

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, resource -> {
            LOGGER.debug(resource.getMetadata().getName());
            JbodStorage jBODVolumeStorage = (JbodStorage) resource.getSpec().getKafka().getStorage();
            jBODVolumeStorage.setVolumes(List.of(idZeroVolumeModified, idOneVolumeOriginal));
        }, namespaceName);

        TestUtils.waitFor("Wait for change of Annotation of PVCs according to changes in 'delete claim' property of Kafka's JBOD storage", Constants.GLOBAL_POLL_INTERVAL, Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> kubeClient().listPersistentVolumeClaims(namespaceName, clusterName).stream()
                .filter(pvc -> pvc.getMetadata().getName().startsWith("data-0") && pvc.getMetadata().getName().contains("-kafka"))
                .allMatch(volume -> "false".equals(volume.getMetadata().getAnnotations().get("strimzi.io/delete-claim")))
        );

        final int volumesCount = kubeClient().listPersistentVolumeClaims(namespaceName, clusterName).size();

        LOGGER.info("Delete cluster Kafka/{} Namespace/{}", clusterName, namespaceName);
        cmdKubeClient(namespaceName).deleteByName("kafka", clusterName);

        LOGGER.info("Wait for PVCs deletion");
        PersistentVolumeClaimUtils.waitForJbodStorageDeletion(namespaceName, volumesCount, clusterName, idZeroVolumeModified, idOneVolumeOriginal);

        LOGGER.info("Verify that PVC which are supposed to remain, really persist even after Kafka cluster un-deployment");
        List<String> remainingPVCNames =  kubeClient().listPersistentVolumeClaims(namespaceName, clusterName).stream().map(e -> e.getMetadata().getName()).toList();
        assertThat("Kafka broker with id 0 does not preserve its JBOD storage's PVC", remainingPVCNames.stream().anyMatch(e -> e.startsWith("data-0") && e.contains("-kafka-0")));
        assertThat("Kafka broker with id 1 does not preserve its JBOD storage's PVC", remainingPVCNames.stream().anyMatch(e -> e.startsWith("data-0") && e.contains("-kafka-1")));
    }

    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    void testRegenerateCertExternalAddressChange(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        LOGGER.info("Creating kafka without external listener");
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 1).build());

        final String brokerSecret = clusterName + "-kafka-brokers";

        Secret secretsWithoutExt = kubeClient(namespaceName).getSecret(namespaceName, brokerSecret);

        LOGGER.info("Editing kafka with external listener");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            List<GenericKafkaListener> lst = asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build(),
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(true)
                            .withNewConfiguration()
                                .withFinalizers(LB_FINALIZERS)
                            .endConfiguration()
                            .build()
            );
            kafka.getSpec().getKafka().setListeners(lst);
        }, namespaceName);

        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, 3, PodUtils.podSnapshot(namespaceName, kafkaSelector));

        Secret secretsWithExt = kubeClient(namespaceName).getSecret(namespaceName, brokerSecret);

        LOGGER.info("Checking secrets");
        kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName)).forEach(kafkaPod -> {
            String kafkaPodName = kafkaPod.getMetadata().getName();
            assertThat(secretsWithExt.getData().get(kafkaPodName + ".crt"), is(not(secretsWithoutExt.getData().get(kafkaPodName + ".crt"))));
            assertThat(secretsWithExt.getData().get(kafkaPodName + ".key"), is(not(secretsWithoutExt.getData().get(kafkaPodName + ".key"))));
        });
    }

    /**
     * @description This test case verifies the presence of expected Strimzi specific labels, also labels and annotations specified by user.
     * Some of user-specified labels are later modified (new one is added, one is modified) which triggers rolling update after which
     * all changes took place as expected.
     *
     * @steps
     *  1. - Deploy Kafka with persistent storage and specify custom labels in CR metadata, and also other labels and annotation in PVC metadata
     *     - Kafka is deployed with its default labels and all others specified by user.
     *  2. - Deploy Producer and Consumer configured to produce and consume default number of messages, to make sure Kafka works as expected
     *     - Producer and Consumer are able to produce and consume messages respectively.
     *  3. - Modify configuration of Kafka CR with addition of new labels and modification of existing
     *     - Kafka is rolling and new labels are present in Kafka CR, and managed resources
     *  4. - Deploy Producer and Consumer configured to produce and consume default number of messages, to make sure Kafka works as expected
     *     - Producer and Consumer are able to produce and consume messages respectively.
     *
     * @usecase
     *  - annotations
     *  - labels
     *  - kafka-rolling-update
     *  - persistent-storage
     */
    @ParallelNamespaceTest
    @KRaftNotSupported("JBOD is not supported by KRaft mode and is used in this test case.")
    @SuppressWarnings({"checkstyle:JavaNCSS", "checkstyle:NPathComplexity", "checkstyle:MethodLength"})
    @Tag(INTERNAL_CLIENTS_USED)
    void testLabelsExistenceAndManipulation(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final int kafkaReplicas = 3;

        // label key and values to be used as part of kafka CR
        final String firstKafkaLabelKey = "first-kafka-label-key";
        final String firstKafkaLabelValue = "first-kafka-label-value";
        final String secondKafkaLabelKey = "second-kafka-label-key";
        final String secondKafkaLabelValue = "second-kafka-label-value";
        final Map<String, String> customSpecifiedLabels = new HashMap<>();
        customSpecifiedLabels.put(firstKafkaLabelKey, firstKafkaLabelValue);
        customSpecifiedLabels.put(secondKafkaLabelKey, secondKafkaLabelValue);

        // label key and value used in addition for while creating kafka CR (as part of PVCs label and annotation)
        final String pvcLabelOrAnnotationKey = "pvc-label-annotation-key";
        final String pvcLabelOrAnnotationValue = "pvc-label-annotation-value";
        final Map<String, String> customSpecifiedLabelOrAnnotationPvc = new HashMap<>();
        customSpecifiedLabelOrAnnotationPvc.put(pvcLabelOrAnnotationKey, pvcLabelOrAnnotationValue);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 1)
            .editMetadata()
                .withLabels(customSpecifiedLabels)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withNewTemplate()
                        .withNewPersistentVolumeClaim()
                            .withNewMetadata()
                                .addToLabels(customSpecifiedLabelOrAnnotationPvc)
                                .addToAnnotations(customSpecifiedLabelOrAnnotationPvc)
                            .endMetadata()
                        .endPersistentVolumeClaim()
                    .endTemplate()
                    .withStorage(new JbodStorageBuilder().withVolumes(
                            new PersistentClaimStorageBuilder()
                                .withDeleteClaim(false)
                                .withId(0)
                                .withSize("20Gi")
                                .build(),
                            new PersistentClaimStorageBuilder()
                                .withDeleteClaim(true)
                                .withId(1)
                                .withSize("10Gi")
                                .build())
                            .build())
                .endKafka()
                .editZookeeper()
                    .withNewTemplate()
                        .withNewPersistentVolumeClaim()
                            .withNewMetadata()
                                .addToLabels(customSpecifiedLabelOrAnnotationPvc)
                                .addToAnnotations(customSpecifiedLabelOrAnnotationPvc)
                            .endMetadata()
                        .endPersistentVolumeClaim()
                    .endTemplate()
                    .withNewPersistentClaimStorage()
                        .withDeleteClaim(false)
                        .withSize("3Gi")
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        LOGGER.info("--> Test Strimzi related expected labels of managed kubernetes resources <--");

        LOGGER.info("---> PODS <---");

        List<Pod> pods = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getClusterName());

        for (Pod pod : pods) {
            LOGGER.info("Verify labels of  Pod/{} namespace/{}", pod.getMetadata().getName(), pod.getMetadata().getNamespace());
            verifyAppLabels(pod.getMetadata().getLabels());
        }

        LOGGER.info("---> STRIMZI POD SETS <---");

        Map<String, String> kafkaLabelsObtained = StrimziPodSetUtils.getLabelsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());

        LOGGER.info("Verify labels of StrimziPodSet of Kafka resource");
        verifyAppLabels(kafkaLabelsObtained);

        if (!Environment.isKRaftModeEnabled()) {
            Map<String, String> zooLabels = StrimziPodSetUtils.getLabelsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getZookeeperStatefulSetName());

            LOGGER.info("Verify labels of StrimziPodSet of Zookeeper resource");
            verifyAppLabels(zooLabels);
        }

        LOGGER.info("---> SERVICES <---");

        List<Service> services = kubeClient().listServices(testStorage.getNamespaceName()).stream()
            .filter(service -> service.getMetadata().getName().startsWith(testStorage.getClusterName()))
            .collect(Collectors.toList());

        for (Service service : services) {
            LOGGER.info("Verify labels of service/{} namespace/{}", service.getMetadata().getName(), service.getMetadata().getNamespace());
            verifyAppLabels(service.getMetadata().getLabels());
        }

        LOGGER.info("---> SECRETS <---");

        List<Secret> secrets = kubeClient().listSecrets(testStorage.getNamespaceName()).stream()
            .filter(secret -> secret.getMetadata().getName().startsWith(testStorage.getClusterName()) && secret.getType().equals("Opaque"))
            .collect(Collectors.toList());

        for (Secret secret : secrets) {
            LOGGER.info("Verify labels of secret/{} namespace/{}", secret.getMetadata().getName(), secret.getMetadata().getNamespace());
            verifyAppLabelsForSecretsAndConfigMaps(secret.getMetadata().getLabels());
        }

        LOGGER.info("---> CONFIG MAPS <---");

        List<ConfigMap> configMaps = kubeClient().listConfigMapsInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName());

        for (ConfigMap configMap : configMaps) {
            LOGGER.info("Verify labels of ConfigMap/{} namespace/{}", configMap.getMetadata().getName(), configMap.getMetadata().getNamespace());
            verifyAppLabelsForSecretsAndConfigMaps(configMap.getMetadata().getLabels());
        }

        LOGGER.info("---> PVC (both labels and annotation) <---");

        List<PersistentVolumeClaim> pvcs = kubeClient().listPersistentVolumeClaims(testStorage.getNamespaceName(), testStorage.getClusterName()).stream().filter(
            persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getName().contains(testStorage.getClusterName())).collect(Collectors.toList());

        for (PersistentVolumeClaim pvc : pvcs) {
            LOGGER.info("Verify labels of PVC/{} namespace/{}", pvc.getMetadata().getName(), pvc.getMetadata().getNamespace());
            verifyAppLabels(pvc.getMetadata().getLabels());
        }

        LOGGER.info("---> Test Customer specified labels <--");

        LOGGER.info("---> STRIMZI POD SETS <---");

        LOGGER.info("Waiting for Kafka StrimziPodSet  labels existence {}", customSpecifiedLabels);
        StrimziPodSetUtils.waitForStrimziPodSetLabelsChange(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName(), customSpecifiedLabels);

        LOGGER.info("Getting labels from StrimziPodSet set resource");
        kafkaLabelsObtained = StrimziPodSetUtils.getLabelsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());

        LOGGER.info("Asserting presence of custom labels which should be available in Kafka with labels {}", kafkaLabelsObtained);
        for (Map.Entry<String, String> label : customSpecifiedLabels.entrySet()) {
            String customLabelKey = label.getKey();
            String customLabelValue = label.getValue();
            assertThat("Label exists in StrimziPodSet set with concrete value",
                customLabelValue.equals(kafkaLabelsObtained.get(customLabelKey)));
        }

        LOGGER.info("---> PVC (both labels and annotation) <---");
        for (PersistentVolumeClaim pvc : pvcs) {

            LOGGER.info("Asserting presence of custom label and annotation in PVC/{} namespace/{}", pvc.getMetadata().getName(), pvc.getMetadata().getNamespace());
            assertThat(pvc.getMetadata().getLabels().get(pvcLabelOrAnnotationKey), is(pvcLabelOrAnnotationValue));
            assertThat(pvc.getMetadata().getAnnotations().get(pvcLabelOrAnnotationKey), is(pvcLabelOrAnnotationValue));
        }

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("--> Test Customer specific labels manipulation (add, update) of Kafka CR and (update) PVC <--");

        LOGGER.info("Take a snapshot of Zookeeper and Kafka Pods in order to wait for their respawn after rollout");
        Map<String, String> zkPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getZookeeperSelector());
        Map<String, String> kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());

        // key-value pairs modification and addition of user specified labels for kafka CR metadata
        final String firstKafkaLabelValueModified = "first-kafka-label-value-modified";
        final String thirdKafkaLabelKey = "third-kafka-label-key";
        final String thirdKafkaLabelValue = "third-kafka-label-value";
        customSpecifiedLabels.replace(firstKafkaLabelKey, firstKafkaLabelValueModified);
        customSpecifiedLabels.put(thirdKafkaLabelKey, thirdKafkaLabelValue);
        LOGGER.info("New values of labels which are to modify Kafka CR after their replacement and addition of new one are following {}", customSpecifiedLabels);

        // key-value pair modification of user specified label in managed PVCs
        final String pvcLabelOrAnnotationValueModified = "pvc-label-value-modified";
        customSpecifiedLabelOrAnnotationPvc.replace(pvcLabelOrAnnotationKey, pvcLabelOrAnnotationValueModified);
        LOGGER.info("New values of labels which are to modify label and annotation of PVC present in Kafka CR, with following values {}", customSpecifiedLabelOrAnnotationPvc);

        LOGGER.info("Edit Kafka labels in Kafka CR,as well as labels, and annotations of PVCs");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), resource -> {
            for (Map.Entry<String, String> label : customSpecifiedLabels.entrySet()) {
                resource.getMetadata().getLabels().put(label.getKey(), label.getValue());
            }
            resource.getSpec().getKafka().getTemplate().getPersistentVolumeClaim().getMetadata().setLabels(customSpecifiedLabelOrAnnotationPvc);
            resource.getSpec().getKafka().getTemplate().getPersistentVolumeClaim().getMetadata().setAnnotations(customSpecifiedLabelOrAnnotationPvc);
            resource.getSpec().getZookeeper().getTemplate().getPersistentVolumeClaim().getMetadata().setLabels(customSpecifiedLabelOrAnnotationPvc);
            resource.getSpec().getZookeeper().getTemplate().getPersistentVolumeClaim().getMetadata().setAnnotations(customSpecifiedLabelOrAnnotationPvc);
        }, testStorage.getNamespaceName());

        LOGGER.info("Wait for rolling update of Zookeeper and Kafka");
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), 1, zkPods);
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaPods);

        LOGGER.info("---> PVC (both labels and annotation) <---");

        LOGGER.info("Wait for changes in PVC labels and until Kafka becomes ready");
        PersistentVolumeClaimUtils.waitUntilPVCLabelsChange(testStorage.getNamespaceName(), testStorage.getClusterName(), customSpecifiedLabelOrAnnotationPvc, pvcLabelOrAnnotationKey);
        PersistentVolumeClaimUtils.waitUntilPVCAnnotationChange(testStorage.getNamespaceName(), testStorage.getClusterName(), customSpecifiedLabelOrAnnotationPvc, pvcLabelOrAnnotationKey);

        pvcs = kubeClient().listPersistentVolumeClaims(testStorage.getNamespaceName(), testStorage.getClusterName()).stream().filter(
            persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getName().contains(testStorage.getClusterName())).collect(Collectors.toList());
        LOGGER.info(pvcs.toString());

        for (PersistentVolumeClaim pvc : pvcs) {
            LOGGER.info("Verifying replaced PVC/{} label/{}={}, as both label and annotation", pvc.getMetadata().getName(), pvcLabelOrAnnotationKey, pvc.getMetadata().getLabels().get(pvcLabelOrAnnotationKey));

            assertThat(pvc.getMetadata().getLabels().get(pvcLabelOrAnnotationKey), is(pvcLabelOrAnnotationValueModified));
            assertThat(pvc.getMetadata().getAnnotations().get(pvcLabelOrAnnotationKey), is(pvcLabelOrAnnotationValueModified));
        }

        LOGGER.info("---> SERVICES <---");

        LOGGER.info("Waiting for Kafka Service labels changed {}", customSpecifiedLabels);
        ServiceUtils.waitForServiceLabelsChange(testStorage.getNamespaceName(), KafkaResources.brokersServiceName(testStorage.getClusterName()), customSpecifiedLabels);

        LOGGER.info("Verifying Kafka labels via Services");
        Service service = kubeClient().getService(testStorage.getNamespaceName(), KafkaResources.brokersServiceName(testStorage.getClusterName()));

        verifyPresentLabels(customSpecifiedLabels, service.getMetadata().getLabels());

        LOGGER.info("---> CONFIG MAPS <---");

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(testStorage.getClusterName(), kafkaReplicas)) {
            LOGGER.info("Waiting for Kafka ConfigMap {}/{} to have new labels: {}", testStorage.getNamespaceName(), cmName, customSpecifiedLabels);
            ConfigMapUtils.waitForConfigMapLabelsChange(testStorage.getNamespaceName(), cmName, customSpecifiedLabels);

            LOGGER.info("Verifying Kafka labels on ConfigMap {}/{}", testStorage.getNamespaceName(), cmName);
            ConfigMap configMap = kubeClient(testStorage.getNamespaceName()).getConfigMap(testStorage.getNamespaceName(), cmName);

            verifyPresentLabels(customSpecifiedLabels, configMap.getMetadata().getLabels());
        }

        LOGGER.info("---> STRIMZI POD SETS <---");

        LOGGER.info("Waiting for StrimziPodSet labels changed {}", customSpecifiedLabels);
        StrimziPodSetUtils.waitForStrimziPodSetLabelsChange(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName(), customSpecifiedLabels);

        LOGGER.info("Verifying Kafka labels via StrimziPodSet");
        verifyPresentLabels(customSpecifiedLabels, StrimziPodSetUtils.getLabelsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName()));

        LOGGER.info("Verifying via Kafka Pods");
        Map<String, String> podLabels = kubeClient().getPod(testStorage.getNamespaceName(), KafkaResources.kafkaPodName(testStorage.getClusterName(), 0)).getMetadata().getLabels();

        for (Map.Entry<String, String> label : customSpecifiedLabels.entrySet()) {
            assertThat("Label exists in Kafka Pods", label.getValue().equals(podLabels.get(label.getKey())));
        }

        LOGGER.info("Produce and Consume messages to make sure Kafka cluster is not broken by labels and annotations manipulation");
        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    @ParallelNamespaceTest
    void testUOListeningOnlyUsersInSameCluster(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String firstClusterName = "my-cluster-1";
        final String secondClusterName = "my-cluster-2";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(firstClusterName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(secondClusterName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(namespaceName, firstClusterName, userName).build());

        LOGGER.info("Verifying that KafkaUser {} in cluster {} is created", userName, firstClusterName);
        String entityOperatorPodName = kubeClient(namespaceName).listPodNamesInSpecificNamespace(namespaceName, Labels.STRIMZI_NAME_LABEL, KafkaResources.entityOperatorDeploymentName(firstClusterName)).get(0);
        String uOLogs = kubeClient(namespaceName).logsInSpecificNamespace(namespaceName, entityOperatorPodName, "user-operator");
        assertThat(uOLogs, containsString("KafkaUser " + userName + " in namespace " + namespaceName + " was ADDED"));

        LOGGER.info("Verifying that KafkaUser {} in cluster {} is not created", userName, secondClusterName);
        entityOperatorPodName = kubeClient(namespaceName).listPodNamesInSpecificNamespace(namespaceName, Labels.STRIMZI_NAME_LABEL, KafkaResources.entityOperatorDeploymentName(secondClusterName)).get(0);
        uOLogs = kubeClient(namespaceName).logsInSpecificNamespace(namespaceName, entityOperatorPodName, "user-operator");
        assertThat(uOLogs, not(containsString("KafkaUser " + userName + " in namespace " + namespaceName + " was ADDED")));

        LOGGER.info("Verifying that KafkaUser belongs to {} cluster", firstClusterName);
        String kafkaUserResource = cmdKubeClient(namespaceName).getResourceAsYaml("kafkauser", userName);
        assertThat(kafkaUserResource, containsString(Labels.STRIMZI_CLUSTER_LABEL + ": " + firstClusterName));
    }


    /**
     * @description This test case verifies correct storage of messages on disk, and their presence even after rolling update of all Kafka Pods. Test case
     * also checks if offset topic related files are present.
     *
     * @steps
     *  1. - Deploy persistent Kafka with corresponding configuration of offsets topic.
     *     - Kafka is created with expected configuration.
     *  2. - Create Kafka topic with corresponding configuration
     *     - Kafka topic is created with expected configuration.
     *  3. - Execute command to check presence of offsets topic related files.
     *     - Files related to Offset topic are present.
     *  4. - Produce default number of messages to already created topic.
     *     - Produced messages are present.
     *  5. - Perform rolling update on all Kafka Pods, in this case single broker.
     *     - After rolling update is completed all messages are again present, as they were successfully stored on disk.
     *
     * @usecase
     *  - data-storage
     *  - kafka-configuration
     */
    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testMessagesAndConsumerOffsetFilesOnDisk(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(testStorage.getClusterName(), testStorage.getKafkaStatefulSetName());
        final Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "1");
        kafkaConfig.put("offsets.topic.num.partitions", "100");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1, 1)
            .editSpec()
                .editKafka()
                    .withConfig(kafkaConfig)
                .endKafka()
            .endSpec()
            .build());

        Map<String, String> kafkaPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), kafkaSelector);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 1, 1).build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        TestUtils.waitFor("KafkaTopic creation inside kafka pod", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(testStorage.getClusterName(), 0), "/bin/bash",
                        "-c", "cd /var/lib/kafka/data/kafka-log0; ls -1").out().contains(testStorage.getTopicName()));

        String topicDirNameInPod = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(testStorage.getClusterName(), 0), "/bin/bash",
                "-c", "cd /var/lib/kafka/data/kafka-log0; ls -1 | sed -n '/" + testStorage.getTopicName() + "/p'").out();

        String commandToGetDataFromTopic =
                "cd /var/lib/kafka/data/kafka-log0/" + topicDirNameInPod + "/;cat 00000000000000000000.log";

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, KafkaResources.kafkaPodName(testStorage.getClusterName(), 0));
        String topicData = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(testStorage.getClusterName(), 0),
                "/bin/bash", "-c", commandToGetDataFromTopic).out();

        LOGGER.info("Topic {} is present in kafka broker {} with no data", testStorage.getTopicName(), KafkaResources.kafkaPodName(testStorage.getClusterName(), 0));
        assertThat("Topic contains data", topicData, emptyOrNullString());

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Verify presence of files created to store offsets topic");
        String commandToGetFiles = "cd /var/lib/kafka/data/kafka-log0/; ls -l | grep __consumer_offsets | wc -l";
        String result = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(testStorage.getClusterName(), 0),
            "/bin/bash", "-c", commandToGetFiles).out();

        assertThat("Folder kafka-log0 doesn't contain 100 files related to storing consumer offsets", Integer.parseInt(result.trim()) == 100);

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, KafkaResources.kafkaPodName(testStorage.getClusterName(), 0));
        topicData = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(testStorage.getClusterName(), 0), "/bin/bash", "-c",
                commandToGetDataFromTopic).out();

        assertThat("Topic has no data", topicData, notNullValue());

        List<Pod> kafkaPods = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());

        for (Pod kafkaPod : kafkaPods) {
            LOGGER.info("Deleting kafka pod {}", kafkaPod.getMetadata().getName());
            kubeClient(testStorage.getNamespaceName()).deletePod(testStorage.getNamespaceName(), kafkaPod);
        }

        LOGGER.info("Wait for kafka to rolling restart ...");
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), kafkaSelector, 1, kafkaPodsSnapshot);

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, KafkaResources.kafkaPodName(testStorage.getClusterName(), 0));
        topicData = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(testStorage.getClusterName(), 0), "/bin/bash", "-c",
                commandToGetDataFromTopic).out();

        assertThat("Topic has no data", topicData, notNullValue());
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(CRUISE_CONTROL)
    void testReadOnlyRootFileSystem(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        Kafka kafka = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewKafkaContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endKafkaContainer()
                        .endTemplate()
                    .endKafka()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewZookeeperContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endZookeeperContainer()
                        .endTemplate()
                    .endZookeeper()
                    .editEntityOperator()
                        .withNewTemplate()
                            .withNewTlsSidecarContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endTlsSidecarContainer()
                            .withNewTopicOperatorContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endTopicOperatorContainer()
                            .withNewUserOperatorContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endUserOperatorContainer()
                        .endTemplate()
                    .endEntityOperator()
                    .editOrNewKafkaExporter()
                        .withNewTemplate()
                            .withNewContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endContainer()
                        .endTemplate()
                    .endKafkaExporter()
                    .editOrNewCruiseControl()
                        .withNewTemplate()
                            .withNewTlsSidecarContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endTlsSidecarContainer()
                            .withNewCruiseControlContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endCruiseControlContainer()
                        .endTemplate()
                    .endCruiseControl()
                .endSpec()
                .build();

        kafka.getSpec().getEntityOperator().getTemplate().setTopicOperatorContainer(null);

        resourceManager.createResource(extensionContext, kafka);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build());

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
    }

    void verifyVolumeNamesAndLabels(String namespaceName, String clusterName, int kafkaReplicas, int diskCountPerReplica, String diskSizeGi) {
        ArrayList<String> pvcs = new ArrayList<>();

        kubeClient(namespaceName).listPersistentVolumeClaims(namespaceName, clusterName).stream()
            .filter(pvc -> pvc.getMetadata().getName().contains(clusterName + "-kafka"))
            .forEach(volume -> {
                String volumeName = volume.getMetadata().getName();
                pvcs.add(volumeName);
                LOGGER.info("Checking labels for volume:" + volumeName);
                assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
                assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(Kafka.RESOURCE_KIND));
                assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_NAME_LABEL), is(clusterName.concat("-kafka")));
                assertThat(volume.getSpec().getResources().getRequests().get("storage"), is(new Quantity(diskSizeGi, "Gi")));
            });

        LOGGER.info("Checking PVC names included in JBOD array");
        for (int i = 0; i < kafkaReplicas; i++) {
            for (int j = 0; j < diskCountPerReplica; j++) {
                assertThat(pvcs.contains("data-" + j + "-" + clusterName + "-kafka-" + i), is(true));
            }
        }

        LOGGER.info("Checking PVC on Kafka pods");
        for (int i = 0; i < kafkaReplicas; i++) {
            ArrayList<String> dataSourcesOnPod = new ArrayList<>();
            ArrayList<String> pvcsOnPod = new ArrayList<>();

            LOGGER.info("Getting list of mounted data sources and PVCs on Kafka pod " + i);
            for (int j = 0; j < diskCountPerReplica; j++) {
                dataSourcesOnPod.add(kubeClient(namespaceName).getPod(namespaceName, clusterName.concat("-kafka-" + i))
                        .getSpec().getVolumes().get(j).getName());
                pvcsOnPod.add(kubeClient(namespaceName).getPod(namespaceName, clusterName.concat("-kafka-" + i))
                        .getSpec().getVolumes().get(j).getPersistentVolumeClaim().getClaimName());
            }

            LOGGER.info("Verifying mounted data sources and PVCs on Kafka pod " + i);
            for (int j = 0; j < diskCountPerReplica; j++) {
                assertThat(dataSourcesOnPod.contains("data-" + j), is(true));
                assertThat(pvcsOnPod.contains("data-" + j + "-" + clusterName + "-kafka-" + i), is(true));
            }
        }
    }

    void verifyPresentLabels(Map<String, String> labels, Map<String, String> resourceLabels) {
        for (Map.Entry<String, String> label : labels.entrySet()) {
            assertThat("Label exists with concrete value in HasMetadata(Services, CM, STS) resources",
                    label.getValue().equals(resourceLabels.get(label.getKey())));
        }
    }

    void verifyAppLabels(Map<String, String> labels) {
        LOGGER.info("Verifying labels {}", labels);
        assertThat("Label " + Labels.STRIMZI_CLUSTER_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_CLUSTER_LABEL));
        assertThat("Label " + Labels.STRIMZI_KIND_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_KIND_LABEL));
        assertThat("Label " + Labels.STRIMZI_NAME_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_NAME_LABEL));
    }

    void verifyAppLabelsForSecretsAndConfigMaps(Map<String, String> labels) {
        LOGGER.info("Verifying labels {}", labels);
        assertThat("Label " + Labels.STRIMZI_CLUSTER_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_CLUSTER_LABEL));
        assertThat("Label " + Labels.STRIMZI_KIND_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_KIND_LABEL));
    }

    protected void afterEachMayOverride(ExtensionContext extensionContext) throws Exception {
        resourceManager.deleteResources(extensionContext);

        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);

        if (KafkaResource.kafkaClient().inNamespace(namespaceName).withName(OPENSHIFT_CLUSTER_NAME).get() != null) {
            cmdKubeClient(namespaceName).deleteByName(Kafka.RESOURCE_KIND, OPENSHIFT_CLUSTER_NAME);
        }

        kubeClient(namespaceName).listPods(namespaceName).stream()
            .filter(p -> p.getMetadata().getName().startsWith(OPENSHIFT_CLUSTER_NAME))
            .forEach(p -> PodUtils.deletePodWithWait(p.getMetadata().getNamespace(), p.getMetadata().getName()));

        kubeClient(namespaceName).getClient().resources(KafkaTopic.class, KafkaTopicList.class).inNamespace(namespaceName).delete();
        kubeClient(namespaceName).getClient().persistentVolumeClaims().inNamespace(namespaceName).delete();

        testSuiteNamespaceManager.deleteParallelNamespace(extensionContext);
    }
}
