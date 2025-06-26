/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.enums.LogLevel;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.JvmOptionsBuilder;
import io.strimzi.api.kafka.model.common.SystemProperty;
import io.strimzi.api.kafka.model.common.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.common.template.ResourceTemplateBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.KindNotSupported;
import io.strimzi.systemtest.annotations.MultiNodeClusterOnly;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.kubernetes.ConfigMapTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.VerificationUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.ConfigMapUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@Tag(REGRESSION)
@SuppressWarnings("checkstyle:ClassFanOutComplexity")
@SuiteDoc(
    description = @Desc("Test suite containing Kafka related stuff (i.e., JVM resources, EO, TO or UO removal from Kafka cluster), which ensures proper functioning of Kafka clusters."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator across all namespaces, with custom configuration.", expected = "Cluster Operator is deployed.")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA)
    }
)
class KafkaST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaST.class);
    private static final String OPENSHIFT_CLUSTER_NAME = "openshift-my-cluster";

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test case verifies that Pod's resources (limits and requests), custom JVM configurations, and expected Java configuration are propagated correctly to Pods, containers, and processes."),
        steps = {
            @Step(value = "Deploy Kafka and its components with custom specifications, including specifying resources and JVM configuration.", expected = "Kafka and Entity Operator are deployed."),
            @Step(value = "For each component (Kafka, Topic Operator, User Operator), verify specified configuration of JVM, resources, and also environment variables.", expected = "Each of the components has requests and limits assigned correctly, JVM, and environment variables configured according to the specification."),
            @Step(value = "Wait for a time to observe that no initiated components need rolling update.", expected = "All Kafka components remain in stable state.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testJvmAndResources() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        ArrayList<SystemProperty> javaSystemProps = new ArrayList<>();
        javaSystemProps.add(new SystemPropertyBuilder().withName("javax.net.debug")
                .withValue("verbose").build());
        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        ResourceRequirements brokersResReq = new ResourceRequirementsBuilder()
            .addToLimits("memory", new Quantity("1.5Gi"))
            .addToLimits("cpu", new Quantity("1"))
            .addToRequests("memory", new Quantity("1Gi"))
            .addToRequests("cpu", new Quantity("50m"))
            .build();

        ResourceRequirements controlResReq = new ResourceRequirementsBuilder()
            .addToLimits("memory", new Quantity("1G"))
            .addToLimits("cpu", new Quantity("0.5"))
            .addToRequests("memory", new Quantity("0.5G"))
            .addToRequests("cpu", new Quantity("25m"))
            .build();

        JvmOptions brokerJvmOptions = new JvmOptionsBuilder()
            .withXmx("1g")
            .withXms("512m")
            .withXx(jvmOptionsXX)
            .build();

        JvmOptions controlJvmOptions = new JvmOptionsBuilder()
            .withXmx("1G")
            .withXms("512M")
            .withXx(jvmOptionsXX)
            .build();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1)
                .editSpec()
                    .withResources(brokersResReq)
                    .withJvmOptions(brokerJvmOptions)
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1)
                .editSpec()
                    .withResources(controlResReq)
                    .withJvmOptions(controlJvmOptions)
                .endSpec()
                .build()
        );

        Kafka kafka = KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editSpec()
                .editKafka()
                    .withResources(brokersResReq)
                    .withJvmOptions(brokerJvmOptions)
                .endKafka()
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
            .build();

        KubeResourceManager.get().createResourceWithWait(kafka);

        // Make snapshots for Kafka cluster to make sure that there is no rolling update after CO reconciliation
        final String eoDepName = KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName());
        final Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        final Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        final Map<String, String> eoPods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), eoDepName);

        String brokerPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();

        LOGGER.info("Verifying resources and JVM configuration of Kafka Broker Pod");
        VerificationUtils.assertPodResourceRequests(testStorage.getNamespaceName(), brokerPodName, "kafka",
                "1536Mi", "1", "1Gi", "50m");
        VerificationUtils.assertJvmOptions(testStorage.getNamespaceName(), brokerPodName, "kafka",
                "-Xmx1g", "-Xms512m", "-XX:+UseG1GC");

        LOGGER.info("Verifying resources, JVM configuration, and environment variables of Entity Operator's components");

        Optional<Pod> pod = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName())
                .stream().filter(p -> p.getMetadata().getName().startsWith(KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName())))
                .findFirst();
        assertThat("EO Pod does not exist", pod.isPresent(), is(true));

        VerificationUtils.assertPodResourceRequests(testStorage.getNamespaceName(), pod.get().getMetadata().getName(), "topic-operator",
                "1Gi", "500m", "384Mi", "25m");
        VerificationUtils.assertPodResourceRequests(testStorage.getNamespaceName(), pod.get().getMetadata().getName(), "user-operator",
                "512M", "300m", "256M", "30m");
        VerificationUtils.assertJvmOptions(testStorage.getNamespaceName(), pod.get().getMetadata().getName(), "topic-operator",
                "-Xmx2G", "-Xms1024M", null);
        VerificationUtils.assertJvmOptions(testStorage.getNamespaceName(), pod.get().getMetadata().getName(), "user-operator",
                "-Xmx1G", "-Xms512M", null);

        String eoPod = eoPods.keySet().toArray()[0].toString();
        KubeResourceManager.get().kubeClient().getClient().pods().inNamespace(testStorage.getNamespaceName()).withName(eoPod).get().getSpec().getContainers().forEach(container -> {
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
        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getControllerSelector(), controllerPods);
        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods);
        DeploymentUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), eoDepName, eoPods);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test case verifies the correct deployment of the Entity Operator, including both the User Operator and Topic Operator. First, the Entity Operator is modified to exclude the User Operator. Then, it's restored to its default configuration, which includes the User Operator. Next, the Topic Operator is removed, followed by the User Operator, with the Topic Operator already excluded"),
        steps = {
            @Step(value = "Deploy Kafka with Entity Operator set.", expected = "Kafka is deployed, and Entity Operator consists of both Topic Operator and User Operator."),
            @Step(value = "Remove User Operator from the Kafka specification.", expected = "User Operator container is deleted."),
            @Step(value = "Set User Operator back in the Kafka specification.", expected = "User Operator container is recreated."),
            @Step(value = "Remove Topic Operator from the Kafka specification.", expected = "Topic Operator container is removed from Entity Operator."),
            @Step(value = "Remove User Operator from the Kafka specification.", expected = "Entity Operator Pod is removed, as there are no other containers present.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testRemoveComponentsFromEntityOperator() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        LOGGER.info("Deploying Kafka cluster {}", testStorage.getClusterName());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        Map<String, String> eoSnapshot = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName()));

        LOGGER.info("Remove User Operator from Entity Operator");
        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> k.getSpec().getEntityOperator().setUserOperator(null));
        
        // Waiting when EO pod will be recreated without UO
        eoSnapshot = DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName()), 1, eoSnapshot);

        PodUtils.waitUntilPodContainersCount(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName()), 1);

        // Checking that UO was removed
        KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName())).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), not(containsString("user-operator")));
            });
        });

        LOGGER.info("Recreate User Operator");
        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> k.getSpec().getEntityOperator().setUserOperator(new EntityUserOperatorSpec()));
        //Waiting when EO pod will be recreated with UO
        eoSnapshot = DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName()), 1, eoSnapshot);

        PodUtils.waitUntilPodContainersCount(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName()), 2);

        LOGGER.info("Verifying that Entity Operator and all its component are correctly recreated");
        // names of containers present in EO pod
        List<String> entityOperatorContainerNames = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName()))
                .get(0).getSpec().getContainers()
                .stream()
                .map(Container::getName)
                .toList();

        assertThat("user-operator container is not present in EO", entityOperatorContainerNames.stream().anyMatch(name -> name.contains("user-operator")));
        assertThat("topic-operator container is not present in EO", entityOperatorContainerNames.stream().anyMatch(name -> name.contains("topic-operator")));

        LOGGER.info("Remove Topic Operator from Entity Operator");
        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> k.getSpec().getEntityOperator().setTopicOperator(null));
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName()), 1, eoSnapshot);
        PodUtils.waitUntilPodContainersCount(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName()), 1);

        //Checking that TO was removed
        LOGGER.info("Verifying that Topic Operator container is no longer present in Entity Operator Pod");
        KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName())).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), not(containsString("topic-operator")));
            });
        });

        LOGGER.info("Remove User Operator, after removed Topic Operator");
        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> {
            k.getSpec().getEntityOperator().setUserOperator(null);
        });

        // both TO and UO are unset, which means EO should not be deployed
        LOGGER.info("Waiting for deletion of Entity Operator Pod");
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName()), 0);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test case verifies Kafka running with persistent JBOD storage, and configured with the `deleteClaim`  storage property."),
        steps = {
            @Step(value = "Deploy Kafka with persistent storage and JBOD storage with 2 volumes, both of which are configured to delete their Persistent Volume Claims on Kafka cluster un-provision.", expected = "Kafka is deployed, volumes are labeled and linked to Pods correctly."),
            @Step(value = "Verify that labels in Persistent Volume Claims are set correctly.", expected = "Persistent Volume Claims contains expected labels and values."),
            @Step(value = "Modify Kafka CustomResource, specifically 'deleteClaim' property of its first Kafka Volume.", expected = "Kafka CR is successfully modified, annotation of according Persistent Volume Claim is changed afterwards by Cluster Operator."),
            @Step(value = "Delete Kafka cluster.", expected = "Kafka cluster and its components are deleted, including Persistent Volume Claim of Volume with 'deleteClaim' property set to true."),
            @Step(value = "Verify remaining Persistent Volume Claims.", expected = "Persistent Volume Claim referenced by volume of formerly deleted Kafka CustomResource with property 'deleteClaim' set to true is still present.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    @SuppressWarnings("deprecation") // Storage is deprecated, but some API methods are still called here
    void testKafkaJBODDeleteClaimsTrueFalse() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int kafkaReplicas = 2;
        final String diskSizeGi = "10";

        //Volume Storages (original and modified)
        PersistentClaimStorage idZeroVolumeOriginal = new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize(diskSizeGi + "Gi").build();
        PersistentClaimStorage idOneVolumeOriginal = new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize(diskSizeGi + "Gi").build();
        PersistentClaimStorage idZeroVolumeModified = new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(0).withSize(diskSizeGi + "Gi").build();

        JbodStorage jbodStorage = new JbodStorageBuilder().withVolumes(idZeroVolumeOriginal, idOneVolumeOriginal).build();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), kafkaReplicas)
                .editSpec()
                    .withStorage(jbodStorage)
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), kafkaReplicas).build()
        );

        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaReplicas)
            .editSpec()
                .editKafka()
                    .withStorage(jbodStorage)
                .endKafka()
            .endSpec()
            .build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        // kafka cluster already deployed
        TestUtils.waitFor("expected labels on PVCs", TestConstants.GLOBAL_POLL_INTERVAL_5_SECS, TestConstants.GLOBAL_STATUS_TIMEOUT, () -> {
            try {
                verifyVolumeNamesAndLabels(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getBrokerComponentName(), kafkaReplicas, 2, diskSizeGi);
                return true;
            } catch (AssertionError ex) {
                LOGGER.info("Some of the expected labels are not in place, rerunning the check...");
                return false;
            }
        });

        //change value of first PVC to delete its claim once Kafka is deleted.
        LOGGER.info("Update Volume with id=0 in Kafka CR by setting 'Delete Claim' property to false");

        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), resource -> {
            LOGGER.debug(resource.getMetadata().getName());
            JbodStorage jBODVolumeStorage = (JbodStorage) resource.getSpec().getStorage();
            jBODVolumeStorage.setVolumes(List.of(idZeroVolumeModified, idOneVolumeOriginal));
        });

        TestUtils.waitFor("PVC(s)' annotation to change according to Kafka JBOD storage 'delete claim'", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> PersistentVolumeClaimUtils.listPVCsByNameSubstring(testStorage.getNamespaceName(), testStorage.getClusterName()).stream()
                .filter(pvc -> pvc.getMetadata().getName().startsWith("data-0") && pvc.getMetadata().getName().contains(testStorage.getBrokerComponentName()))
                .allMatch(volume -> "false".equals(volume.getMetadata().getAnnotations().get("strimzi.io/delete-claim")))
        );

        final int volumesCount = PersistentVolumeClaimUtils.listPVCsByNameSubstring(testStorage.getNamespaceName(), testStorage.getBrokerComponentName()).size();

        LOGGER.info("Deleting Kafka: {}/{} cluster", testStorage.getNamespaceName(), testStorage.getClusterName());
        // we cannot use ResourceManager here, as it would delete all the PVCs (part of the KafkaResource#delete method)
        KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).deleteByName(Kafka.RESOURCE_KIND, testStorage.getClusterName());
        KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).deleteByName(KafkaNodePool.RESOURCE_KIND, testStorage.getBrokerPoolName());

        LOGGER.info("Waiting for PVCs deletion");
        PersistentVolumeClaimUtils.waitForJbodStorageDeletion(testStorage.getNamespaceName(), volumesCount, testStorage.getBrokerComponentName(), List.of(idZeroVolumeModified, idOneVolumeOriginal));

        LOGGER.info("Verifying that PVC which are supposed to remain, really persist even after Kafka cluster un-deployment");
        List<String> remainingPVCNames =  PersistentVolumeClaimUtils.listPVCsByNameSubstring(testStorage.getNamespaceName(), testStorage.getBrokerComponentName()).stream().map(e -> e.getMetadata().getName()).toList();
        brokerPods.keySet().forEach(broker -> assertThat("Kafka Broker: " + broker + " does not preserve its JBOD storage's PVC",
            remainingPVCNames.stream().anyMatch(e -> e.equals("data-0-" + broker))));
    }

    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    @TestDoc(
        description = @Desc("Test regenerates certificates after changing Kafka's external address."),
        steps = {
            @Step(value = "Create Kafka without external listener.", expected = "Kafka instance is created without an external listener."),
            @Step(value = "Edit Kafka to include an external listener.", expected = "External listener is correctly added to the Kafka instance."),
            @Step(value = "Wait until the Kafka component has rolled.", expected = "Kafka component rolls successfully with the new external listener."),
            @Step(value = "Compare Kafka broker secrets before and after adding external listener.", expected = "Secrets are different before and after adding the external listener.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testRegenerateCertExternalAddressChange() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        LOGGER.info("Creating Kafka without external listener");
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        Map<String, Secret> secretsWithoutExt = KubeResourceManager.get().kubeClient().getClient()
            .secrets().inNamespace(testStorage.getNamespaceName()).list().getItems()
            .stream()
            .collect(Collectors.toMap(secret -> secret.getMetadata().getName(), secret -> secret));

        LOGGER.info("Editing Kafka with external listener");
        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            List<GenericKafkaListener> lst = asList(
                    new GenericKafkaListenerBuilder()
                            .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build(),
                    new GenericKafkaListenerBuilder()
                            .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(true)
                            .withNewConfiguration()
                                .withFinalizers(LB_FINALIZERS)
                            .endConfiguration()
                            .build()
            );
            kafka.getSpec().getKafka().setListeners(lst);
        });

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector()));

        LOGGER.info("Checking Secrets");
        KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), KafkaComponents.getBrokerPodSetName(testStorage.getClusterName())).forEach(kafkaPod -> {
            String kafkaPodName = kafkaPod.getMetadata().getName();
            Secret secretWithExt = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(kafkaPodName).get();
            assertThat(secretWithExt.getData().get(kafkaPodName + ".crt"), is(not(secretsWithoutExt.get(kafkaPodName).getData().get(kafkaPodName + ".crt"))));
            assertThat(secretWithExt.getData().get(kafkaPodName + ".key"), is(not(secretsWithoutExt.get(kafkaPodName).getData().get(kafkaPodName + ".key"))));
        });
    }

    @ParallelNamespaceTest
    @SuppressWarnings({"checkstyle:JavaNCSS", "checkstyle:NPathComplexity", "checkstyle:MethodLength", "checkstyle:CyclomaticComplexity"})
    @TestDoc(
        description = @Desc("This test case verifies the presence of expected Strimzi specific labels, also labels and annotations specified by user. Some user-specified labels are later modified (new one is added, one is modified) which triggers rolling update after which all changes took place as expected."),
        steps = {
            @Step(value = "Deploy Kafka with persistent storage and specify custom labels in CR metadata, and also other labels and annotation in PVC metadata.", expected = "Kafka is deployed with its default labels and all others specified by user."),
            @Step(value = "Deploy Producer and Consumer configured to produce and consume default number of messages, to make sure Kafka works as expected.", expected = "Producer and Consumer are able to produce and consume messages respectively."),
            @Step(value = "Modify configuration of Kafka CR with addition of new labels and modification of existing.", expected = "Kafka is rolling and new labels are present in Kafka CR, and managed resources."),
            @Step(value = "Deploy Producer and Consumer configured to produce and consume default number of messages, to make sure Kafka works as expected.", expected = "Producer and Consumer are able to produce and consume messages respectively.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testLabelsExistenceAndManipulation() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

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

        JbodStorage jbodStorage = new JbodStorageBuilder()
            .withVolumes(
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
            .build();

        ResourceTemplate pvcResourceTemplate = new ResourceTemplateBuilder()
            .withNewMetadata()
                .addToLabels(customSpecifiedLabelOrAnnotationPvc)
                .addToAnnotations(customSpecifiedLabelOrAnnotationPvc)
            .endMetadata()
            .build();

        PersistentClaimStorage persistentClaimStorage = new PersistentClaimStorageBuilder()
            .withDeleteClaim(false)
            .withSize("3Gi")
            .build();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                .editMetadata()
                    .addToLabels(customSpecifiedLabels)
                .endMetadata()
                .editSpec()
                    .withNewTemplate()
                        .withPersistentVolumeClaim(pvcResourceTemplate)
                    .endTemplate()
                    .withStorage(jbodStorage)
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1)
                .editSpec()
                    .withNewTemplate()
                        .withPersistentVolumeClaim(pvcResourceTemplate)
                    .endTemplate()
                    .withStorage(persistentClaimStorage)
                .endSpec()
                .build()
        );

        final KafkaBuilder kafkaBuilder = KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editMetadata()
                .withLabels(customSpecifiedLabels)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withNewTemplate()
                        .withPersistentVolumeClaim(pvcResourceTemplate)
                    .endTemplate()
                    .withStorage(jbodStorage)
                .endKafka()
            .endSpec();

        KubeResourceManager.get().createResourceWithWait(kafkaBuilder.build());
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        LOGGER.info("--> Test Strimzi related expected labels of managed kubernetes resources <--");

        LOGGER.info("---> PODS <---");

        List<Pod> pods = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getClusterName());

        for (Pod pod : pods) {
            LOGGER.info("Verifying labels of  Pod: {}/{}", pod.getMetadata().getNamespace(), pod.getMetadata().getName());
            verifyAppLabels(pod.getMetadata().getLabels());
        }

        LOGGER.info("---> STRIMZI POD SETS <---");

        Map<String, String> kafkaLabelsObtained = StrimziPodSetUtils.getLabelsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName());

        LOGGER.info("Verifying labels of StrimziPodSet of Kafka resource - broker role");
        verifyAppLabels(kafkaLabelsObtained);

        kafkaLabelsObtained = StrimziPodSetUtils.getLabelsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getControllerComponentName());

        LOGGER.info("Verifying labels of StrimziPodSet of Kafka resource - controller role");
        verifyAppLabels(kafkaLabelsObtained);

        LOGGER.info("---> SERVICES <---");

        List<Service> services = KubeResourceManager.get().kubeClient().getClient().services().inNamespace(testStorage.getNamespaceName()).list().getItems()
            .stream()
            .filter(service -> service.getMetadata().getName().startsWith(testStorage.getClusterName())).toList();

        for (Service service : services) {
            LOGGER.info("Verifying labels of Service: {}/{}", service.getMetadata().getNamespace(), service.getMetadata().getName());
            verifyAppLabels(service.getMetadata().getLabels());
        }

        LOGGER.info("---> SECRETS <---");

        List<Secret> secrets = KubeResourceManager.get().kubeClient().getClient()
            .secrets().inNamespace(testStorage.getNamespaceName()).list().getItems()
            .stream().filter(secret -> secret.getMetadata().getName().startsWith(testStorage.getClusterName()) && secret.getType().equals("Opaque")).toList();

        for (Secret secret : secrets) {
            LOGGER.info("Verifying labels of Secret: {}/{}", secret.getMetadata().getNamespace(), secret.getMetadata().getName());
            verifyAppLabelsForSecretsAndConfigMaps(secret.getMetadata().getLabels());
        }

        LOGGER.info("---> CONFIG MAPS <---");

        List<ConfigMap> configMaps = ConfigMapUtils.listWithPrefix(testStorage.getNamespaceName(), testStorage.getClusterName());

        for (ConfigMap configMap : configMaps) {
            LOGGER.info("Verifying labels of ConfigMap: {}/{}", configMap.getMetadata().getNamespace(), configMap.getMetadata().getName());
            verifyAppLabelsForSecretsAndConfigMaps(configMap.getMetadata().getLabels());
        }

        LOGGER.info("---> PVC (both labels and annotation) <---");

        List<PersistentVolumeClaim> pvcs = PersistentVolumeClaimUtils.listPVCsByNameSubstring(testStorage.getNamespaceName(), testStorage.getClusterName()).stream().filter(
            persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getName().contains(testStorage.getClusterName())).collect(Collectors.toList());

        for (PersistentVolumeClaim pvc : pvcs) {
            LOGGER.info("Verifying labels of PVC {}/{}", pvc.getMetadata().getNamespace(), pvc.getMetadata().getName());
            verifyAppLabels(pvc.getMetadata().getLabels());
        }

        LOGGER.info("---> Test Customer specified labels <--");

        LOGGER.info("---> STRIMZI POD SETS <---");

        LOGGER.info("Waiting for Kafka StrimziPodSet  labels existence {}", customSpecifiedLabels);
        StrimziPodSetUtils.waitForStrimziPodSetLabelsChange(testStorage.getNamespaceName(), testStorage.getBrokerComponentName(), customSpecifiedLabels);

        LOGGER.info("Getting labels from StrimziPodSet set resource");
        kafkaLabelsObtained = StrimziPodSetUtils.getLabelsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName());

        LOGGER.info("Asserting presence of custom labels which should be available in Kafka with labels {}", kafkaLabelsObtained);
        for (Map.Entry<String, String> label : customSpecifiedLabels.entrySet()) {
            String customLabelKey = label.getKey();
            String customLabelValue = label.getValue();
            assertThat("Label exists in StrimziPodSet set with concrete value",
                customLabelValue.equals(kafkaLabelsObtained.get(customLabelKey)));
        }

        LOGGER.info("---> PVC (both labels and annotation) <---");
        for (PersistentVolumeClaim pvc : pvcs) {

            LOGGER.info("Asserting presence of custom label and annotation in PVC {}/{}", pvc.getMetadata().getNamespace(), pvc.getMetadata().getName());
            assertThat(pvc.getMetadata().getLabels().get(pvcLabelOrAnnotationKey), is(pvcLabelOrAnnotationValue));
            assertThat(pvc.getMetadata().getAnnotations().get(pvcLabelOrAnnotationKey), is(pvcLabelOrAnnotationValue));
        }

        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage);
        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("--> Test Customer specific labels manipulation (add, update) of Kafka CR and (update) PVC <--");

        LOGGER.info("Take a snapshot of Kafka Pods in order to wait for their respawn after rollout");
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

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

        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), resource -> {
            for (Map.Entry<String, String> label : customSpecifiedLabels.entrySet()) {
                resource.getMetadata().getLabels().put(label.getKey(), label.getValue());
            }
            resource.getSpec().getTemplate().getPersistentVolumeClaim().getMetadata().setLabels(customSpecifiedLabelOrAnnotationPvc);
            resource.getSpec().getTemplate().getPersistentVolumeClaim().getMetadata().setAnnotations(customSpecifiedLabelOrAnnotationPvc);
        });

        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), resource -> {
            for (Map.Entry<String, String> label : customSpecifiedLabels.entrySet()) {
                resource.getMetadata().getLabels().put(label.getKey(), label.getValue());
            }
            resource.getSpec().getTemplate().getPersistentVolumeClaim().getMetadata().setLabels(customSpecifiedLabelOrAnnotationPvc);
            resource.getSpec().getTemplate().getPersistentVolumeClaim().getMetadata().setAnnotations(customSpecifiedLabelOrAnnotationPvc);
        });

        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), resource -> {
            for (Map.Entry<String, String> label : customSpecifiedLabels.entrySet()) {
                resource.getMetadata().getLabels().put(label.getKey(), label.getValue());
            }
        });

        LOGGER.info("Waiting for rolling update of Kafka");
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 1, controllerPods);
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        LOGGER.info("---> PVC (both labels and annotation) <---");

        LOGGER.info("Waiting for changes in PVC labels and Kafka to become ready");
        PersistentVolumeClaimUtils.waitUntilPVCLabelsChange(testStorage.getNamespaceName(), testStorage.getClusterName(), customSpecifiedLabelOrAnnotationPvc, pvcLabelOrAnnotationKey);
        PersistentVolumeClaimUtils.waitUntilPVCAnnotationChange(testStorage.getNamespaceName(), testStorage.getClusterName(), customSpecifiedLabelOrAnnotationPvc, pvcLabelOrAnnotationKey);

        pvcs = PersistentVolumeClaimUtils.listPVCsByNameSubstring(testStorage.getNamespaceName(), testStorage.getClusterName()).stream().filter(
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
        Service service = KubeResourceManager.get().kubeClient().getClient().services().inNamespace(testStorage.getNamespaceName()).withName(KafkaResources.brokersServiceName(testStorage.getClusterName())).get();

        verifyPresentLabels(customSpecifiedLabels, service.getMetadata().getLabels());

        LOGGER.info("---> CONFIG MAPS <---");

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(testStorage.getNamespaceName(), testStorage.getClusterName())) {
            LOGGER.info("Waiting for Kafka ConfigMap {}/{} to have new labels: {}", testStorage.getNamespaceName(), cmName, customSpecifiedLabels);
            ConfigMapUtils.waitForConfigMapLabelsChange(testStorage.getNamespaceName(), cmName, customSpecifiedLabels);

            LOGGER.info("Verifying Kafka labels on ConfigMap {}/{}", testStorage.getNamespaceName(), cmName);
            ConfigMap configMap = KubeResourceManager.get().kubeClient().getClient().configMaps().inNamespace(testStorage.getNamespaceName()).withName(cmName).get();

            verifyPresentLabels(customSpecifiedLabels, configMap.getMetadata().getLabels());
        }

        LOGGER.info("---> STRIMZI POD SETS <---");

        LOGGER.info("Waiting for StrimziPodSet labels changed {}", customSpecifiedLabels);
        StrimziPodSetUtils.waitForStrimziPodSetLabelsChange(testStorage.getNamespaceName(), testStorage.getBrokerComponentName(), customSpecifiedLabels);

        LOGGER.info("Verifying Kafka labels via StrimziPodSet");
        verifyPresentLabels(customSpecifiedLabels, StrimziPodSetUtils.getLabelsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName()));

        LOGGER.info("Verifying via Kafka Pods");
        Map<String, String> podLabels = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).stream().findFirst().orElseThrow().getMetadata().getLabels();

        for (Map.Entry<String, String> label : customSpecifiedLabels.entrySet()) {
            assertThat("Label exists in Kafka Pods", label.getValue().equals(podLabels.get(label.getKey())));
        }

        LOGGER.info("Produce and Consume messages to make sure Kafka cluster is not broken by labels and annotations manipulation");
        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test case verifies correct storage of messages on disk, and their presence even after rolling update of all Kafka Pods. Test case also checks if offset topic related files are present."),
        steps = {
            @Step(value = "Deploy persistent Kafka with corresponding configuration of offsets topic.", expected = "Kafka is created with expected configuration."),
            @Step(value = "Create KafkaTopic with corresponding configuration.", expected = "KafkaTopic is created with expected configuration."),
            @Step(value = "Execute command to check presence of offsets topic related files.", expected = "Files related to Offset topic are present."),
            @Step(value = "Produce default number of messages to already created topic.", expected = "Produced messages are present."),
            @Step(value = "Perform rolling update on all Kafka Pods, in this case single broker.", expected = "After rolling update is completed all messages are again present, as they were successfully stored on disk.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testMessagesAndConsumerOffsetFilesOnDisk() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "1");
        kafkaConfig.put("offsets.topic.num.partitions", "100");

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editSpec()
                .editKafka()
                    .withConfig(kafkaConfig)
                .endKafka()
            .endSpec()
            .build());

        Map<String, String> brokerPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 1, 1).build());

        String brokerPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();

        TestUtils.waitFor("KafkaTopic creation inside Kafka Pod", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                String output = KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(brokerPodName, "/bin/bash",
                    "-c", "cd /var/lib/kafka/data/kafka-log0; ls -1").out();
                if (output.contains(testStorage.getTopicName())) {
                    return true;
                } else {
                    LOGGER.debug("{} should be in {}, trying again...", testStorage.getTopicName(), output);
                    return false;
                }
            });

        String topicDirNameInPod = KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(brokerPodName, "/bin/bash",
            "-c", "cd /var/lib/kafka/data/kafka-log0; ls -1 | sed -n '/" + testStorage.getTopicName() + "/p'").out();

        String commandToGetDataFromTopic =
            "cd /var/lib/kafka/data/kafka-log0/" + topicDirNameInPod + "/;cat 00000000000000000000.log";

        LOGGER.info("Executing command: {} in {}", commandToGetDataFromTopic, brokerPodName);
        String topicData = KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(brokerPodName,
            "/bin/bash", "-c", commandToGetDataFromTopic).out();

        LOGGER.info("Topic: {} is present in Kafka Broker: {} with no data", testStorage.getTopicName(), brokerPodName);
        assertThat("Topic contains data", topicData, emptyOrNullString());

        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage);
        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Verifying presence of files created to store offsets Topic");
        String commandToGetFiles = "cd /var/lib/kafka/data/kafka-log0/; ls -l | grep __consumer_offsets | wc -l";
        String result = KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(brokerPodName,
            "/bin/bash", "-c", commandToGetFiles).out();

        assertThat("Folder kafka-log0 doesn't contain 100 files related to storing consumer offsets", Integer.parseInt(result.trim()) == 100);

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, brokerPodName);
        topicData = KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(brokerPodName,
            "/bin/bash", "-c", commandToGetDataFromTopic).out();

        assertThat("Topic has no data", topicData, notNullValue());

        List<Pod> brokerPods = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getBrokerComponentName());

        for (Pod kafkaPod : brokerPods) {
            LOGGER.info("Deleting Kafka Pod: {}/{}", testStorage.getNamespaceName(), kafkaPod.getMetadata().getName());
            KubeResourceManager.get().deleteResourceWithoutWait(kafkaPod);
        }

        LOGGER.info("Waiting for Kafka rolling restart");
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 1, brokerPodsSnapshot);

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, brokerPodName);
        topicData = KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(brokerPodName,
            "/bin/bash", "-c", commandToGetDataFromTopic).out();

        assertThat("Topic has no data", topicData, notNullValue());
    }

    @ParallelNamespaceTest
    @Tag(CRUISE_CONTROL)
    @TestDoc(
        description = @Desc("This test case verifies that Kafka (with all its components, including Entity Operator, KafkaExporter, CruiseControl) configured with 'withReadOnlyRootFilesystem' can be deployed and also works correctly."),
        steps = {
            @Step(value = "Deploy persistent Kafka with 3 replicas, Entity Operator, CruiseControl, and KafkaExporter. Each component has configuration 'withReadOnlyRootFilesystem' set to true.", expected = "Kafka and its components are deployed."),
            @Step(value = "Create Kafka producer and consumer.", expected = "Kafka clients are successfully created."),
            @Step(value = "Produce and consume messages using created clients.", expected = "Messages are successfully sent and received.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testReadOnlyRootFileSystem() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        Kafka kafka = KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewKafkaContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endKafkaContainer()
                        .endTemplate()
                    .endKafka()
                    .editEntityOperator()
                        .editOrNewTemplate()
                            .editOrNewTopicOperatorContainer()
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
                            .withNewCruiseControlContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endCruiseControlContainer()
                        .endTemplate()
                    .endCruiseControl()
                .endSpec()
                .build();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewTemplate()
                        .withNewKafkaContainer()
                            .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                        .endKafkaContainer()
                    .endTemplate()
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .withNewTemplate()
                        .withNewKafkaContainer()
                            .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                        .endKafkaContainer()
                    .endTemplate()
                .endSpec()
                .build()
        );
        KubeResourceManager.get().createResourceWithWait(kafka);

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClientBuilder(testStorage).build();
        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test to ensure that deploying Kafka with an unsupported version results in the expected error."),
        steps = {
            @Step(value = "Initialize test storage with current context.", expected = "Test storage is initialized."),
            @Step(value = "Create KafkaNodePools", expected = "KafkaNodePools are created and ready"),
            @Step(value = "Deploy Kafka with a non-existing version", expected = "Kafka deployment with non-supported version begins"),
            @Step(value = "Log Kafka deployment process", expected = "Log entry for Kafka deployment is created"),
            @Step(value = "Wait for Kafka to not be ready", expected = "Kafka is not ready as expected"),
            @Step(value = "Verify Kafka status message for unsupported version", expected = "Error message for unsupported version is found in Kafka status")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testDeployUnsupportedKafka() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        String nonExistingVersion = "6.6.6";
        String nonExistingVersionMessage = "Unsupported Kafka.spec.kafka.version: " + nonExistingVersion + ". Supported versions are:.*";

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithoutWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editSpec()
                .editKafka()
                    .withVersion(nonExistingVersion)
                .endKafka()
            .endSpec()
            .build()
        );

        LOGGER.info("Kafka with version {} deployed.", nonExistingVersion);

        KafkaUtils.waitForKafkaNotReady(testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(testStorage.getNamespaceName(), testStorage.getClusterName(), nonExistingVersionMessage);
    }

    @KindNotSupported       // Storage Class standard does not support resizing of volumes
    @MultiNodeClusterOnly   // in multi-node we use different Storage Class, which support re-sizing of volumes
    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test verifies the functionality of resizing JBOD storage volumes on a Kafka cluster. It checks that the system can handle volume size changes and performs a rolling update to apply these changes."),
        steps = {
            @Step(value = "Deploy a Kafka cluster with JBOD storage and initial volume sizes.", expected = "Kafka cluster is operational."),
            @Step(value = "Produce and consume messages continuously to simulate cluster activity.", expected = "Message traffic is consistent."),
            @Step(value = "Increase the size of one of the JBOD volumes.", expected = "Volume size change is applied."),
            @Step(value = "Verify that the updated volume size is reflected.", expected = "PVC reflects the new size."),
            @Step(value = "Ensure continuous message production and consumption are unaffected during the update process.", expected = "Message flow continues without interruption.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    @SuppressWarnings("deprecation") // Storage is deprecated, but some API methods are still called here
    void testResizeJbodVolumes() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int numberOfKafkaReplicas = 3;

        // 300 messages will take 300 seconds in that case
        final int continuousClientsMessageCount = 300;

        PersistentClaimStorage vol0 = new PersistentClaimStorageBuilder().withId(0).withSize("1Gi").withDeleteClaim(true).build();
        PersistentClaimStorage vol1 = new PersistentClaimStorageBuilder().withId(1).withSize("1Gi").withDeleteClaim(true).build();
        PersistentClaimStorage vol1Modified = new PersistentClaimStorageBuilder().withId(1).withSize("5Gi").withDeleteClaim(true).build();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), numberOfKafkaReplicas)
                .editSpec()
                .withStorage(
                    new JbodStorageBuilder()
                        // add two small volumes
                        .addToVolumes(vol0, vol1)
                        .build())
                .endSpec()
                .build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), numberOfKafkaReplicas)
            .editSpec()
                .editKafka()
                    .withStorage(
                        new JbodStorageBuilder()
                            // add two small volumes
                            .addToVolumes(vol0, vol1)
                            .build())
                .endKafka()
            .endSpec()
            .build());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        // ##############################
        // Attach clients which will continuously produce/consume messages to/from Kafka brokers
        // ##############################
        // Setup topic, which has 3 replicas and 2 min.isr

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getContinuousTopicName(), testStorage.getClusterName(), 3, 3, 2).build());

        String producerAdditionConfiguration = "delivery.timeout.ms=40000\nrequest.timeout.ms=5000";
        // Add transactional id to make producer transactional
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\ntransactional.id=" + testStorage.getContinuousTopicName() + ".1");
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\nenable.idempotence=true");

        KafkaClients kafkaBasicClientJob = ClientUtils.getContinuousPlainClientBuilder(testStorage)
            .withMessageCount(continuousClientsMessageCount)
            .withAdditionalConfig(producerAdditionConfiguration)
            .build();

        KubeResourceManager.get().createResourceWithWait(kafkaBasicClientJob.producerStrimzi(), kafkaBasicClientJob.consumerStrimzi());

        // ##############################
        KafkaClients clients = ClientUtils.getInstantPlainClientBuilder(testStorage).build();
        KubeResourceManager.get().createResourceWithWait(clients.producerStrimzi());
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        // Replace Jbod to bigger one volume to Kafka => triggers RU
        LOGGER.info("Replace JBOD to bigger one volume to the Kafka cluster {}", testStorage.getBrokerComponentName());

        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), kafkaNodePool -> {
            JbodStorage storage = (JbodStorage) kafkaNodePool.getSpec().getStorage();

            // set modified volume
            storage.setVolumes(List.of(vol0, vol1Modified));

            // override storage
            kafkaNodePool.getSpec().setStorage(storage);
        });

        // check that volume with index 1 change its size
        PersistentVolumeClaimUtils.waitUntilSpecificPvcSizeChange(
            testStorage,
            "data-" + vol1Modified.getId() + "-" + testStorage.getClusterName() + "-",
            vol1Modified.getSize());
        // and volume with index 0 did not change its size
        PersistentVolumeClaimUtils.waitUntilSpecificPvcSizeChange(
            testStorage,
            "data-" + vol0.getId() + "-" + testStorage.getClusterName() + "-",
            vol0.getSize());

        KubeResourceManager.get().createResourceWithWait(clients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitForContinuousClientSuccess(testStorage, continuousClientsMessageCount);
        // ##############################
    }

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(BRIDGE)
    @TestDoc(
        description = @Desc("This test validates the mounting and usage of additional volumes for Kafka, Kafka Connect, and Kafka Bridge components. It tests whether secret and config map volumes are correctly created, mounted, and accessible across various deployments."),
        steps = {
            @Step(value = "Setup environment prerequisites and configure test storage.", expected = "Ensure the environment is in KRaft mode."),
            @Step(value = "Create necessary Kafka resources with additional volumes for secrets and config maps.", expected = "Resources are correctly instantiated with specified volumes."),
            @Step(value = "Deploy Kafka, Kafka Connect, and Kafka Bridge with these volumes.", expected = "Components are correctly configured with additional volumes."),
            @Step(value = "Verify that all pods (Kafka, Connect, and Bridge) have additional volumes mounted and accessible.", expected = "Volumes are correctly mounted and usable within pods.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testAdditionalVolumes() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int numberOfKafkaReplicas = 3;
        final String configMapName = "example-configmap";
        final String secretName = "example-secret";
        final String secretKey = "apikey123456";
        final String secretValue = "password123";
        final String secretKeyBase64 = Base64.getEncoder().encodeToString(secretKey.getBytes());
        final String secretValueBase64 = Base64.getEncoder().encodeToString(secretValue.getBytes());
        final String secretMountPath = "/mnt/secret-volume";

        final String configMapKey = "ENV_VAR_EXAMPLE";
        final String configMapValue = "VALUE";
        final String configMapMountPath = "/mnt/configmap-volume";

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), numberOfKafkaReplicas).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );

        SecretUtils.createSecret(testStorage.getNamespaceName(), secretName, secretKeyBase64, secretValueBase64);
        KubeResourceManager.get().createResourceWithWait(ConfigMapTemplates.buildConfigMap(testStorage.getNamespaceName(), configMapName,
            configMapKey, configMapValue));

        AdditionalVolume[] additionalVolumes = new AdditionalVolume[]{
            // Secret additional volume
            new AdditionalVolumeBuilder()
                .withName(secretName)
                .withSecret(new SecretVolumeSourceBuilder()
                    .withSecretName(secretName)
                    .build())
                .build(),
            // ConfigMap additional volume
            new AdditionalVolumeBuilder()
                .withName(configMapName)
                .withConfigMap(new ConfigMapVolumeSourceBuilder()
                    .withName(configMapName)
                    .build())
                .build()
        };
        VolumeMount[] volumeMounts = new VolumeMount[]{
            new VolumeMountBuilder()
                .withName(secretName)
                .withMountPath(secretMountPath)
                .build(),
            new VolumeMountBuilder()
                .withName(configMapName)
                .withMountPath(configMapMountPath)
                .build()
        };

        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), numberOfKafkaReplicas)
            .editSpec()
                .editKafka()
                    .editTemplate()
                        .editPod()
                            .addToVolumes(additionalVolumes)
                        .endPod()
                        .editKafkaContainer()
                            .addToVolumeMounts(volumeMounts)
                        .endKafkaContainer()
                    .endTemplate()
                .endKafka()
            .endSpec()
            .build());

        KubeResourceManager.get().createResourceWithWait(KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editSpec()
                .editTemplate()
                    .editPod()
                        .addToVolumes(additionalVolumes)
                    .endPod()
                    .editConnectContainer()
                        .addToVolumeMounts(volumeMounts)
                    .endConnectContainer()
                .endTemplate()
            .endSpec()
            .build());

        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(testStorage.getNamespaceName(), testStorage.getClusterName(),
                        KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), 1)
            .editSpec()
                .editTemplate()
                    .editPod()
                        .addToVolumes(additionalVolumes)
                    .endPod()
                    .editBridgeContainer()
                        .addToVolumeMounts(volumeMounts)
                    .endBridgeContainer()
                .endTemplate()
            .endSpec()
            .build());

        // 1. check Kafka pods
        List<Pod> kafkaPods = new ArrayList<>(KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getBrokerComponentName()));
        // controller pods
        kafkaPods.addAll(KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getControllerComponentName()));

        for (Pod kafkaPod : kafkaPods) {
            verifyPodSecretVolume(testStorage.getNamespaceName(), kafkaPod, "kafka", secretMountPath, secretValueBase64, secretKeyBase64);
            verifyPodConfigMapVolume(testStorage.getNamespaceName(), kafkaPod, "kafka", configMapMountPath, configMapValue, configMapKey);
        }

        // 2. check Connect pods
        List<Pod> connectPods = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector());
        for (Pod connectPod : connectPods) {
            verifyPodSecretVolume(testStorage.getNamespaceName(), connectPod, KafkaConnectResources.componentName(testStorage.getClusterName()), secretMountPath, secretValueBase64, secretKeyBase64);
            verifyPodConfigMapVolume(testStorage.getNamespaceName(), connectPod, KafkaConnectResources.componentName(testStorage.getClusterName()), configMapMountPath, configMapValue, configMapKey);
        }

        // 3. check Bridge pods
        List<Pod> bridgePods = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(),
            testStorage.getBridgeSelector());

        for (Pod bridgePod : bridgePods) {
            verifyPodSecretVolume(testStorage.getNamespaceName(), bridgePod, KafkaBridgeResources.componentName(testStorage.getClusterName()), secretMountPath, secretValueBase64, secretKeyBase64);
            verifyPodConfigMapVolume(testStorage.getNamespaceName(), bridgePod, KafkaBridgeResources.componentName(testStorage.getClusterName()), configMapMountPath, configMapValue, configMapKey);
        }
    }

    /**
     * Verifies the configuration and content of a secret volume mounted in a specified Kubernetes pod.
     * This includes checks to ensure the secret is mounted at the specified path and contains the expected Base64-encoded value.
     *
     * @param namespace         The Kubernetes namespace in which the pod is located.
     * @param pod               The pod object to verify.
     * @param secretMountPath   The mount path for the secret volume.
     * @param secretValueBase64 The expected Base64-encoded value of the secret to be verified.
     * @param secretKeyBase64   The key within the secret volume used to access the secret content.
     * @param containerName     The name of the container in the pod where the check is to be performed.
     */
    private void verifyPodSecretVolume(final String namespace, final Pod pod, final String containerName,
                                       final String secretMountPath, final String secretValueBase64, final String secretKeyBase64) {
        final String podName = pod.getMetadata().getName();
        final String secretMountCheck = KubeResourceManager.get().kubeCmdClient().inNamespace(namespace)
                .execInPodContainer(LogLevel.DEBUG, podName, containerName, "sh", "-c", "cat /proc/mounts | grep " + secretMountPath).out().trim();

        // Assert that secret mount exists
        assertThat(secretMountCheck, containsString(secretMountPath));

        // Verify content inside the secret volume
        final String secretContentCheck = KubeResourceManager.get().kubeCmdClient().inNamespace(namespace)
                .execInPodContainer(LogLevel.DEBUG, podName, containerName, "sh", "-c", "cat " + secretMountPath + "/" + secretKeyBase64).out().trim();

        assertThat(secretContentCheck, is(secretValueBase64));
    }

    /**
     * Verifies the configuration and content of a config map volume mounted in a specified Kubernetes pod.
     * This includes checks to ensure the config map is mounted at the specified path and contains the expected value.
     *
     * @param namespace            The Kubernetes namespace in which the pod is located.
     * @param pod                  The pod object to verify.
     * @param configMapMountPath   The mount path for the config map volume.
     * @param configMapValue       The expected value of the config map to be verified.
     * @param configMapKey         The key within the config map volume used to access the config map content.
     * @param containerName        The name of the container in the pod where the check is to be performed.
     */
    private void verifyPodConfigMapVolume(final String namespace, final Pod pod, final String containerName,
                                          final String configMapMountPath, final String configMapValue, final String configMapKey) {
        final String podName = pod.getMetadata().getName();
        final String configMountCheck = KubeResourceManager.get().kubeCmdClient().inNamespace(namespace)
            .execInPodContainer(LogLevel.DEBUG, podName, containerName, "sh", "-c", "cat /proc/mounts | grep " + configMapMountPath).out().trim();

        // Assert that config map mount exists
        assertThat(configMountCheck, containsString(configMapMountPath));

        // Verify content inside the config map volume
        final String configContentCheck = KubeResourceManager.get().kubeCmdClient().inNamespace(namespace)
                .execInPodContainer(LogLevel.DEBUG, podName, containerName, "sh", "-c", "cat " + configMapMountPath + "/" + configMapKey).out().trim();

        assertThat(configContentCheck, is(configMapValue));
    }

    void verifyVolumeNamesAndLabels(String namespaceName, String clusterName, String podSetName, int kafkaReplicas, int diskCountPerReplica, String diskSizeGi) {
        ArrayList<String> pvcs = new ArrayList<>();

        PersistentVolumeClaimUtils.listPVCsByNameSubstring(namespaceName, clusterName).stream()
            .filter(pvc -> pvc.getMetadata().getName().contains(podSetName))
            .forEach(volume -> {
                String volumeName = volume.getMetadata().getName();
                pvcs.add(volumeName);
                LOGGER.info("Checking labels for volume: " + volumeName);
                assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
                assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(Kafka.RESOURCE_KIND));
                assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_NAME_LABEL), is(clusterName.concat("-kafka")));
                assertThat(volume.getSpec().getResources().getRequests().get("storage"), is(new Quantity(diskSizeGi, "Gi")));
            });

        LOGGER.info("Checking PVC names included in JBOD array");
        for (int i = 0; i < kafkaReplicas; i++) {
            for (int j = 0; j < diskCountPerReplica; j++) {
                assertThat(pvcs.contains("data-" + j + "-" + podSetName + "-" + i), is(true));
            }
        }

        LOGGER.info("Checking PVC on Kafka Pods");
        for (int i = 0; i < kafkaReplicas; i++) {
            ArrayList<String> dataSourcesOnPod = new ArrayList<>();
            ArrayList<String> pvcsOnPod = new ArrayList<>();

            LOGGER.info("Getting list of mounted data sources and PVCs on Kafka Pod: " + i);
            for (int j = 0; j < diskCountPerReplica; j++) {
                dataSourcesOnPod.add(KubeResourceManager.get().kubeClient().getClient().pods().inNamespace(namespaceName).withName(String.join("-", podSetName, String.valueOf(i))).get()
                    .getSpec().getVolumes().get(j).getName());
                pvcsOnPod.add(KubeResourceManager.get().kubeClient().getClient().pods().inNamespace(namespaceName).withName(String.join("-", podSetName, String.valueOf(i))).get()
                    .getSpec().getVolumes().get(j).getPersistentVolumeClaim().getClaimName());
            }

            LOGGER.info("Verifying mounted data sources and PVCs on Kafka Pod: " + i);
            for (int j = 0; j < diskCountPerReplica; j++) {
                assertThat(dataSourcesOnPod.contains("data-" + j), is(true));
                assertThat(pvcsOnPod.contains("data-" + j + "-" + podSetName + "-" + i), is(true));
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

    protected void afterEachMayOverride() {
        KubeResourceManager.get().deleteResources();

        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, KubeResourceManager.get().getTestContext());
        if (CrdClients.kafkaClient().inNamespace(namespaceName).withName(OPENSHIFT_CLUSTER_NAME).get() != null) {
            KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).deleteByName(Kafka.RESOURCE_KIND, OPENSHIFT_CLUSTER_NAME);
        }

        KubeResourceManager.get().kubeClient().listPods(namespaceName).stream()
            .filter(p -> p.getMetadata().getName().startsWith(OPENSHIFT_CLUSTER_NAME))
            .forEach(p -> PodUtils.deletePodWithWait(p.getMetadata().getNamespace(), p.getMetadata().getName()));

        CrdClients.kafkaTopicClient().inNamespace(namespaceName).delete();
        KubeResourceManager.get().kubeClient().getClient().persistentVolumeClaims().inNamespace(namespaceName).delete();
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();
    }
}
