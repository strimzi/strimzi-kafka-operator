/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.RequiredMinKubeOrOcpBasedKubeVersion;
import io.strimzi.systemtest.enums.PodSecurityProfile;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.POD_SECURITY_PROFILES_RESTRICTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 *
 * PodSecurityProfilesIsolatedST provides tests for Pod Security profiles. In short, Pod security profiles are a mechanism used
 * in Pods or containers, which may prohibit some set of operations (e.g., running only as a non-root user, allowing
 * only some Volume types etc.).
 *
 * Reason why is this test suite has to run in complete isolation (i.e., {@link IsolatedSuite})
 * is that we need to modify Cluster Operator configuration, specifically env {@code STRIMZI_POD_SECURITY_PROVIDER_CLASS} to restricted.
 *
 * Test cases are design to verify common behaviour of Pod Security profiles. Specifically, (i.) we check if containers such
 * as Kafka, ZooKeeper, EntityOperator, KafkaBridge has properly set .securityContext (ii.) then we check if these
 * resources working and are stable with exchanging messages.
 */
@Tag(REGRESSION)
@Tag(POD_SECURITY_PROFILES_RESTRICTED)
@IsolatedSuite
public class PodSecurityProfilesIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(PodSecurityProfilesIsolatedST.class);

    @Tag(ACCEPTANCE)
    @ParallelNamespaceTest
    // Pod Security profiles works from 1.23 Kubernetes version or OCP 4.11
    @RequiredMinKubeOrOcpBasedKubeVersion(kubeVersion = 1.23, ocpBasedKubeVersion = 1.24)
    void testKafkaWithRestrictedSecurityProfile(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        addRestrictedPodSecurityProfileToNamespace(testStorage.getNamespaceName());

        // if Kafka Pods deploys it means that it works...
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3).build());

        // 1. check the generated structure of SecurityContext of Kafka Pods
        // verifies that (i.) Pods and (ii.) Containers has proper generated SC
        verifyPodAndContainerSecurityContext(PodUtils.getKafkaClusterPods(testStorage));

        // 2. check that Kafka cluster is usable and everything is working
        verifyStabilityOfKafkaCluster(extensionContext, testStorage);
    }

    @ParallelNamespaceTest
    @RequiredMinKubeOrOcpBasedKubeVersion(kubeVersion = 1.23, ocpBasedKubeVersion = 1.24)
    void testKafkaWithKafkaBridgeRestrictedSecurityProfile(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        addRestrictedPodSecurityProfileToNamespace(testStorage.getNamespaceName());

        // if Kafka and KafkaBridge Pods deploys it means that it works...
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3).build());
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(testStorage.getClusterName(),
            KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), 1).build());

        final List<Pod> kafkaClusterAndKafkaBridgePods = PodUtils.getKafkaClusterPods(testStorage);
        // add KafkaBridge Pod
        kafkaClusterAndKafkaBridgePods.addAll(kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaBridge.RESOURCE_KIND));

        // 1. check the generated structure of SecurityContext of Kafka and KafkaBridge Pods
        // verifies that (i.) Pods and (ii.) Containers has proper generated SC
        verifyPodAndContainerSecurityContext(kafkaClusterAndKafkaBridgePods);

        // 2. check that Kafka cluster is usable and everything is working
        verifyStabilityOfKafkaCluster(extensionContext, testStorage);
    }

    @ParallelNamespaceTest
    @RequiredMinKubeOrOcpBasedKubeVersion(kubeVersion = 1.23, ocpBasedKubeVersion = 1.24)
    void testKafkaWithMirrorMakerRestrictedSecurityProfile(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        addRestrictedPodSecurityProfileToNamespace(testStorage.getNamespaceName());

        // if Kafka and KafkaMirrorMaker Pods deploys it means that it works...
        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1).build(),
            KafkaTemplates.kafkaPersistent(testStorage.getTargetClusterName(), 1).build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build(),
            KafkaTopicTemplates.topic(testStorage.getTargetClusterName(), testStorage.getTargetTopicName()).build());

        final KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withUserName(testStorage.getUserName())
            .withPodSecurityPolicy(PodSecurityProfile.RESTRICTED)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerStrimzi());
        ClientUtils.waitForProducerClientSuccess(testStorage);

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), testStorage.getTargetClusterName(), testStorage.getClusterName(), ClientUtils.generateRandomConsumerGroup(), 1, false).build());

        final List<Pod> kafkaClusterAndKafkaMirrorMakerPods = PodUtils.getKafkaClusterPods(testStorage);
        // add KafkaMirrorMaker Pod
        kafkaClusterAndKafkaMirrorMakerPods.addAll(kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND));

        // 1. check the generated structure of SecurityContext of Kafka and KafkaMirrorMaker Pods
        // verifies that (i.) Pods and (ii.) Containers has proper generated SC
        verifyPodAndContainerSecurityContext(kafkaClusterAndKafkaMirrorMakerPods);

        // 2. check that KMM mirrors messages
        resourceManager.createResource(extensionContext, kafkaClients.consumerStrimzi());

        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @RequiredMinKubeOrOcpBasedKubeVersion(kubeVersion = 1.23, ocpBasedKubeVersion = 1.24)
    void testKafkaWithMirrorMaker2RestrictedSecurityProfile(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        addRestrictedPodSecurityProfileToNamespace(testStorage.getNamespaceName());

        // if Kafka and KafkaMirrorMaker Pods deploys it means that it works...
        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1).build(),
            KafkaTemplates.kafkaPersistent(testStorage.getTargetClusterName(), 1).build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build(),
            KafkaTopicTemplates.topic(testStorage.getTargetClusterName(), testStorage.getTargetTopicName()).build());

        final KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withUserName(testStorage.getUserName())
            .withPodSecurityPolicy(PodSecurityProfile.RESTRICTED)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerStrimzi());
        ClientUtils.waitForProducerClientSuccess(testStorage);

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(testStorage.getClusterName(), testStorage.getTargetClusterName(), testStorage.getClusterName(), ClientUtils.generateRandomConsumerGroup(), 1, false).build());

        final List<Pod> kafkaClusterAndKafkaMirrorMakerPods = PodUtils.getKafkaClusterPods(testStorage);
        // add KafkaMirrorMaker2 Pod
        kafkaClusterAndKafkaMirrorMakerPods.addAll(kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND));

        // 1. check the generated structure of SecurityContext of Kafka and KafkaMirrorMaker2 Pods
        // verifies that (i.) Pods and (ii.) Containers has proper generated SC
        verifyPodAndContainerSecurityContext(kafkaClusterAndKafkaMirrorMakerPods);

        // 2. check that KMM2 mirrors messages
        resourceManager.createResource(extensionContext, kafkaClients.consumerStrimzi());

        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @RequiredMinKubeOrOcpBasedKubeVersion(kubeVersion = 1.23, ocpBasedKubeVersion = 1.24)
    void testKafkaWithIncorrectConfiguredKafkaClient(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        addRestrictedPodSecurityProfileToNamespace(testStorage.getNamespaceName());

        // if Kafka Pods deploys it means that it works...
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3).build());

        // 1. check the generated structure of SecurityContext of Kafka Pods
        // verifies that (i.) Pods and (ii.) Containers has proper generated SC
        verifyPodAndContainerSecurityContext(PodUtils.getKafkaClusterPods(testStorage));

        // 2. check that Client is unable to create due to incorrect configuration
        final KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withConsumerName(testStorage.getConsumerName())
            .withProducerName(testStorage.getProducerName())
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            // we set DEFAULT, which is in this case incorrect and client should crash
            .withPodSecurityPolicy(PodSecurityProfile.DEFAULT)
            .build();

        // 3. excepted failure
        // job_controller.go:1437 pods "..." is forbidden: violates PodSecurity "restricted:latest": allowPrivilegeEscalation != false
        // (container "..." must set securityContext.allowPrivilegeEscalation=false),
        // unrestricted capabilities (container "..." must set securityContext.capabilities.drop=["ALL"]),
        // runAsNonRoot != true (pod or container "..." must set securityContext.runAsNonRoot=true),
        // seccompProfile (pod or container "..." must set securityContext.seccompProfile.type to "RuntimeDefault" or "Localhost")
        resourceManager.createResource(extensionContext, false, kafkaClients.producerStrimzi());
        ClientUtils.waitForProducerClientTimeout(testStorage);
    }

    @BeforeAll
    void beforeAll(ExtensionContext extensionContext) {
        // we configure Pod Security via provider class, which sets SecurityContext to all containers (e.g., Kafka, ZooKeeper,
        // EntityOperator, Bridge). Another alternative but more complicated is to set it via .template section inside each CR.
        clusterOperator.unInstall();
        clusterOperator = clusterOperator
            .defaultInstallation()
            .withExtraEnvVars(Collections.singletonList(new EnvVarBuilder()
                .withName("STRIMZI_POD_SECURITY_PROVIDER_CLASS")
                // default is `baseline` and thus other tests suites are testing it
                .withValue("restricted")
                .build()))
            .createInstallation()
            .runInstallation();
    }

    private void verifyPodAndContainerSecurityContext(final Iterable<? extends Pod> kafkaPods) {
        for (final Pod pod : kafkaPods) {
            // verify Container SecurityContext
            if (pod.getSpec().getContainers() != null) {
                verifyContainerSecurityContext(pod.getSpec().getContainers());
            }
        }
    }

    /**
     * Provides a check for Container SecurityContext inside Pod resource.
     *
     * securityContext:
     *     allowPrivilegeEscalation: false
     *     capabilities:
     *      drop:
     *       - ALL
     *     runAsNonRoot: true
     *     seccompProfile:
     *      type: RuntimeDefault
     * @param containers    containers, with injected SecurityContext by Cluster Operator for restricted profile
     */
    private void verifyContainerSecurityContext(final Iterable<? extends Container> containers) {
        for (final Container c : containers) {
            final SecurityContext sc = c.getSecurityContext();
            LOGGER.debug("Verifying Container: {} with following SecurityContext: {}", c.getName(), sc);

            assertThat(sc.getAllowPrivilegeEscalation(), CoreMatchers.is(false));
            assertThat(sc.getCapabilities().getDrop(), CoreMatchers.hasItem("ALL"));
            assertThat(sc.getRunAsNonRoot(), CoreMatchers.is(true));
            assertThat(sc.getSeccompProfile().getType(), CoreMatchers.is("RuntimeDefault"));
        }
    }

    private void verifyStabilityOfKafkaCluster(final ExtensionContext extensionContext, final TestStorage testStorage) {
        final KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withConsumerName(testStorage.getConsumerName())
            .withProducerName(testStorage.getProducerName())
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .withPodSecurityPolicy(PodSecurityProfile.RESTRICTED)
            .build();

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );

        ClientUtils.waitForClientsSuccess(testStorage);
    }

    private void addRestrictedPodSecurityProfileToNamespace(final String namespaceName) {
        final Namespace namespace = kubeClient().getNamespace(namespaceName);
        final Map<String, String> namespaceLabels = namespace.getMetadata().getLabels();
        namespaceLabels.put("pod-security.kubernetes.io/enforce", "restricted");
        namespace.getMetadata().setLabels(namespaceLabels);
        kubeClient().createOrReplaceNamespace(namespace);
    }
}
